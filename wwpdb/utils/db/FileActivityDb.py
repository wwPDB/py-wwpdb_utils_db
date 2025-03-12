##
# File:    FileActivityDb.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-03-07
#
# Updates:
#   - Added configuration check methods
#   - Improved error handling and logging
##
"""
Enhanced module implementing file activity database operations.

This module provides support for tracking and managing file activities
in the OneDep system database with improved integration capabilities.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import re
import sys
from datetime import datetime
from contextlib import contextmanager
from typing import List, Optional, Tuple, Dict, Any, Generator, ClassVar, TextIO

from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppCommon
from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery
from wwpdb.io.locator.PathInfo import PathInfo

logger = logging.getLogger(__name__)

class FileActivityDb:
    """
    An enhanced class to manage file activity database operations in the OneDep system.

    This class provides methods for:
    - Loading file metadata into the database
    - Querying file changes based on various criteria
    - Displaying formatted database contents
    - Purging database records

    New features include:
    - Error handling
    - Connection management improvements

    The class maintains a connection to the OneDep metadata database and provides
    both high-level operations and utility methods for database interactions.
    """

    # Class-level constants
    TABLE_NAME: ClassVar[str] = "file_activity_log"
    DEFAULT_DB_NAME: ClassVar[str] = "onedep_metadata"  # Default database name

    def __init__(self, siteId: Optional[str] = None, verbose: bool = False, log: TextIO = sys.stderr) -> None:
        """
        Initialize the FileActivityDb instance.

        The database connection is not established during initialization.
        It will be established on first use.

        Args:
            siteId (Optional[str]): Site identifier. If None, it will be determined automatically.
            verbose (bool): Enable verbose output
            log (TextIO): Log file handle for verbose output
        """
        self.__myQuery: Optional[MyDbQuery] = None
        self.__verbose: bool = verbose
        self.__dbcon = None
        self.__closed: bool = True  # Start with no connection
        self.__siteId = siteId if siteId is not None else getSiteId()
        self.__path_info = PathInfo(siteId=self.__siteId, verbose=verbose)
        self.__lfh = log

    def __initializeDbConnection(self) -> None:
        """
        Initialize database connection using wwPDB utilities.

        Establishes a connection to the OneDep metadata database using configuration
        from ConfigInfo. Sets up the query object for database operations.
        This is called lazily when the connection is first needed.

        Raises:
            Exception: If database connection fails or configuration is invalid.
        """
        if not self.__closed:
            return  # Connection already open

        try:
            config = ConfigInfo()

            # Get the database name from site configuration
            db_name = self.DEFAULT_DB_NAME

            myC = MyDbConnect(
                dbServer="mysql",
                dbHost=config.get("SITE_DB_HOST_NAME"),
                dbName=db_name,
                dbUser=config.get("SITE_DB_USER_NAME"),
                dbPw=config.get("SITE_DB_PASSWORD"),
                dbPort=str(config.get("SITE_DB_PORT_NUMBER")),
                dbSocket=config.get("SITE_DB_SOCKET"),
                verbose=self.__verbose,
                log=self.__lfh
            )
            self.__dbcon = myC.connect()
            if self.__dbcon:
                self.__myQuery = MyDbQuery(dbcon=self.__dbcon, verbose=self.__verbose, log=self.__lfh)
                self.__closed = False  # Mark connection as open
            else:
                raise Exception("Failed to establish database connection")
        except Exception as err:
            logger.error("Unable to connect to the database: %s", err)
            raise

    @contextmanager
    def __connection(self) -> Generator[None, None, None]:
        """
        Context manager for database connection.

        Ensures connection is established before operation and closed after operation.
        Should be used in a with statement around database operations.

        Yields:
            None

        Raises:
            Exception: If the database connection cannot be established.
        """
        need_close = self.__closed  # Only close if we created the connection
        try:
            self.__initializeDbConnection()
            yield
        finally:
            if need_close:
                self.close()

    def close(self) -> None:
        """
        Close the database connection.

        This should be called when done with database operations to free up resources.
        The connection will be reestablished automatically if needed.
        """
        if not self.__closed and self.__dbcon is not None:
            try:
                self.__dbcon.close()
            except Exception as err:
                logger.warning("Error closing database connection: %s", err)
            finally:
                self.__myQuery = None
                self.__dbcon = None
                self.__closed = True

    def __enter__(self) -> 'FileActivityDb':
        """
        Context manager entry.

        Returns:
            FileActivityDb: self for use in with statement
        """
        self.__initializeDbConnection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit.

        Ensures database connection is properly closed.
        """
        self.close()

    def isTrackingEnabled(self) -> bool:
        """
        Check if file activity tracking is enabled in the site configuration.

        Returns:
            bool: True if file activity tracking is enabled, False otherwise
        """
        try:
            config_app = ConfigInfoAppCommon(self.__siteId)
            return config_app.get_file_activity_db_support()
        except Exception as e:
            logger.warning(f"Error checking file activity configuration: {str(e)}")
            return False

    def logActivity(self, file_path: str, timestamp: Optional[datetime] = None) -> bool:
        """
        Log a file activity.

        This method checks if tracking is enabled, then records the file activity
        in the database using a context manager to ensure proper connection handling.

        Args:
            file_path (str): Path to the file to log
            timestamp (Optional[datetime]): File timestamp. If None, will use
                                            the file's modification time.

        Returns:
            bool: True if logging was successful or if logging is disabled,
                  False if an error occurred during logging
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            return True

        try:
            # Use a context manager to ensure proper connection handling
            with self.__connection():
                # Log the file directly
                result = self.addFileRecord(file_path, timestamp)

                if self.__verbose and self.__lfh:
                    if result:
                        self.__lfh.write(f"+FileActivityDb.logActivity Successfully logged: {file_path}\n")
                    else:
                        self.__lfh.write(f"+FileActivityDb.logActivity Failed to log: {file_path}\n")

                return result
        except Exception as e:
            if self.__verbose and self.__lfh:
                self.__lfh.write(f"+FileActivityDb.logActivity Error logging file: {str(e)}\n")
            logger.error(f"Error logging file activity: {str(e)}")
            return False

    def populateFileActivityDb(self, directory: str, site_id: Optional[str] = None) -> None:
        """
        Populate the database with file metadata from the given directory.

        Scans the directory and its immediate subdirectories for files matching
        the OneDep naming convention. For each unique file key, inserts or updates
        only the file with the highest version into the database.

        Args:
            directory (str): Path to the directory containing files to process
            site_id (Optional[str]): Site ID to use. If None, uses instance site_id.

        Raises:
            ValueError: If the directory is invalid or inaccessible
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.info("File activity tracking is disabled in site configuration")
            return

        try:
            with self.__connection():
                if not os.path.isdir(directory):
                    raise ValueError(f"Invalid directory: {directory}")

                # Use the instance's site_id if none is provided
                if site_id is None:
                    site_id = self.__siteId

                logger.debug("Scanning directory: %s", directory)

                for subdir_entry in os.scandir(directory):
                    if subdir_entry.is_dir():
                        logger.debug("Scanning subdirectory: %s", subdir_entry.path)
                        group_files: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}
                        for file_entry in os.scandir(subdir_entry.path):
                            if file_entry.is_file():
                                logger.debug("Processing file: %s", file_entry.name)
                                try:
                                    file_name = os.path.basename(file_entry.path)
                                    metadata_tuple = self.__path_info.splitFileName(file_name)
                                    deposition_id, content_type, format_type, part_number, version_number = metadata_tuple

                                    if None in (deposition_id, content_type, format_type, part_number):
                                        logger.warning("Skipping unrecognized file: %s", file_entry.path)
                                        continue

                                    # PathInfo doesn't parse milestone
                                    milestone = "unknown"
                                    if version_number is None:
                                        version_number = 1

                                    key = (deposition_id, content_type, format_type, part_number)
                                    created_date = datetime.fromtimestamp(file_entry.stat().st_ctime).strftime('%Y-%m-%d %H:%M:%S')

                                    if key not in group_files or group_files[key]['version_number'] < version_number:
                                        group_files[key] = {
                                            'deposition_id': deposition_id,
                                            'content_type': content_type,
                                            'format_type': format_type,
                                            'part_number': part_number,
                                            'version_number': version_number,
                                            'milestone': milestone,
                                            'file_path': file_entry.path,
                                            'created_date': created_date
                                        }
                                        logger.debug("Updated group_files with: %s", group_files[key])
                                except Exception as e:
                                    logger.warning("Error processing file %s: %s", file_entry.path, str(e))

                        self.__updateDbRecords(group_files, site_id)

                logger.info("Successfully loaded the latest version files from %s into the database.", directory)
        except Exception as e:
            logger.error("Error loading files from directory: %s", e)
            raise

    def __updateDbRecord(self, record: Dict[str, Any], site_id: Optional[str] = None) -> None:
        """
        Update a single database record.

        Args:
            record (Dict[str, Any]): Dictionary containing the file record data
            site_id (Optional[str]): The site identifier for recording file location. Defaults to instance site_id.

        Raises:
            Exception: For database operation failures
        """
        # Use the instance's site_id if none is provided
        if site_id is None:
            site_id = self.__siteId

        # Query to check for existing record
        check_sql = f"""
            SELECT version_number, created_date FROM {self.TABLE_NAME}
            WHERE deposition_id = '{record['deposition_id']}'
            AND content_type = '{record['content_type']}'
            AND format_type = '{record['format_type']}'
            AND part_number = {record['part_number']};
        """

        if self.__verbose:
            logger.debug("Executing check SQL: %s", check_sql)

        # Execute the SQL directly
        existing_record = self.__myQuery.selectRows(check_sql)

        if not existing_record or existing_record[0][0] < record['version_number']:
            # Use the new record's timestamp since we're updating to latest version
            created_date = record['created_date']

            # Create the SQL statement for insert/update operation
            insert_sql = f"""
                INSERT INTO {self.TABLE_NAME}
                (deposition_id, content_type, format_type, part_number,
                 version_number, milestone, location, site_id, metadata_json, created_date)
                VALUES (
                    '{record['deposition_id']}',
                    '{record['content_type']}',
                    '{record['format_type']}',
                    {record['part_number']},
                    {record['version_number']},
                    '{record['milestone']}',
                    '{record['file_path'].replace("'", "''")}',
                    '{site_id if site_id else ""}',
                    NULL,
                    '{created_date}'
                )
                ON DUPLICATE KEY UPDATE
                    version_number = VALUES(version_number),
                    location = VALUES(location),
                    created_date = VALUES(created_date);
            """

            if self.__verbose:
                logger.debug("Executing insert SQL: %s", insert_sql)

            self.__myQuery.sqlCommand([insert_sql])

    def __updateDbRecords(self, group_files: Dict[Tuple[str, str, str, int], Dict[str, Any]], site_id: Optional[str] = None) -> None:
        """
        Update database records for a group of files.

        For each file record, checks if it exists in the database and updates it
        if the new version is higher than the existing one.

        Args:
            group_files (Dict[Tuple[str, str, str, int], Dict[str, Any]]): Dictionary of file records grouped by key
            site_id (Optional[str]): The site identifier for recording file location. Defaults to None.

        Raises:
            Exception: For database operation failures
        """
        for record in group_files.values():
            self.__updateDbRecord(record, site_id)

    def getFileActivity(self, hours: Optional[int] = None, days: Optional[int] = None,
                       site_id: Optional[str] = None, deposition_ids: str = "ALL",
                       file_types: str = "ALL", formats: str = "ALL") -> List[str]:
        """
        Retrieve file activity based on specified criteria.

        Args:
            hours (Optional[int]): Time range in hours
            days (Optional[int]): Time range in days
            site_id (Optional[str]): Site ID to filter results. If None, uses instance site_id.
            deposition_ids (str): Deposition IDs to filter (ALL, single ID, range, or comma-separated list)
            file_types (str): File types to filter (ALL or comma-separated list)
            formats (str): File formats to filter (ALL or comma-separated list)

        Returns:
            List[str]: List of file locations matching the criteria

        Raises:
            Exception: If there's an error processing the query
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.info("File activity tracking is disabled in site configuration")
            return []

        # Use the instance's site_id if none is provided
        if site_id is None:
            site_id = self.__siteId

        # Calculate total hours from either hours or days parameter
        if hours is not None:
            total_hours = hours
        elif days is not None:
            total_hours = days * 24
        else:
            total_hours = 24  # Default to last 24 hours if neither specified

        base_query = f"SELECT location FROM {self.TABLE_NAME} WHERE created_date >= DATE_SUB(NOW(), INTERVAL {total_hours} HOUR)"

        # Add site_id filter if provided
        if site_id:
            base_query += f" AND site_id = '{site_id}'"

        # Handle deposition IDs
        if deposition_ids.upper() != "ALL":
            if "-" in deposition_ids:
                try:
                    start_id, end_id = deposition_ids.split("-")
                    start_num = int(start_id.replace("D_", ""))
                    end_num = int(end_id.replace("D_", ""))
                    base_query += f" AND CAST(SUBSTRING(deposition_id, 3) AS UNSIGNED) BETWEEN {start_num} AND {end_num}"
                except Exception:
                    logger.error("Invalid deposition-ids range '%s'. Valid options: ALL, D_XXXX, D_XXXX-D_YYYY, or comma separated list.", deposition_ids)
                    raise ValueError("Invalid deposition-ids range")
            elif "," in deposition_ids:
                dep_list = [f"'{d.strip()}'" for d in deposition_ids.split(",")]
                base_query += f" AND deposition_id IN ({','.join(dep_list)})"
            else:
                base_query += f" AND deposition_id = '{deposition_ids}'"

        # Handle file types
        if file_types.upper() != "ALL":
            types_list = file_types.split(",")
            type_conditions = []
            for ft in types_list:
                ft = ft.strip()
                type_conditions.append(f"content_type LIKE '{ft}%'")
            if type_conditions:
                base_query += f" AND ({' OR '.join(type_conditions)})"

        # Handle formats
        if formats.upper() != "ALL":
            formats_list = formats.split(",")
            format_conditions = []
            for fmt in formats_list:
                fmt = fmt.strip()
                format_conditions.append(f"format_type = '{fmt}'")
            if format_conditions:
                base_query += f" AND ({' OR '.join(format_conditions)})"

        with self.__connection():
            try:
                results = self.__myQuery.selectRows(base_query)
                return [row[0] for row in results] if results else []
            except Exception as err:
                logger.error("Unable to retrieve changed files from file_activity_log: %s", err)
                raise

    def purgeFileActivityDb(self, confirmed: bool = False) -> None:
        """
        Purge all data from the file activity database.

        This is a destructive operation that removes all records while maintaining
        the database structure. Requires explicit confirmation.

        Args:
            confirmed (bool): Must be True to execute the purge operation

        Raises:
            Exception: If confirmation is False or database operation fails

        See Also:
            purgeDepositionData: For purging data for a specific deposition ID
        """
        if not confirmed:
            logger.error("Must provide --confirmed flag to purge database")
            raise ValueError("Must provide --confirmed flag to purge database")

        with self.__connection():
            try:
                self.__myQuery.sqlCommand([f"TRUNCATE TABLE {self.TABLE_NAME};"])
                logger.info(f"Successfully purged all data from {self.TABLE_NAME} table.")
            except Exception as err:
                logger.error("Failed to purge database: %s", err)
                raise

    def purgeDepositionData(self, deposition_id: str, confirmed: bool = False) -> None:
        """
        Purge all data for a specific deposition ID from the file activity database.

        This is a destructive operation that removes all records for the specified
        deposition ID. Requires explicit confirmation to prevent accidental deletion.

        Args:
            deposition_id (str): The deposition ID to purge (e.g., "D_1000000000")
            confirmed (bool): Must be True to execute the purge operation

        Raises:
            ValueError: If confirmation is False or deposition_id format is invalid
            Exception: For database operation failures
        """
        if not confirmed:
            logger.error("Must provide confirmed=True to purge deposition data")
            raise ValueError("Confirmation required for purge operation")

        # Validate deposition ID format - must be D_ followed by any 10 digits
        if not re.match(r"^D_\d{10}$", deposition_id):
            logger.error("Invalid deposition ID format: %s. Expected format: D_XXXXXXXXXX", deposition_id)
            raise ValueError(f"Invalid deposition ID format: {deposition_id}")

        with self.__connection():
            try:
                # First, get count of records to be deleted
                count_sql = f"""
                    SELECT COUNT(*) FROM {self.TABLE_NAME}
                    WHERE deposition_id = '{deposition_id}';
                """
                count_result = self.__myQuery.selectRows(count_sql)
                record_count = count_result[0][0] if count_result else 0

                # Execute the delete
                delete_sql = f"""
                    DELETE FROM {self.TABLE_NAME}
                    WHERE deposition_id = '{deposition_id}';
                """
                self.__myQuery.sqlCommand([delete_sql])

                logger.info("Successfully purged %d records for deposition ID: %s",
                          record_count, deposition_id)
            except Exception as err:
                logger.error("Failed to purge data for deposition ID %s: %s",
                           deposition_id, err)
                raise

    def displayFileActivityDb(self, hours: Optional[int] = None, days: Optional[int] = None,
                            site_id: Optional[str] = None) -> None:
        """
        Display formatted database contents.

        Prints a formatted table of database records matching the time and site criteria.
        Output format:
            site_id, dep_id, file_type, last_timestamp

        Args:
            hours (Optional[int]): Time range in hours
            days (Optional[int]): Time range in days
            site_id (Optional[str]): Site ID to filter results. If None, uses instance site_id.

        Raises:
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.info("File activity tracking is disabled in site configuration")
            return

        # Use the instance's site_id if none is provided
        if site_id is None:
            site_id = self.__siteId

        total_hours = hours if hours is not None else (days * 24 if days is not None else 24)

        query = f"""
            SELECT DISTINCT site_id, deposition_id, content_type, created_date
            FROM {self.TABLE_NAME}
            WHERE created_date >= DATE_SUB(NOW(), INTERVAL {total_hours} HOUR)
        """

        if site_id:
            query += f" AND site_id = '{site_id}'"

        with self.__connection():
            try:
                results = self.__myQuery.selectRows(query)
                if not results:
                    logger.info("No records found for the specified criteria.")
                    return

                # Print headers
                headers = ["site_id", "dep_id", "file_type", "last_timestamp"] if site_id else ["dep_id", "file_type", "last_timestamp"]
                print(",".join(headers))

                # Print data
                for row in results:
                    if site_id:
                        print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
                    else:
                        print(f"{row[1]},{row[2]},{row[3]}")

            except Exception as err:
                logger.error("Failed to display database contents: %s", err)
                raise

    # -----------------------------------------
    # Public methods for file record management
    # -----------------------------------------

    def addFileRecord(self, file_path: str, timestamp: Optional[datetime] = None) -> bool:
        """
        Add or update a file record in the database.

        This method parses the file metadata and adds or updates the record in the database.
        If the file doesn't follow the OneDep naming convention, the method returns False.

        Args:
            file_path (str): Path to the file
            timestamp (Optional[datetime]): File timestamp. If None, will use the file's
                                            current timestamp from the filesystem

        Returns:
            bool: True if the record was successfully added or updated, False otherwise

        Raises:
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.debug("File activity tracking is disabled in site configuration")
            return True

        # Get timestamp from filesystem if not provided
        if timestamp is None and os.path.exists(file_path):
            timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

        # Try using PathInfo first for parsing metadata
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            deposition_id, content_type, format_type, part_number, version_number = metadata_tuple

            if None in (deposition_id, content_type, format_type, part_number):
                logger.warning(f"File {file_path} doesn't follow OneDep naming convention, can't add to database")
                return False
            else:
                # Use PathInfo result but add milestone
                milestone = "unknown"  # PathInfo doesn't parse milestone
                if version_number is None:
                    version_number = 1
        except Exception as e:
            logger.warning(f"File {file_path} couldn't be parsed: {str(e)}")
            return False

        # Format timestamp for database
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        # Create the record dictionary
        record = {
            'deposition_id': deposition_id,
            'content_type': content_type,
            'format_type': format_type,
            'part_number': part_number,
            'version_number': version_number,
            'milestone': milestone,
            'file_path': file_path,
            'created_date': timestamp_str
        }

        # Update the database
        try:
            with self.__connection():
                self.__updateDbRecord(record)
            return True
        except Exception as e:
            logger.error(f"Failed to add/update file record for {file_path}: {str(e)}")
            return False

    def getFileTimestamp(self, file_path: str) -> Optional[datetime]:
        """
        Get the timestamp for a file from the database.

        This method retrieves the timestamp for a file from the database
        based on the file's metadata extracted from its path.

        Note: This method does not check if tracking is enabled.

        Args:
            file_path (str): Path to the file

        Returns:
            Optional[datetime]: Timestamp from the database if found, None otherwise

        Raises:
            Exception: For database operation failures
        """
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            deposition_id, content_type, format_type, part_number, _ = metadata_tuple

            if None in (deposition_id, content_type, format_type, part_number):
                logger.warning(f"File {file_path} doesn't follow OneDep naming convention")
                return None
        except Exception as e:
            logger.warning(f"Failed to parse file path {file_path}: {str(e)}")
            return None

        query = f"""
            SELECT created_date FROM {self.TABLE_NAME}
            WHERE deposition_id = '{deposition_id}' AND content_type = '{content_type}'
            AND format_type = '{format_type}' AND part_number = {part_number};
        """

        try:
            with self.__connection():
                if self.__verbose:
                    logger.debug(f"Executing query: {query}")
                result = self.__myQuery.selectRows(query)

                if result and result[0][0]:
                    # Convert string timestamp to datetime object
                    timestamp_str = result[0][0]
                    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                return None
        except Exception as e:
            logger.error(f"Failed to get timestamp for {file_path}: {str(e)}")
            return None

    def updateFileTimestamp(self, file_path: str, timestamp: datetime) -> bool:
        """
        Update just the timestamp for a file in the database.

        This is a lightweight operation to fix inconsistencies between the
        filesystem and database timestamps.

        Args:
            file_path (str): Path to the file
            timestamp (datetime): New timestamp to set

        Returns:
            bool: True if the timestamp was successfully updated, False otherwise

        Raises:
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.debug("File activity tracking is disabled in site configuration")
            return True

        # Try using PathInfo first for parsing metadata
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            deposition_id, content_type, format_type, part_number, _ = metadata_tuple

            if None in (deposition_id, content_type, format_type, part_number):
                logger.warning(f"File {file_path} doesn't follow OneDep naming convention")
                return False
        except Exception as e:
            logger.warning(f"Failed to parse file path {file_path}: {str(e)}")
            return False

        # Format timestamp for SQL
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        update_sql = f"""
            UPDATE {self.TABLE_NAME}
            SET created_date = '{timestamp_str}'
            WHERE deposition_id = '{deposition_id}' AND content_type = '{content_type}'
            AND format_type = '{format_type}' AND part_number = {part_number};
        """

        try:
            with self.__connection():
                if self.__verbose:
                    logger.debug(f"Executing update: {update_sql}")
                self.__myQuery.sqlCommand([update_sql])

                # Check if the record exists after update
                check_sql = f"""
                    SELECT COUNT(*) FROM {self.TABLE_NAME}
                    WHERE deposition_id = '{deposition_id}' AND content_type = '{content_type}'
                    AND format_type = '{format_type}' AND part_number = {part_number};
                """

                result = self.__myQuery.selectRows(check_sql)
                return result and result[0][0] > 0
        except Exception as e:
            logger.error(f"Failed to update timestamp for {file_path}: {str(e)}")
            return False
