##
# File:    FileActivityDb.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#   - Added a __closed flag to prevent reinitialization after close()
#   - Modified __connection and __initializeDbConnection to check for closed status
#   - Tightened deposition ID validation in purgeDepositionData to only allow IDs starting with "D_100" followed by 7 digits.
##
"""
Module implementing file activity database operations.

This module provides support for tracking and managing file activities
in the OneDep system database.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import re
import socket
import sys
from datetime import datetime
from contextlib import contextmanager
from typing import List, Optional, Tuple, Dict, Any, Union, NoReturn, Generator

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery

logger = logging.getLogger(__name__)

class FileActivityDb:
    """
    A class to manage file activity database operations in the OneDep system.

    This class provides methods for:
    - Loading file metadata into the database
    - Querying file changes based on various criteria
    - Displaying formatted database contents
    - Purging database records

    The class maintains a connection to the OneDep metadata database and provides
    both high-level operations and utility methods for database interactions.
    """

    def __init__(self) -> None:
        """
        Initialize the FileActivityDb instance.

        The database connection is not established during initialization.
        It will be established on first use.
        """
        self.__myQuery: Optional[MyDbQuery] = None
        self.__debug: bool = False
        self.__dbcon = None
        self.__closed: bool = False

    def __initializeDbConnection(self) -> None:
        """
        Initialize database connection using wwPDB utilities.

        Establishes a connection to the OneDep metadata database using configuration
        from ConfigInfo. Sets up the query object for database operations.
        This is called lazily when the connection is first needed.

        Raises:
            Exception: If database connection fails, configuration is invalid,
                       or the connection has already been closed.
        """
        if self.__closed:
            raise Exception("Database connection is closed")
        if self.__myQuery is not None:
            return

        try:
            config = ConfigInfo()
            myC = MyDbConnect(
                dbServer="mysql",
                dbHost=config.get("SITE_DB_HOST_NAME"),
                dbName="onedep_metadata",
                dbUser=config.get("SITE_DB_USER_NAME"),
                dbPw=config.get("SITE_DB_PASSWORD"),
                dbPort=str(config.get("SITE_DB_PORT_NUMBER")),
                dbSocket=config.get("SITE_DB_SOCKET"),
                verbose=True,
                log=sys.stderr
            )
            self.__dbcon = myC.connect()
            if self.__dbcon:
                self.__myQuery = MyDbQuery(dbcon=self.__dbcon, verbose=True, log=sys.stderr)
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
            Exception: If the database connection is closed or cannot be established.
        """
        if self.__closed:
            raise Exception("Database connection is closed")
        need_close = self.__myQuery is None  # Only close if we created the connection
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
        Once closed, the FileActivityDb instance cannot be reused.
        """
        if self.__myQuery is not None:
            try:
                if hasattr(self.__myQuery, 'close'):
                    self.__myQuery.close()
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

    def setDebug(self, flag: bool = True) -> None:
        """Set debug mode for the database operations.

        Args:
            flag (bool): Debug mode flag
        """
        self.__debug = flag

    def getDebug(self) -> bool:
        """Get current debug mode status.

        Returns:
            bool: Current debug mode status
        """
        return self.__debug

    @staticmethod
    def parseFileMetadata(file_name: str) -> Optional[Tuple[str, str, str, int, int, str]]:
        """
        Parse metadata from a filename following OneDep naming conventions.

        Expected format:
          D_XXXXXXXXXX_<content_type>-<milestone>_P<part>.<format>.V<version>[.gz]

        Args:
            file_name (str): The name of the file to parse

        Returns:
            Optional[Tuple[str, str, str, int, int, str]]: A tuple containing:
                - deposition_id: The D_XXXXXXXX identifier
                - content_type: The type of content
                - format_type: The file format
                - part_number: The part number as integer
                - version_number: The version number as integer
                - milestone: The milestone identifier
            Returns None if the filename doesn't match the expected pattern
        """
        if not file_name:
            return None
        pattern = re.compile(
            r"(D_\d+)_([a-zA-Z0-9-_]+)_P(\d+)\.(\w+)(?:\.V(\d+))?(?:\.(gz))?$"
        )
        match = pattern.match(file_name)
        if match:
            deposition_id, content_type_with_milestone, part_number, format_type, version_number, _ = match.groups()
            version_number = int(version_number) if version_number else 1
            parts = content_type_with_milestone.split("-")
            if len(parts) > 1:
                milestone = parts[-1]
                content_type = "-".join(parts[:-1])
            else:
                content_type = content_type_with_milestone
                milestone = "unknown"
            return deposition_id, content_type, format_type, int(part_number), version_number, milestone
        return None

    def populateFileActivityDb(self, directory: str) -> None:
        """
        Populate the database with file metadata from the given directory.

        Scans the directory and its immediate subdirectories for files matching
        the OneDep naming convention. For each unique file key, inserts or updates
        only the file with the highest version into the database.

        Args:
            directory (str): Path to the directory containing files to process

        Raises:
            ValueError: If the directory is invalid or inaccessible
            Exception: For database operation failures
        """
        try:
            with self.__connection():
                if not os.path.isdir(directory):
                    raise ValueError(f"Invalid directory: {directory}")

                site_id = None  # Initialize site_id to None for now
                logger.debug("Scanning directory: %s", directory)

                for subdir_entry in os.scandir(directory):
                    if subdir_entry.is_dir():
                        logger.debug("Scanning subdirectory: %s", subdir_entry.path)
                        group_files: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}
                        for file_entry in os.scandir(subdir_entry.path):
                            if file_entry.is_file():
                                logger.debug("Processing file: %s", file_entry.name)
                                metadata = self.parseFileMetadata(file_entry.name)
                                if metadata:
                                    deposition_id, content_type, format_type, part_number, version_number, milestone = metadata
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
                                else:
                                    logger.warning("Skipping unrecognized file: %s", file_entry.path)

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
            site_id (Optional[str]): The site identifier for recording file location. Defaults to None.

        Raises:
            Exception: For database operation failures
        """
        check_sql = """
            SELECT version_number, created_date FROM file_activity_log
            WHERE deposition_id = '{}' AND content_type = '{}'
            AND format_type = '{}' AND part_number = {};
        """.format(
            record['deposition_id'], record['content_type'],
            record['format_type'], record['part_number']
        )
        if self.__debug:
            logger.debug("Executing check SQL: %s", check_sql)
        existing_record = self.__myQuery.selectRows(check_sql)

        if not existing_record or existing_record[0][0] < record['version_number']:
            insert_sql = """
                INSERT INTO file_activity_log
                (deposition_id, content_type, format_type, part_number,
                 version_number, milestone, location, site_id, metadata_json, created_date)
                VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}', {}, NULL, '{}')
                ON DUPLICATE KEY UPDATE
                    version_number = VALUES(version_number),
                    location = VALUES(location);
            """.format(
                record['deposition_id'], record['content_type'], record['format_type'],
                record['part_number'], record['version_number'], record['milestone'],
                record['file_path'],
                f"'{site_id}'" if site_id is not None else 'NULL',
                existing_record[0][1] if existing_record else record['created_date']
            )
            if self.__debug:
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
            site_id (Optional[str]): Site ID to filter results
            deposition_ids (str): Deposition IDs to filter (ALL, single ID, range, or comma-separated list)
            file_types (str): File types to filter (ALL or comma-separated list)
            formats (str): File formats to filter (ALL or comma-separated list)

        Returns:
            List[str]: List of file locations matching the criteria

        Raises:
            Exception: If there's an error processing the query
        """
        total_hours = hours if hours is not None else days * 24
        base_query = f"SELECT location FROM file_activity_log WHERE created_date >= DATE_SUB(NOW(), INTERVAL {total_hours} HOUR)"

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
                self.__myQuery.sqlCommand(["TRUNCATE TABLE file_activity_log;"])
                logger.info("Successfully purged all data from file_activity_log table.")
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

        # Validate deposition ID format - must be D_ followed by "100" and exactly 7 digits
        if not re.match(r"^D_100\d{7}$", deposition_id):
            logger.error("Invalid deposition ID format: %s. Expected format: D_100XXXXXXXX", deposition_id)
            raise ValueError(f"Invalid deposition ID format: {deposition_id}")

        with self.__connection():
            try:
                # First, get count of records to be deleted
                count_sql = f"""
                    SELECT COUNT(*) FROM file_activity_log
                    WHERE deposition_id = '{deposition_id}';
                """
                count_result = self.__myQuery.selectRows(count_sql)
                record_count = count_result[0][0] if count_result else 0

                # Execute the delete
                delete_sql = f"""
                    DELETE FROM file_activity_log
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
        If site_id is not provided, it's omitted from the output.

        Args:
            hours (Optional[int]): Time range in hours
            days (Optional[int]): Time range in days
            site_id (Optional[str]): Site ID to filter results

        Raises:
            Exception: For database operation failures
        """
        total_hours = hours if hours is not None else (days * 24 if days is not None else 24)

        query = f"""
            SELECT DISTINCT site_id, deposition_id, content_type, created_date
            FROM file_activity_log
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
        # Get timestamp from filesystem if not provided
        if timestamp is None and os.path.exists(file_path):
            timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

        # Parse file metadata
        metadata = self.parseFileMetadata(file_path)
        if not metadata:
            logger.warning(f"File {file_path} doesn't follow OneDep naming convention, can't add to database")
            return False

        # Create record from metadata
        record = self.createFileRecord(file_path, metadata, timestamp)

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

        Args:
            file_path (str): Path to the file

        Returns:
            Optional[datetime]: Timestamp from the database if found, None otherwise

        Raises:
            Exception: For database operation failures
        """
        metadata = self.parseFileMetadata(file_path)
        if not metadata:
            logger.warning(f"File {file_path} doesn't follow OneDep naming convention")
            return None

        deposition_id, content_type, format_type, part_number, _, _ = metadata

        query = """
            SELECT created_date FROM file_activity_log
            WHERE deposition_id = '{}' AND content_type = '{}'
            AND format_type = '{}' AND part_number = {};
        """.format(deposition_id, content_type, format_type, part_number)

        try:
            with self.__connection():
                if self.__debug:
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
        metadata = self.parseFileMetadata(file_path)
        if not metadata:
            logger.warning(f"File {file_path} doesn't follow OneDep naming convention")
            return False

        deposition_id, content_type, format_type, part_number, _, _ = metadata

        # Format timestamp for SQL
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        update_sql = """
            UPDATE file_activity_log
            SET created_date = '{}'
            WHERE deposition_id = '{}' AND content_type = '{}'
            AND format_type = '{}' AND part_number = {};
        """.format(timestamp_str, deposition_id, content_type, format_type, part_number)

        try:
            with self.__connection():
                if self.__debug:
                    logger.debug(f"Executing update: {update_sql}")
                self.__myQuery.sqlCommand([update_sql])

                # Check if the record exists instead of checking affected rows
                check_sql = """
                    SELECT COUNT(*) FROM file_activity_log
                    WHERE deposition_id = '{}' AND content_type = '{}'
                    AND format_type = '{}' AND part_number = {};
                """.format(deposition_id, content_type, format_type, part_number)

                result = self.__myQuery.selectRows(check_sql)
                return result and result[0][0] > 0
        except Exception as e:
            logger.error(f"Failed to update timestamp for {file_path}: {str(e)}")
            return False

    def createFileRecord(self, file_path: str, metadata: Tuple, timestamp: datetime) -> Dict[str, Any]:
        """
        Create a database record object from file metadata.

        This method standardizes record creation for consistent database entries.

        Args:
            file_path (str): Path to the file
            metadata (Tuple): Metadata tuple from parseFileMetadata
            timestamp (datetime): File timestamp

        Returns:
            Dict[str, Any]: Dictionary containing record data ready for database insertion

        Raises:
            ValueError: If metadata tuple doesn't have the expected format
        """
        if len(metadata) != 6:
            raise ValueError(f"Invalid metadata format: {metadata}")

        deposition_id, content_type, format_type, part_number, version_number, milestone = metadata

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

        return record
