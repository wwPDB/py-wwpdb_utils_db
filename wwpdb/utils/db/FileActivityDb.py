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
import sys
from datetime import datetime
from contextlib import contextmanager
from types import TracebackType
from typing import List, Optional, Tuple, Dict, Any, Generator, TextIO, Union, cast, Type

from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppCommon
from wwpdb.utils.db.MyDbUtil import MyDbConnect
from wwpdb.io.locator.PathInfo import PathInfo

logger = logging.getLogger(__name__)


class FileActivityDb:
    """
    Database manager for tracking file activity in the OneDep system.

    This class provides a robust interface for file activity tracking with:
    - Parameterized SQL queries for security (no SQL injection risks)
    - Efficient connection management with contextmanager pattern
    - Comprehensive error handling and logging
    - Lazy database connection initialization

    Main capabilities:
    - Recording file metadata in the database
    - Querying recent file changes with flexible criteria
    - Adding/updating/retrieving file records with proper metadata parsing
    - Purging database records (all or by dataset)
    - Displaying formatted database contents for reporting

    The class uses the wwPDB configuration system to determine database
    connection parameters and tracks files according to OneDep naming conventions.
    """

    # This will be loaded from configuration in __init__
    __table_name: str = ""

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
        self.__verbose: bool = verbose
        self.__dbcon = None
        self.__closed: bool = True  # Start with no connection
        self.__siteId = siteId if siteId is not None else getSiteId()
        self.__path_info = PathInfo(siteId=self.__siteId, verbose=verbose, log=log)
        self.__lfh = log

        # Load table name from configuration
        config = ConfigInfo()
        self.__table_name = config.get("SITE_DB_FILE_ACTIVITY_TABLE_NAME", "file_activity_log")

    def __initializeDbConnection(self) -> None:
        """
        Initialize database connection using wwPDB utilities.

        Establishes a connection to the OneDep metadata database using configuration
        from ConfigInfo. This is called lazily when the connection is first needed.

        Raises:
            Exception: If database connection fails or configuration is invalid.
        """
        if not self.__closed:
            return  # Connection already open

        try:
            config = ConfigInfo()

            # Get database configuration from site configuration - no fallbacks
            db_name = config.get("SITE_DB_FILE_ACTIVITY_DB_NAME")
            db_host = config.get("SITE_DA_FILE_ACTIVITY_DB_HOST_NAME")
            db_port = str(config.get("SITE_DA_FILE_ACTIVITY_DB_NUMBER"))
            db_socket = config.get("SITE_DA_FILE_ACTIVITY_DB_SOCKET")
            db_user = config.get("SITE_DB_USER_NAME")
            db_pw = config.get("SITE_DB_PASSWORD")

            myC = MyDbConnect(  # type: ignore
                dbServer="mysql",
                dbHost=db_host,
                dbName=db_name,
                dbUser=db_user,
                dbPw=db_pw,
                dbPort=db_port,
                dbSocket=db_socket,
                verbose=self.__verbose,
                log=self.__lfh,
            )
            self.__dbcon = myC.connect()  # type: ignore
            if self.__dbcon:
                self.__closed = False  # Mark connection as open
            else:
                raise Exception("Failed to establish database connection")
        except Exception as err:
            logger.error("Unable to connect to the database: %s", err)
            raise

    def __executeSelectQuery(self, query: str, params: Optional[Union[Tuple[Any, ...], List[Any], Dict[str, Any]]] = None) -> List[Tuple[Any, ...]]:
        """
        Execute a SELECT query with parameters and return results.

        Handles proper cursor management and exception handling.
        Assumes a valid database connection exists.

        Args:
            query: SQL query string with %s placeholders
            params: Tuple, list or dict of parameter values

        Returns:
            List of result tuples or empty list on error
        """
        cursor = None
        try:
            if self.__dbcon is None:
                logger.error("Database connection is not initialized")
                return []

            cursor = self.__dbcon.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return cast(List[Tuple[Any, ...]], results)
        except Exception as e:
            logger.error("Database error executing SELECT: %s", str(e))
            if cursor is not None:
                cursor.close()
            return []

    def __executeUpdateQuery(self, query: str, params: Optional[Union[Tuple[Any, ...], List[Any], Dict[str, Any]]] = None) -> bool:
        """
        Execute an UPDATE/INSERT/DELETE query with parameters.

        Handles proper cursor management, transaction management and exception handling.
        Assumes a valid database connection exists.

        Args:
            query: SQL query string with %s placeholders
            params: Tuple, list or dict of parameter values

        Returns:
            Boolean indicating success/failure
        """
        cursor = None
        try:
            if self.__dbcon is None:
                logger.error("Database connection is not initialized")
                return False

            cursor = self.__dbcon.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.__dbcon.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error("Database error executing UPDATE: %s", str(e))
            if self.__dbcon is not None:
                self.__dbcon.rollback()
            if cursor is not None:
                cursor.close()
            return False

    @contextmanager
    def __connection(self) -> Generator[None, None, None]:
        """
        Context manager for database connection lifecycle.

        Ensures connection is established before operation and closed after operation
        if it was newly created. The connection is lazy-initialized and only closed
        if this context manager created it.

        Usage:
            with self.__connection():
                # Database operations using helper methods
                self.__executeSelectQuery(...)
                self.__executeUpdateQuery(...)

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
                self.__dbcon = None
                self.__closed = True

    def __enter__(self) -> "FileActivityDb":
        """
        Context manager entry.

        Returns:
            FileActivityDb: self for use in with statement
        """
        self.__initializeDbConnection()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
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
            support_value = config_app.get_file_activity_db_support()

            # Debug output
            if self.__verbose:
                logger.debug("File activity tracking config value: %s", support_value)

            # Cast the return value to bool to satisfy typechecking
            # If None is returned, cast will make it False
            return bool(support_value)
        except Exception as e:
            logger.warning("Error checking file activity configuration: %s", str(e))
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
                        self.__lfh.write("+FileActivityDb.logActivity Successfully logged: %s\n" % file_path)
                    else:
                        self.__lfh.write("+FileActivityDb.logActivity Failed to log: %s\n" % file_path)

                return result
        except Exception as e:
            if self.__verbose and self.__lfh:
                self.__lfh.write("+FileActivityDb.logActivity Error logging file: %s\n" % str(e))
            logger.error("Error logging file activity: %s", str(e))
            return False

    def populateFromDirectory(self, directory: str, site_id: Optional[str] = None) -> None:
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
            logger.warning("File activity tracking is disabled in site configuration. " "To enable tracking, set SITE_FILE_ACTIVITY_DB_SUPPORT=True in the configuration.")
            print("NOTE: File activity tracking is disabled in site configuration.")
            return

        try:
            with self.__connection():
                if not os.path.isdir(directory):
                    raise ValueError("Invalid directory: %s" % directory)

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
                                    created_date = datetime.fromtimestamp(file_entry.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S")

                                    if key not in group_files or group_files[key]["version_number"] < version_number:
                                        group_files[key] = {
                                            "deposition_id": deposition_id,
                                            "content_type": content_type,
                                            "format_type": format_type,
                                            "part_number": part_number,
                                            "version_number": version_number,
                                            "milestone": milestone,
                                            "file_path": file_entry.path,
                                            "created_date": created_date,
                                        }
                                        logger.debug("Updated group_files with: %s", group_files[key])
                                except Exception as e:
                                    logger.warning("Error processing file %s: %s", file_entry.path, str(e))

                        self.__updateDbRecords(group_files, site_id)

                logger.info("Successfully loaded the latest version files from %s into the database.", directory)
        except Exception as e:
            logger.error("Error loading files from directory: %s", e)
            raise

    def __updateDbRecordInternal(self, record: Dict[str, Any], site_id: Optional[str] = None) -> None:
        """
        Internal version of update record function without connection management.

        Used by methods that already have an active connection context.

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
            SELECT version_number, created_date FROM {self.__table_name}
            WHERE deposition_id = %s
            AND content_type = %s
            AND format_type = %s
            AND part_number = %s;
        """

        check_params = (record["deposition_id"], record["content_type"], record["format_type"], record["part_number"])

        if self.__verbose:
            logger.debug("Executing check SQL: %s", check_sql)

        # Execute the SQL to check for existing record using our helper method
        existing_record = self.__executeSelectQuery(check_sql, check_params)

        if not existing_record or int(existing_record[0][0]) < record["version_number"]:
            # Use the new record's timestamp since we're updating to latest version
            created_date = record["created_date"]

            # Create the SQL statement for insert/update operation
            insert_sql = f"""
                INSERT INTO {self.__table_name}
                (deposition_id, content_type, format_type, part_number,
                 version_number, milestone, location, site_id, metadata_json, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    version_number = VALUES(version_number),
                    location = VALUES(location),
                    created_date = VALUES(created_date);
            """

            insert_params = (
                record["deposition_id"],
                record["content_type"],
                record["format_type"],
                record["part_number"],
                record["version_number"],
                record["milestone"],
                record["file_path"],
                site_id if site_id else "",
                None,  # metadata_json
                created_date,
            )

            if self.__verbose:
                logger.debug("Executing insert SQL: %s", insert_sql)

            # Use our helper method for UPDATE/INSERT operations
            self.__executeUpdateQuery(insert_sql, insert_params)

    def __updateDbRecord(self, record: Dict[str, Any], site_id: Optional[str] = None) -> None:
        """
        Update a single database record with connection management.

        Args:
            record (Dict[str, Any]): Dictionary containing the file record data
            site_id (Optional[str]): The site identifier for recording file location. Defaults to instance site_id.

        Raises:
            Exception: For database operation failures
        """
        with self.__connection():
            self.__updateDbRecordInternal(record, site_id)

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

    def getFileActivity(
        self, hours: Optional[int] = None, days: Optional[int] = None, site_id: Optional[str] = None, deposition_ids: str = "ALL", file_types: str = "ALL", formats: str = "ALL"
    ) -> List[str]:
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
            logger.warning("File activity tracking is disabled in site configuration. " "To enable tracking, set SITE_FILE_ACTIVITY_DB_SUPPORT=True in the configuration.")
            print("NOTE: File activity tracking is disabled in site configuration.")
            return []

        # Use the instance's site_id if none is provided
        if site_id is None:
            site_id = self.__siteId

        # Calculate time range using helper method
        total_hours = self.__calculateTimeRange(hours, days)

        query_parts = [f"SELECT location FROM {self.__table_name} WHERE created_date >= DATE_SUB(NOW(), INTERVAL %s HOUR)"]
        params: List[Union[int, str]] = [total_hours]

        # Add site_id filter if provided
        if site_id:
            query_parts.append("AND site_id = %s")
            params.append(site_id)

        # Handle deposition IDs
        if deposition_ids.upper() != "ALL":
            if "-" in deposition_ids:
                try:
                    start_id, end_id = deposition_ids.split("-")
                    start_num = int(start_id.replace("D_", ""))
                    end_num = int(end_id.replace("D_", ""))
                    query_parts.append("AND CAST(SUBSTRING(deposition_id, 3) AS UNSIGNED) BETWEEN %s AND %s")
                    params.extend([start_num, end_num])
                except Exception:
                    logger.error("Invalid deposition-ids range '%s'. Valid options: ALL, D_XXXX, D_XXXX-D_YYYY, or comma separated list.", deposition_ids)
                    raise ValueError("Invalid deposition-ids range")
            elif "," in deposition_ids:
                # For IN queries with variable number of items
                dep_list = [d.strip() for d in deposition_ids.split(",")]
                placeholders = ", ".join(["%s"] * len(dep_list))
                query_parts.append("AND deposition_id IN (%s)" % placeholders)
                params.extend(dep_list)
            else:
                query_parts.append("AND deposition_id = %s")
                params.append(deposition_ids)

        # Handle file types
        if file_types.upper() != "ALL":
            types_list = file_types.split(",")
            type_conditions = []
            for ft in types_list:
                ft = ft.strip()
                # Use exact match instead of LIKE with wildcard
                type_conditions.append("content_type = %s")
                params.append(ft)
            if type_conditions:
                query_parts.append("AND (%s)" % " OR ".join(type_conditions))

        # Handle formats
        if formats.upper() != "ALL":
            formats_list = formats.split(",")
            format_conditions = []
            for fmt in formats_list:
                fmt = fmt.strip()
                format_conditions.append("format_type = %s")
                params.append(fmt)
            if format_conditions:
                query_parts.append("AND (%s)" % " OR ".join(format_conditions))

        # Combine all query parts
        base_query = " ".join(query_parts)

        with self.__connection():
            try:
                # Log the constructed query and parameters for debugging
                if self.__verbose:
                    logger.debug("Executing query: %s", base_query)
                    logger.debug("With parameters: %s", params)

                # Use our helper method for parameterized SELECT query
                results = self.__executeSelectQuery(base_query, tuple(params))

                # Log the result count for debugging
                if self.__verbose:
                    logger.debug("Query returned %d results", len(results) if results else 0)

                # For empty results, check if the table exists and has data
                if not results and self.__verbose:
                    check_table_sql = f"SELECT COUNT(*) FROM {self.__table_name}"
                    count_result = self.__executeSelectQuery(check_table_sql)
                    if count_result and count_result[0][0] == 0:
                        logger.info("The table %s exists but is empty", self.__table_name)

                return [row[0] for row in results] if results else []
            except Exception as err:
                logger.error("Unable to retrieve changed files from %s: %s", self.__table_name, err)
                raise

    def purgeAllData(self, confirmed: bool = False) -> None:
        """
        Purge all data from the file activity database.

        This is a destructive operation that removes all records while maintaining
        the database structure. Requires explicit confirmation.

        Args:
            confirmed (bool): Must be True to execute the purge operation

        Raises:
            Exception: If confirmation is False or database operation fails

        See Also:
            purgeDataSetData: For purging data for a specific deposition ID
        """
        if not confirmed:
            logger.error("Must provide --confirmed flag to purge database")
            raise ValueError("Must provide --confirmed flag to purge database")

        with self.__connection():
            try:
                truncate_sql = f"TRUNCATE TABLE {self.__table_name}"
                self.__executeUpdateQuery(truncate_sql)
                logger.info("Successfully purged all data from %s table.", self.__table_name)
            except Exception as err:
                logger.error("Failed to purge database: %s", err)
                raise

    def purgeDataSetData(self, deposition_id: str, confirmed: bool = False) -> None:
        """
        Purge all data for a specific dataset ID from the file activity database.

        This is a destructive operation that removes all records for the specified
        deposition ID. Requires explicit confirmation to prevent accidental deletion.

        Args:
            deposition_id (str): The deposition ID to purge (e.g., "D_800000")
            confirmed (bool): Must be True to execute the purge operation

        Raises:
            ValueError: If confirmation is False
            Exception: For database operation failures
        """
        if not confirmed:
            logger.error("Must provide confirmed=True to purge dataset data")
            raise ValueError("Confirmation required for purge operation")

        # No regex check for development machines where ID range may be D_800000 to D_999999

        with self.__connection():
            try:
                # First, get count of records to be deleted
                count_sql = f"""
                    SELECT COUNT(*) FROM {self.__table_name}
                    WHERE deposition_id = %s;
                """

                # Use our helper method to get results
                count_result = self.__executeSelectQuery(count_sql, (deposition_id,))
                record_count = count_result[0][0] if count_result else 0

                # Execute the delete with parameterized query
                delete_sql = f"""
                    DELETE FROM {self.__table_name}
                    WHERE deposition_id = %s;
                """
                self.__executeUpdateQuery(delete_sql, (deposition_id,))

                logger.info("Successfully purged %d records for deposition ID: %s", record_count, deposition_id)
            except Exception as err:
                logger.error("Failed to purge data for deposition ID %s: %s", deposition_id, err)
                raise

    def __calculateTimeRange(self, hours: Optional[int] = None, days: Optional[int] = None) -> int:
        """
        Calculate time range in hours from hour or day parameters.

        Args:
            hours: Time range in hours (takes precedence if both are provided)
            days: Time range in days

        Returns:
            Time range in hours (defaults to 24 if neither parameter is provided)
        """
        if hours is not None:
            return hours
        elif days is not None:
            return days * 24
        else:
            return 24  # Default to last 24 hours

    def displayActivity(self, hours: Optional[int] = None, days: Optional[int] = None, site_id: Optional[str] = None) -> None:
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
            logger.warning("File activity tracking is disabled in site configuration. " "To enable tracking, set SITE_FILE_ACTIVITY_DB_SUPPORT=True in the configuration.")
            print("NOTE: File activity tracking is disabled in site configuration.")
            return

        # Use the instance's site_id if none is provided
        if site_id is None:
            site_id = self.__siteId

        total_hours = self.__calculateTimeRange(hours, days)

        query = f"""
            SELECT DISTINCT site_id, deposition_id, content_type, created_date
            FROM {self.__table_name}
            WHERE created_date >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        """

        params: List[Union[int, str]] = [total_hours]

        if site_id:
            query += " AND site_id = %s"
            params.append(site_id)

        with self.__connection():
            try:
                # Use our helper method for parameterized SELECT query
                results = self.__executeSelectQuery(query, tuple(params))

                if not results:
                    logger.info("No records found for the specified criteria.")
                    return

                # Print headers
                headers = ["site_id", "dep_id", "file_type", "last_timestamp"] if site_id else ["dep_id", "file_type", "last_timestamp"]
                print(",".join(headers))

                # Print data
                for row in results:
                    if site_id:
                        print(",".join([str(row[0]), str(row[1]), str(row[2]), str(row[3])]))
                    else:
                        print(",".join([str(row[1]), str(row[2]), str(row[3])]))

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

        # Ensure timestamp exists
        if timestamp is None:
            logger.warning("No timestamp available for file %s", file_path)
            return False

        # Try using PathInfo first for parsing metadata
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            deposition_id, content_type, format_type, part_number, version_number = metadata_tuple

            if None in (deposition_id, content_type, format_type, part_number):
                logger.warning("File %s doesn't follow OneDep naming convention, can't add to database", file_path)
                return False
            else:
                # Use PathInfo result but add milestone
                milestone = "unknown"  # PathInfo doesn't parse milestone
                if version_number is None:
                    version_number = 1
        except Exception as e:
            logger.warning("File %s couldn't be parsed: %s", file_path, str(e))
            return False

        # Format timestamp for database
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # Create the record dictionary
        record = {
            "deposition_id": deposition_id,
            "content_type": content_type,
            "format_type": format_type,
            "part_number": part_number,
            "version_number": version_number,
            "milestone": milestone,
            "file_path": file_path,
            "created_date": timestamp_str,
        }

        # Update the database
        try:
            with self.__connection():
                self.__updateDbRecordInternal(record)
            return True
        except Exception as e:
            logger.error("Failed to add/update file record for %s: %s", file_path, str(e))
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
                logger.warning("File %s doesn't follow OneDep naming convention", file_path)
                return None
        except Exception as e:
            logger.warning("Failed to parse file path %s: %s", file_path, str(e))
            return None

        query = f"""
            SELECT created_date FROM {self.__table_name}
            WHERE deposition_id = %s AND content_type = %s
            AND format_type = %s AND part_number = %s;
        """
        params = (deposition_id, content_type, format_type, part_number)

        try:
            with self.__connection():
                if self.__verbose:
                    logger.debug("Executing query: %s", query)

                # Use our helper method for parameterized SELECT query
                results = self.__executeSelectQuery(query, params)
                if not results:
                    return None

                # Get the first row
                result = results[0]

                if result and result[0]:
                    # Convert string timestamp to datetime object
                    timestamp_str = str(result[0])  # Ensure string type
                    return datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                return None
        except Exception as e:
            logger.error("Failed to get timestamp for %s: %s", file_path, str(e))
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
                logger.warning("File %s doesn't follow OneDep naming convention", file_path)
                return False
        except Exception as e:
            logger.warning("Failed to parse file path %s: %s", file_path, str(e))
            return False

        # Format timestamp for SQL
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # For UPDATE operation using parameterized query
        update_sql = f"""
            UPDATE {self.__table_name}
            SET created_date = %s
            WHERE deposition_id = %s AND content_type = %s
            AND format_type = %s AND part_number = %s;
        """

        update_params = (timestamp_str, deposition_id, content_type, format_type, part_number)

        try:
            with self.__connection():
                if self.__verbose:
                    logger.debug("Executing update: %s", update_sql)
                # Use our helper method for parameterized updates
                self.__executeUpdateQuery(update_sql, update_params)

                # Check if the record exists after update
                check_sql = f"""
                    SELECT COUNT(*) FROM {self.__table_name}
                    WHERE deposition_id = %s AND content_type = %s
                    AND format_type = %s AND part_number = %s;
                """

                check_params = (deposition_id, content_type, format_type, part_number)

                # Use our helper method to check if record exists
                results = self.__executeSelectQuery(check_sql, check_params)

                # Check if we got results and the count is > 0
                if results and len(results) > 0:
                    count = int(results[0][0])
                    return count > 0
                return False
        except Exception as e:
            logger.error("Failed to update timestamp for %s: %s", file_path, str(e))
            return False


if __name__ == "__main__":
    pass
