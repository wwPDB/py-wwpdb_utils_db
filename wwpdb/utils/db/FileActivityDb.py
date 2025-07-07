##
# File:    FileActivityDb.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-04-08
#
# Updates:
#   - Added configuration check methods
#   - Improved error handling and logging
#   - Refactored to use FileActivityDbCore and FileMetadataParser
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
from types import TracebackType
from typing import List, Optional, Tuple, Dict, Any, TextIO, Union, Type

from wwpdb.utils.config.ConfigInfo import getSiteId
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppCommon
from wwpdb.utils.db.FileActivityDbCore import FileActivityDbCore
from wwpdb.utils.db.FileMetadataParser import FileMetadataParser
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
        self.__siteId = siteId if siteId is not None else getSiteId()
        self.__lfh = log

        # Initialize the core database handler, metadata parser, and path info
        self.__db_core = FileActivityDbCore(siteId=self.__siteId, verbose=verbose, log=log)
        self.__metadata_parser = FileMetadataParser(siteId=self.__siteId, verbose=verbose, log=log)
        self.__path_info = PathInfo(siteId=self.__siteId, verbose=verbose, log=log)

    def close(self) -> None:
        """
        Close the database connection.

        This should be called when done with database operations to free up resources.
        The connection will be reestablished automatically if needed.
        """
        self.__db_core.close()

    def __enter__(self) -> "FileActivityDb":
        """
        Context manager entry.

        Returns:
            FileActivityDb: self for use in with statement
        """
        # Initialize connection through db_core
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

    def logActivity(self, file_path: str, storage_type: str = "archive", timestamp: Optional[datetime] = None) -> bool:
        """
        Log a file activity.

        This method checks if tracking is enabled, then records the file activity
        in the database using a context manager to ensure proper connection handling.

        Args:
            file_path (str): Path to the file to log
            storage_type (str): Type of storage (archive, deposit, session). Defaults to "archive".
            timestamp (Optional[datetime]): File timestamp. If None, will use
                                            the file's modification time.

        Returns:
            bool: True if logging was successful or if logging is disabled,
                  False if an error occurred during logging
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            return True

        # Skip transient directories
        if storage_type in ["session", "wf-instance"]:
            if self.__verbose and self.__lfh:
                self.__lfh.write(f"+FileActivityDb.logActivity Skipping transient directory file: {file_path}\n")
            return True

        try:
            # Log the file directly
            result = self.addFileRecord(file_path, storage_type, timestamp)

            if self.__verbose and self.__lfh:
                if result:
                    self.__lfh.write(f"+FileActivityDb.logActivity Successfully logged: {file_path} ({storage_type})\n")
                else:
                    self.__lfh.write(f"+FileActivityDb.logActivity Failed to log: {file_path}\n")

            return result
        except Exception as e:
            if self.__verbose and self.__lfh:
                self.__lfh.write(f"+FileActivityDb.logActivity Error logging file: {str(e)}\n")
            logger.error("Error logging file activity: %s", str(e))
            return False

    def populateFromDirectory(self, directory: str, ignore_storage_types: Optional[List[str]] = None) -> None:
        """
        Populate the database with file metadata from the given directory.

        Scans the directory and its immediate subdirectories for files matching
        the OneDep naming convention. For each unique file key, inserts or updates
        only the file with the highest version into the database.

        Args:
            directory (str): Path to the directory containing files to process
            ignore_storage_types (Optional[List[str]]): List of storage types to ignore.
                                                      Defaults to ["session", "wf-instance"].
                                                      Set to empty list to process all types.

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
            with self.__db_core._connection():
                if not os.path.isdir(directory):
                    raise ValueError("Invalid directory: %s" % directory)

                # No longer need site_id as it's been removed from schema

                # Set default storage types to ignore if none provided
                if ignore_storage_types is None:
                    ignore_storage_types = ["session", "wf-instance"]

                logger.debug("Scanning directory: %s", directory)

                # Determine storage_type from directory path
                storage_type = "archive"  # default
                if "/deposit/" in directory:
                    storage_type = "deposit"
                elif "/session/" in directory:
                    storage_type = "session"
                elif "/wf-instance/" in directory:
                    storage_type = "wf-instance"

                # Skip if storage type is in ignore list
                if storage_type in ignore_storage_types:
                    if self.__verbose and self.__lfh:
                        self.__lfh.write(f"+FileActivityDb.populateFromDirectory Skipping {storage_type} directory: {directory}\n")
                    return

                for subdir_entry in os.scandir(directory):
                    if subdir_entry.is_dir():
                        # Skip subdirectories with ignored storage types
                        subdir_storage_type = "archive"  # default
                        if "/deposit/" in subdir_entry.path:
                            subdir_storage_type = "deposit"
                        elif "/session/" in subdir_entry.path:
                            subdir_storage_type = "session"
                        elif "/wf-instance/" in subdir_entry.path:
                            subdir_storage_type = "wf-instance"

                        if subdir_storage_type in ignore_storage_types:
                            if self.__verbose and self.__lfh:
                                self.__lfh.write(f"+FileActivityDb.populateFromDirectory Skipping {subdir_storage_type} subdirectory: {subdir_entry.path}\n")
                            continue

                        logger.debug("Scanning subdirectory: %s", subdir_entry.path)
                        group_files: Dict[Tuple[str, str, str, int, str], Dict[str, Any]] = {}
                        for file_entry in os.scandir(subdir_entry.path):
                            if file_entry.is_file():
                                logger.debug("Processing file: %s", file_entry.name)
                                try:
                                    # Parse file metadata using the metadata parser
                                    file_key = self.__metadata_parser.extractFileKey(file_entry.path)
                                    if file_key is None or None in file_key[0:4]:
                                        logger.warning("Skipping unrecognized file: %s", file_entry.path)
                                        continue

                                    deposition_id, content_type, format_type, part_number, version_number = file_key

                                    if version_number is None:
                                        version_number = 1

                                    key = (deposition_id, content_type, format_type, part_number, storage_type)
                                    created_date = datetime.fromtimestamp(file_entry.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S")

                                    if key not in group_files or group_files[key]["version_number"] < version_number:
                                        group_files[key] = {
                                            "deposition_id": deposition_id,
                                            "content_type": content_type,
                                            "format_type": format_type,
                                            "part_number": part_number,
                                            "version_number": version_number,
                                            "storage_type": storage_type,
                                            "created_date": created_date,
                                        }
                                        logger.debug("Updated group_files with: %s", group_files[key])
                                except Exception as e:
                                    logger.warning("Error processing file %s: %s", file_entry.path, str(e))

                        self.__updateDbRecords(group_files)

                logger.info("Successfully loaded the latest version files from %s into the database.", directory)
        except Exception as e:
            logger.error("Error loading files from directory: %s", e)
            raise

    def __updateDbRecordInternal(self, record: Dict[str, Any]) -> None:
        """
        Internal version of update record function without connection management.

        Used by methods that already have an active connection context.

        Args:
            record (Dict[str, Any]): Dictionary containing the file record data

        Raises:
            Exception: For database operation failures
        """
        # site_id is no longer used as it's been removed from the schema

        table_name = self.__db_core.getTableName()

        # Query to check for existing record
        check_sql = f"""
            SELECT version_number, created_date FROM {table_name}
            WHERE deposition_id = %s
            AND content_type = %s
            AND format_type = %s
            AND part_number = %s
            AND storage_type = %s;
        """

        check_params = (
            record["deposition_id"],
            record["content_type"],
            record["format_type"],
            record["part_number"],
            record["storage_type"],
        )

        if self.__verbose:
            logger.debug("Executing check SQL: %s", check_sql)

        # Execute the SQL to check for existing record using our helper method
        existing_record = self.__db_core._executeSelectQuery(check_sql, check_params)

        if not existing_record or int(existing_record[0][0]) < record["version_number"]:
            # Use the new record's timestamp since we're updating to latest version
            created_date = record["created_date"]

            # Create the SQL statement for insert/update operation
            insert_sql = f"""
                INSERT INTO {table_name}
                (deposition_id, content_type, format_type, part_number,
                 version_number, storage_type, created_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    version_number = VALUES(version_number),
                    created_date = VALUES(created_date),
                    storage_type = VALUES(storage_type);
            """

            insert_params = (
                record["deposition_id"],
                record["content_type"],
                record["format_type"],
                record["part_number"],
                record["version_number"],
                record["storage_type"],
                created_date,
            )

            if self.__verbose:
                logger.debug("Executing insert SQL: %s", insert_sql)

            # Use our helper method for UPDATE/INSERT operations
            self.__db_core._executeUpdateQuery(insert_sql, insert_params)

    def __updateDbRecord(self, record: Dict[str, Any]) -> None:
        """
        Update a single database record with connection management.

        Args:
            record (Dict[str, Any]): Dictionary containing the file record data

        Raises:
            Exception: For database operation failures
        """
        with self.__db_core._connection():
            self.__updateDbRecordInternal(record)

    def __updateDbRecords(self, group_files: Dict[Tuple[str, str, str, int, str], Dict[str, Any]]) -> None:
        """
        Update database records for a group of files.

        For each file record, checks if it exists in the database and updates it
        if the new version is higher than the existing one.

        Args:
            group_files (Dict[Tuple[str, str, str, int, str], Dict[str, Any]]): Dictionary of file records grouped by key

        Raises:
            Exception: For database operation failures
        """
        for record in group_files.values():
            self.__updateDbRecord(record)

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

    def getFileActivity(
        self, hours: Optional[int] = None, days: Optional[int] = None, deposition_ids: str = "ALL", file_types: str = "ALL", formats: str = "ALL", storage_types: str = "ALL"
    ) -> List[str]:
        """
        Retrieve file activity based on specified criteria.

        Args:
            hours (Optional[int]): Time range in hours
            days (Optional[int]): Time range in days
            deposition_ids (str): Deposition IDs to filter (ALL, single ID, range, or comma-separated list)
            file_types (str): File types to filter (ALL or comma-separated list)
            formats (str): File formats to filter (ALL or comma-separated list)
            storage_types (str): Storage types to filter (ALL or comma-separated list)

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

        # site_id has been removed from the schema

        # Calculate time range using helper method
        total_hours = self.__calculateTimeRange(hours, days)

        table_name = self.__db_core.getTableName()
        query_parts = [f"SELECT deposition_id, content_type, format_type, part_number, version_number, storage_type FROM {table_name} WHERE created_date >= DATE_SUB(NOW(), INTERVAL %s HOUR)"]
        params: List[Union[int, str]] = [total_hours]

        # No need to filter by site_id as it's been removed from schema

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
                # Map common file extensions to format_type values
                if fmt.lower() == 'cif':
                    mapped_fmt = 'pdbx'
                elif fmt.lower() == 'xml':
                    mapped_fmt = 'xml'
                elif fmt.lower() == 'json':
                    mapped_fmt = 'json'
                else:
                    mapped_fmt = fmt
                
                format_conditions.append("format_type = %s")
                params.append(mapped_fmt)
                
                if self.__verbose:
                    logger.debug("Mapped format '%s' to format_type '%s'", fmt, mapped_fmt)
            
            if format_conditions:
                query_parts.append("AND (%s)" % " OR ".join(format_conditions))

        # Handle storage types
        if storage_types.upper() != "ALL":
            storage_list = storage_types.split(",")
            storage_conditions = []
            for st in storage_list:
                st = st.strip()
                storage_conditions.append("storage_type = %s")
                params.append(st)
            if storage_conditions:
                query_parts.append("AND (%s)" % " OR ".join(storage_conditions))

        # Combine all query parts
        base_query = " ".join(query_parts)

        with self.__db_core._connection():
            try:
                # Log the constructed query and parameters for debugging
                if self.__verbose:
                    logger.debug("Executing query: %s", base_query)
                    logger.debug("With parameters: %s", params)

                # Use our helper method for parameterized SELECT query
                results = self.__db_core._executeSelectQuery(base_query, tuple(params))

                # Log the result count for debugging
                if self.__verbose:
                    logger.debug("Query returned %d results", len(results) if results else 0)

                # For empty results, check if the table exists and has data
                if not results and self.__verbose:
                    check_table_sql = f"SELECT COUNT(*) FROM {table_name}"
                    count_result = self.__db_core._executeSelectQuery(check_table_sql)
                    if count_result and count_result[0][0] == 0:
                        logger.info("The table %s exists but is empty", table_name)

                # If we have results, construct file paths using PathInfo.getFilePath
                # This dynamically reconstructs the file paths instead of storing them in the database
                file_paths = []
                path_info = self.__path_info  # Reuse the existing PathInfo instance

                for row in results:
                    deposition_id, content_type, format_type, part_number, version_number, storage_type = row

                    # Use PathInfo.getFilePath to consistently reconstruct file paths
                    # This ensures paths follow the OneDep conventions for all storage types
                    file_path = path_info.getFilePath(
                        dataSetId=deposition_id,      # e.g., "D_1000001"
                        contentType=content_type,     # e.g., "model"
                        formatType=format_type,       # e.g., "pdbx"
                        fileSource=storage_type,      # e.g., "archive"
                        versionId=str(version_number),# e.g., "3"
                        partNumber=str(part_number)  # e.g., "0"
                    )
                    file_paths.append(file_path)

                return file_paths
            except Exception as err:
                logger.error("Unable to retrieve changed files from %s: %s", table_name, err)
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

        with self.__db_core._connection():
            try:
                table_name = self.__db_core.getTableName()
                truncate_sql = f"TRUNCATE TABLE {table_name}"
                self.__db_core._executeUpdateQuery(truncate_sql)
                logger.info("Successfully purged all data from %s table.", table_name)
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

        with self.__db_core._connection():
            try:
                table_name = self.__db_core.getTableName()

                # First, get count of records to be deleted
                count_sql = f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE deposition_id = %s;
                """

                # Use our helper method to get results
                count_result = self.__db_core._executeSelectQuery(count_sql, (deposition_id,))
                record_count = count_result[0][0] if count_result else 0

                # Execute the delete with parameterized query
                delete_sql = f"""
                    DELETE FROM {table_name}
                    WHERE deposition_id = %s;
                """
                self.__db_core._executeUpdateQuery(delete_sql, (deposition_id,))

                logger.info("Successfully purged %d records for deposition ID: %s", record_count, deposition_id)
            except Exception as err:
                logger.error("Failed to purge data for deposition ID %s: %s", deposition_id, err)
                raise

    def displayActivity(self, hours: Optional[int] = None, days: Optional[int] = None) -> None:
        """
        Display formatted database contents.

        Prints a formatted table of database records matching the time criteria.
        Output format:
            dep_id, file_type, storage_type, last_timestamp

        Args:
            hours (Optional[int]): Time range in hours
            days (Optional[int]): Time range in days

        Raises:
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.warning("File activity tracking is disabled in site configuration. " "To enable tracking, set SITE_FILE_ACTIVITY_DB_SUPPORT=True in the configuration.")
            print("NOTE: File activity tracking is disabled in site configuration.")
            return

        total_hours = self.__calculateTimeRange(hours, days)
        table_name = self.__db_core.getTableName()

        query = f"""
            SELECT DISTINCT deposition_id, content_type, storage_type, created_date
            FROM {table_name}
            WHERE created_date >= DATE_SUB(NOW(), INTERVAL %s HOUR)
        """

        params: List[Union[int, str]] = [total_hours]

        with self.__db_core._connection():
            try:
                # Use our helper method for parameterized SELECT query
                results = self.__db_core._executeSelectQuery(query, tuple(params))

                if not results:
                    logger.info("No records found for the specified criteria.")
                    return

                # Print headers
                headers = ["dep_id", "file_type", "storage_type", "last_timestamp"]
                print(",".join(headers))

                # Print data
                for row in results:
                    print(",".join([str(row[0]), str(row[1]), str(row[2]), str(row[3])]))

            except Exception as err:
                logger.error("Failed to display database contents: %s", err)
                raise

    # -----------------------------------------
    # Public methods for file record management
    # -----------------------------------------

    def addFileRecord(self, file_path: str, storage_type: str = "archive", timestamp: Optional[datetime] = None) -> bool:
        """
        Add or update a file record in the database.

        This method parses the file metadata and adds or updates the record in the database.
        If the file doesn't follow the OneDep naming convention, the method returns False.

        Args:
            file_path (str): Path to the file
            storage_type (str): Type of storage (archive, deposit, session). Defaults to "archive".
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

        # Use the metadata parser to get a standardized record
        record = self.__metadata_parser.parseFilePath(file_path, storage_type, timestamp)
        if record is None:
            return False

        # Update the database
        try:
            with self.__db_core._connection():
                self.__updateDbRecordInternal(record)
            return True
        except Exception as e:
            logger.error("Failed to add/update file record for %s: %s", file_path, str(e))
            return False

    def getFileTimestamp(self, file_path: str, storage_type: str = "archive") -> Optional[datetime]:
        """
        Get the timestamp for a file from the database.

        This method retrieves the timestamp for a file from the database
        based on the file's metadata extracted from its path.

        Note: This method does not check if tracking is enabled.

        Args:
            file_path (str): Path to the file
            storage_type (str): Type of storage (archive, deposit, session). Defaults to "archive".

        Returns:
            Optional[datetime]: Timestamp from the database if found, None otherwise

        Raises:
            Exception: For database operation failures
        """
        # Use the metadata parser to extract file key components
        file_key = self.__metadata_parser.extractFileKey(file_path)
        if file_key is None or None in file_key[0:4]:
            logger.warning("File %s doesn't follow OneDep naming convention", file_path)
            return None

        deposition_id, content_type, format_type, part_number, _ = file_key
        table_name = self.__db_core.getTableName()

        query = f"""
            SELECT created_date FROM {table_name}
            WHERE deposition_id = %s AND content_type = %s
            AND format_type = %s AND part_number = %s
            AND storage_type = %s;
        """
        params = (deposition_id, content_type, format_type, part_number, storage_type)

        try:
            with self.__db_core._connection():
                if self.__verbose:
                    logger.debug("Executing query: %s", query)

                # Use the db_core for parameterized SELECT query
                results = self.__db_core._executeSelectQuery(query, params)
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

    def updateFileTimestamp(self, file_path: str, timestamp: datetime, storage_type: str = "archive") -> bool:
        """
        Update just the timestamp for a file in the database.

        This is a lightweight operation to fix inconsistencies between the
        filesystem and database timestamps.

        Args:
            file_path (str): Path to the file
            timestamp (datetime): New timestamp to set
            storage_type (str): Type of storage (archive, deposit, session). Defaults to "archive".

        Returns:
            bool: True if the timestamp was successfully updated, False otherwise

        Raises:
            Exception: For database operation failures
        """
        # Check if tracking is enabled
        if not self.isTrackingEnabled():
            logger.debug("File activity tracking is disabled in site configuration")
            return True

        # Use the metadata parser to extract file key components
        file_key = self.__metadata_parser.extractFileKey(file_path)
        if file_key is None or None in file_key[0:4]:
            logger.warning("File %s doesn't follow OneDep naming convention", file_path)
            return False

        deposition_id, content_type, format_type, part_number, _ = file_key
        table_name = self.__db_core.getTableName()

        # Format timestamp for SQL
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")

        # For UPDATE operation using parameterized query
        update_sql = f"""
            UPDATE {table_name}
            SET created_date = %s
            WHERE deposition_id = %s AND content_type = %s
            AND format_type = %s AND part_number = %s
            AND storage_type = %s;
        """

        update_params = (timestamp_str, deposition_id, content_type, format_type, part_number, storage_type)

        try:
            with self.__db_core._connection():
                if self.__verbose:
                    logger.debug("Executing update: %s", update_sql)
                # Use the db_core for parameterized updates
                self.__db_core._executeUpdateQuery(update_sql, update_params)

                # Check if the record exists after update
                check_sql = f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE deposition_id = %s AND content_type = %s
                    AND format_type = %s AND part_number = %s
                    AND storage_type = %s;
                """

                check_params = (deposition_id, content_type, format_type, part_number, storage_type)

                # Use the db_core to check if record exists
                results = self.__db_core._executeSelectQuery(check_sql, check_params)

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
