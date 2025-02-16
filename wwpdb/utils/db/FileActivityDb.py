##
# File:    FileActivityDb.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#
##
"""
Module implementing file activity database management operations.

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
from typing import List, Optional, Tuple, Dict, Any

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

    Attributes:
        myQuery (MyDbQuery): Database query object for executing SQL commands
    """

    def __init__(self):
        """
        Initialize the FileActivityDb instance.

        Sets up logging and establishes a database connection using wwPDB utilities.
        Raises an exception if the database connection cannot be established.
        """
        self.myQuery = None
        self.setupLogging()
        self.initializeDbConnection()

    def setupLogging(self):
        """
        Configure logging for database operations.

        Sets up basic logging with ERROR level and a timestamp format.
        """
        logging.basicConfig(
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def initializeDbConnection(self):
        """
        Initialize database connection using wwPDB utilities.

        Establishes a connection to the OneDep metadata database using configuration
        from ConfigInfo. Sets up the query object for database operations.

        Raises:
            Exception: If database connection fails or configuration is invalid
        """
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
            dbcon = myC.connect()
            if dbcon:
                self.myQuery = MyDbQuery(dbcon=dbcon, verbose=True, log=sys.stderr)
            else:
                raise Exception("Failed to establish database connection")
        except Exception as err:
            sys.stderr.write(f"ERROR: Unable to connect to the database. {err}\n")
            sys.exit(1)

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
            if not os.path.isdir(directory):
                raise ValueError(f"Invalid directory: {directory}")

            hostname = socket.gethostname()
            logging.debug(f"Scanning directory: {directory}")

            for subdir_entry in os.scandir(directory):
                if subdir_entry.is_dir():
                    logging.debug(f"Scanning subdirectory: {subdir_entry.path}")
                    group_files = {}  # key: (deposition_id, content_type, format_type, part_number)
                    for file_entry in os.scandir(subdir_entry.path):
                        if file_entry.is_file():
                            logging.debug(f"Processing file: {file_entry.name}")
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
                                    logging.debug(f"Updated group_files with: {group_files[key]}")
                            else:
                                logging.warning(f"Skipping unrecognized file: {file_entry.path}")

                    self._updateDbRecords(group_files, hostname)

            print(f"Successfully loaded the latest version files from {directory} into the database.")
        except Exception as e:
            logging.error(f"Error loading files from directory: {e}")
            print(f"ERROR: Unable to load files. {e}")

    def _updateDbRecords(self, group_files: Dict[Tuple, Dict], hostname: str) -> None:
        """
        Update database records for a group of files.

        For each file record, checks if it exists in the database and updates it
        if the new version is higher than the existing one.

        Args:
            group_files (Dict[Tuple, Dict]): Dictionary of file records grouped by key
            hostname (str): The current host's name for recording file location

        Raises:
            Exception: For database operation failures
        """
        for record in group_files.values():
            check_sql = """
                SELECT version_number, created_date FROM file_activity_log
                WHERE deposition_id = '{}' AND content_type = '{}'
                AND format_type = '{}' AND part_number = {};
            """.format(
                record['deposition_id'], record['content_type'],
                record['format_type'], record['part_number']
            )
            logging.debug(f"Executing check SQL: {check_sql}")
            existing_record = self.myQuery.selectRows(check_sql)

            if not existing_record or existing_record[0][0] < record['version_number']:
                insert_sql = """
                    INSERT INTO file_activity_log
                    (deposition_id, content_type, format_type, part_number,
                     version_number, milestone, location, site_id, metadata_json, created_date)
                    VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}', '{}', NULL, '{}')
                    ON DUPLICATE KEY UPDATE
                        version_number = VALUES(version_number),
                        location = VALUES(location);
                """.format(
                    record['deposition_id'], record['content_type'], record['format_type'],
                    record['part_number'], record['version_number'], record['milestone'],
                    record['file_path'], hostname,
                    existing_record[0][1] if existing_record else record['created_date']
                )
                logging.debug(f"Executing insert SQL: {insert_sql}")
                self.myQuery.sqlCommand([insert_sql])

    def getFileActivity(self, hours: Optional[int] = None, days: Optional[int] = None,
                         site_id: Optional[str] = None, deposition_ids: str = "ALL",
                         file_types: str = "ALL", formats: str = "ALL") -> List[str]:
        """
        Retrieve file activity based on specified criteria.
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
                    sys.stderr.write(f"ERROR: Invalid deposition-ids range '{deposition_ids}'. Valid options: ALL, D_XXXX, D_XXXX-D_YYYY, or comma separated list.\n")
                    sys.exit(1)
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

        try:
            results = self.myQuery.selectRows(base_query)
            return [row[0] for row in results] if results else []
        except Exception as err:
            sys.stderr.write(f"ERROR: Unable to retrieve changed files from file_activity_log. {err}\n")
            sys.exit(1)

    def _buildQuery(self, hours: int, site_id: Optional[str], deposition_ids: str,
                    file_types: str, formats: str) -> Tuple[str, List[Any]]:
        """
        This method is now deprecated in favor of direct query construction in getFileActivity
        """
        return "", []

    def purgeFileActivityDb(self, confirmed: bool = False) -> None:
        """
        Purge all data from the file activity database.

        This is a destructive operation that removes all records while maintaining
        the database structure. Requires explicit confirmation.

        Args:
            confirmed (bool): Must be True to execute the purge operation

        Raises:
            Exception: If confirmation is False or database operation fails
        """
        if not confirmed:
            sys.stderr.write("ERROR: Must provide --confirmed flag to purge database\n")
            sys.exit(1)

        try:
            self.myQuery.sqlCommand(["TRUNCATE TABLE file_activity_log;"])
            print("Successfully purged all data from file_activity_log table.")
        except Exception as err:
            sys.stderr.write(f"ERROR: Failed to purge database. {err}\n")
            sys.exit(1)

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

        try:
            results = self.myQuery.selectRows(query)
            if not results:
                print("No records found for the specified criteria.")
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
            sys.stderr.write(f"ERROR: Failed to display database contents. {err}\n")
            sys.exit(1)