##
# File:    AdvancedWriter.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#   - Removed create_dirs parameter from write() method
#   - Updated to match regular write() return value (number of characters written)
#   - Added update of DB timestamp when filesystem and DB are inconsistent
#   - Added proper exception handling to match standard write() behavior
#   - Converted method names to camelCase style to match FileActivityDb
#   - Reduced logging to only warn on failures to update DB timestamps
##
"""
Module implementing advanced file writing with automatic database tracking.

This module provides support for writing files with automatic tracking
in the file activity database in the OneDep system.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import sys
from datetime import datetime
from typing import Optional, Union, Tuple, Dict, Any, List, NoReturn, TextIO

from wwpdb.utils.db.FileActivityDb import FileActivityDb

logger = logging.getLogger(__name__)

class AdvancedWriter:
    """
    A class for writing files with automatic tracking in the file activity database.
    """

    def __init__(self, file_db: Optional[FileActivityDb] = None) -> None:
        """
        Initialize with optional FileActivityDb instance.

        Args:
            file_db: Optional FileActivityDb instance. If None, a new one is created.
        """
        self.__file_db = file_db if file_db is not None else FileActivityDb()
        self.__debug: bool = False

    def setDebug(self, flag: bool = True) -> None:
        """
        Set debug mode.

        Args:
            flag: Boolean debug flag
        """
        self.__debug = flag

    def write(self, file_path: str, data: str, mode: str = 'w') -> int:
        """
        Write data to file with tracking in file activity database.

        Args:
            file_path: Path to file
            data: Content to write (text only)
            mode: Write mode ('w' or 'a')

        Returns:
            int: Number of characters written (matching standard write() behavior)

        Raises:
            ValueError: If mode is not supported or parent directory doesn't exist
            IOError: If file writing fails
        """
        # PARAMETER VALIDATION

        # Validate the mode
        if mode not in ['w', 'a']:
            raise ValueError(f"Unsupported mode: {mode}. Mode must be one of: 'w', 'a'")

        # Check if file exists for append modes
        if mode == 'a' and not os.path.exists(file_path):
            logger.warning(f"Appending to non-existent file {file_path}. New file will be created.")

        # Check if parent directory exists
        parent_dir = os.path.dirname(file_path)
        if parent_dir and not os.path.exists(parent_dir):
            raise ValueError(f"Directory {parent_dir} does not exist. Directory must be created before writing file.")

        # DATABASE CONSISTENCY CHECKS

        # Get file's timestamp from filesystem (if it exists)
        file_timestamp: Optional[datetime] = None
        if os.path.exists(file_path):
            file_timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

            # Parse file metadata based on filename for DB checks
            file_metadata = self.__file_db.parseFileMetadata(os.path.basename(file_path))

            if file_metadata:
                # Check if file exists in database
                db_timestamp = self.getDbTimestamp(file_path)

                if db_timestamp is None:
                    # Only log debug information if debug mode is enabled
                    if self.__debug:
                        logger.debug(f"File {file_path} exists in filesystem but is not tracked in database. "
                                    "It will be added after the write operation.")
                elif db_timestamp != file_timestamp:
                    # Only log debug information if debug mode is enabled
                    if self.__debug:
                        logger.debug(f"Timestamp mismatch for {file_path}:")
                        if file_timestamp > db_timestamp:
                            logger.debug(f"Filesystem has newer version ({file_timestamp}) than database ({db_timestamp})")
                        else:
                            logger.debug(f"Filesystem has older version ({file_timestamp}) than database ({db_timestamp})")

                    # Update DB with current timestamp to fix inconsistency
                    update_success = self.updateTimestampInDb(file_path, file_timestamp)
                    # Only log warnings on failure to update timestamp
                    if not update_success:
                        logger.warning(f"Failed to update timestamp in DB for {file_path}")

        # WRITE OPERATION
        chars_written: int = 0
        try:
            # Open the file and write string data
            with open(file_path, mode, encoding='utf-8') as f:
                if not isinstance(data, str):
                    data = str(data)
                chars_written = f.write(data)

        except IOError as e:
            logger.error(f"Failed to write to {file_path}: {str(e)}")
            raise  # Re-raise the exception to maintain same behavior as regular write()

        # DATABASE UPDATE

        # Get timestamp of newly written file
        new_timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))

        # Parse file metadata based on filename
        file_metadata = self.__file_db.parseFileMetadata(os.path.basename(file_path))

        if file_metadata:
            # Try to update database with new file info
            try:
                # Create record from metadata
                record = self.createDbRecord(file_path, file_metadata, new_timestamp)
                # Update database record - we don't use the result so no assignment
                update_success = self.updateRecordInDb(record)
                # Only log warnings on failure to update record
                if not update_success and self.__debug:
                    logger.warning(f"Failed to update file record in DB for {file_path}")
            except Exception as e:
                # Log error but don't fail the operation
                logger.error(f"Failed to update database for {file_path}: {str(e)}")
                # In the future, this could raise an exception

        # Return number of characters written to match standard write() behavior
        return chars_written

    def getDbTimestamp(self, file_path: str) -> Optional[datetime]:
        """
        Get the timestamp for file from database.

        Args:
            file_path: Path to the file

        Returns:
            datetime object if found in database, None otherwise
        """
        # Use the new public method from FileActivityDb
        return self.__file_db.getFileTimestamp(file_path)

    def updateTimestampInDb(self, file_path: str, timestamp: datetime) -> bool:
        """
        Update the timestamp for file in database.

        Args:
            file_path: Path to the file
            timestamp: New timestamp to set

        Returns:
            Boolean success flag
        """
        # Use the new public method from FileActivityDb
        return self.__file_db.updateFileTimestamp(file_path, timestamp)

    def createDbRecord(self, file_path: str, file_metadata: Tuple, timestamp: datetime) -> Dict[str, Any]:
        """
        Create a database record from file metadata.

        Args:
            file_path: Path to the file
            file_metadata: Metadata tuple from parseFileMetadata
            timestamp: File timestamp

        Returns:
            Dictionary containing record data
        """
        # Use the new public method from FileActivityDb
        return self.__file_db.createFileRecord(file_path, file_metadata, timestamp)

    def updateRecordInDb(self, record: Dict[str, Any]) -> bool:
        """
        Update a record in the database.

        Args:
            record: Record dictionary to update

        Returns:
            Boolean success flag
        """
        # Use the new public method from FileActivityDb
        try:
            # Extract the file path and timestamp from the record
            file_path = record['file_path']

            # Extract timestamp from the record
            timestamp_str = record.get('created_date')
            if not timestamp_str:
                logger.warning(f"No timestamp found in record for {file_path}, using current time")
                timestamp = datetime.now()
            else:
                # Convert the timestamp string to a datetime object
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    logger.warning(f"Invalid timestamp format in record: {timestamp_str}, using current time")
                    timestamp = datetime.now()

            return self.__file_db.addFileRecord(file_path, timestamp)
        except Exception as e:
            logger.error(f"Failed to update record in database: {str(e)}")
            return False

    def close(self) -> None:
        """Close the database connection if it was created by this instance"""
        if hasattr(self, "__file_db") and self.__file_db:
            self.__file_db.close()

    def __enter__(self) -> 'AdvancedWriter':
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit"""
        self.close()