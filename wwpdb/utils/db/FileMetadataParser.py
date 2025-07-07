##
# File:    FileMetadataParser.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-04-08
#
# Updates:
#   - Initial implementation extracted from FileActivityDb.py
##
"""
File metadata parsing functionality for OneDep system.

This module provides utilities for parsing file metadata according
to OneDep naming conventions.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional, TextIO, Tuple

from wwpdb.io.locator.PathInfo import PathInfo

logger = logging.getLogger(__name__)


class FileMetadataParser:
    """
    Parse and extract metadata from file paths using OneDep naming conventions.
    
    This class provides methods to extract standardized metadata from file paths
    and create consistent record dictionaries for database operations.
    """
    
    def __init__(self, siteId: Optional[str] = None, verbose: bool = False, log: TextIO = sys.stderr) -> None:
        """
        Initialize the file metadata parser.
        
        Args:
            siteId (Optional[str]): Site identifier for configuration
            verbose (bool): Enable verbose output
            log (TextIO): Log file handle for verbose output
        """
        self.__siteId = siteId
        self.__verbose = verbose
        self.__lfh = log
        self.__path_info = PathInfo(siteId=siteId, verbose=verbose, log=log)
    
    def parseFilePath(self, file_path: str, storage_type: str = "archive", timestamp: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Parse a file path into standardized metadata.
        
        This method extracts metadata from a file path according to OneDep naming conventions
        and returns a dictionary containing the parsed metadata. The resulting metadata
        can be used with PathInfo.getFilePath() to reliably reconstruct the file path.
        
        Note: This method no longer includes the 'milestone' field, as milestone information
        is already implied in the content_type field.
        
        Args:
            file_path (str): Path to the file
            storage_type (str): Type of storage (archive, deposit, session)
            timestamp (Optional[datetime]): File timestamp. If None, will use
                                           the file's modification time.
                                           
        Returns:
            Optional[Dict[str, Any]]: Dictionary with standardized metadata keys
                                     or None if parsing fails
        """
        # Get timestamp from filesystem if not provided
        if timestamp is None and os.path.exists(file_path):
            timestamp = datetime.fromtimestamp(os.path.getmtime(file_path))
            
        # Ensure timestamp exists
        if timestamp is None:
            logger.warning("No timestamp available for file %s", file_path)
            return None
            
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            deposition_id, content_type, format_type, part_number, version_number = metadata_tuple
            
            if None in (deposition_id, content_type, format_type, part_number):
                logger.warning("File %s doesn't follow OneDep naming convention", file_path)
                return None
                
            if version_number is None:
                version_number = 1
                
            # Format timestamp for database
            timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            
            # Create standardized record dictionary
            record = {
                "deposition_id": deposition_id,
                "content_type": content_type,
                "format_type": format_type,
                "part_number": part_number,
                "version_number": version_number,
                "storage_type": storage_type,
                "created_date": timestamp_str,
            }
            
            return record
        except Exception as e:
            logger.warning("Failed to parse file path %s: %s", file_path, str(e))
            return None
    
    def extractFileKey(self, file_path: str) -> Optional[Tuple[str, str, str, int, Optional[int]]]:
        """
        Extract the key components from a file path.
        
        Returns the tuple of components used to uniquely identify a file in the database.
        
        Args:
            file_path (str): Path to the file
            
        Returns:
            Optional[Tuple[str, str, str, int, Optional[int]]]: Tuple of 
                (deposition_id, content_type, format_type, part_number, version_number)
                or None if parsing fails
        """
        try:
            file_name = os.path.basename(file_path)
            metadata_tuple = self.__path_info.splitFileName(file_name)
            return metadata_tuple
        except Exception as e:
            logger.warning("Failed to extract file key from %s: %s", file_path, str(e))
            return None