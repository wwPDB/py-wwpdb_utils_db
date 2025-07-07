##
# File:    FileActivityDbCore.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-04-08
#
# Updates:
#   - Initial implementation extracted from FileActivityDb.py
##
"""
Core database operations for file activity tracking.

This module provides low-level database connection and query execution
functionality for the file activity database.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import logging
import sys
from contextlib import contextmanager
from typing import List, Optional, Tuple, Dict, Any, Generator, TextIO, Union, cast

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from wwpdb.utils.db.MyDbUtil import MyDbConnect

logger = logging.getLogger(__name__)


class FileActivityDbCore:
    """
    Core database functionality for file activity tracking.
    
    This class handles low-level database operations, including:
    - Connection initialization and management
    - Query execution with parameterized SQL
    - Transaction management
    - Error handling
    
    It is designed to be used by higher-level file activity tracking classes
    that implement business logic and public interfaces.
    """

    def __init__(self, siteId: Optional[str] = None, verbose: bool = False, log: TextIO = sys.stderr) -> None:
        """
        Initialize the database core with connection parameters.
        
        The database connection is not established during initialization.
        It will be established on first use.
        
        Args:
            siteId (Optional[str]): Site identifier for configuration lookup
            verbose (bool): Enable verbose output
            log (TextIO): Log file handle for verbose output
        """
        self._verbose: bool = verbose
        self._dbcon = None
        self._closed: bool = True  # Start with no connection
        self._siteId = siteId
        self._lfh = log
        
        # Load table name from configuration
        config = ConfigInfo()
        self._table_name = config.get("SITE_FILE_ACTIVITY_DB_TABLE_NAME", "file_activity_log")

    def _initializeDbConnection(self) -> None:
        """
        Initialize database connection using wwPDB utilities.
        
        Establishes a connection to the OneDep metadata database using configuration
        from ConfigInfo. This is called lazily when the connection is first needed.
        
        Raises:
            Exception: If database connection fails or configuration is invalid.
        """
        if not self._closed:
            return  # Connection already open
        
        try:
            config = ConfigInfo()
            
            # Get database configuration from site configuration - no fallbacks
            db_name = config.get("SITE_FILE_ACTIVITY_DB_NAME")
            db_host = config.get("SITE_FILE_ACTIVITY_DB_HOST_NAME")
            db_port = str(config.get("SITE_FILE_ACTIVITY_DB_NUMBER"))
            db_socket = config.get("SITE_FILE_ACTIVITY_DB_SOCKET")
            db_user = config.get("SITE_FILE_ACTIVITY_DB_USER_NAME")
            db_pw = config.get("SITE_FILE_ACTIVITY_DB_PASSWORD")
            
            myC = MyDbConnect(  # type: ignore
                dbServer="mysql",
                dbHost=db_host,
                dbName=db_name,
                dbUser=db_user,
                dbPw=db_pw,
                dbPort=db_port,
                dbSocket=db_socket,
                verbose=self._verbose,
                log=self._lfh,
            )
            self._dbcon = myC.connect()  # type: ignore
            if self._dbcon:
                self._closed = False  # Mark connection as open
            else:
                raise Exception("Failed to establish database connection")
        except Exception as err:
            logger.error("Unable to connect to the database: %s", err)
            raise

    def _executeSelectQuery(self, query: str, params: Optional[Union[Tuple[Any, ...], List[Any], Dict[str, Any]]] = None) -> List[Tuple[Any, ...]]:
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
            if self._dbcon is None:
                logger.error("Database connection is not initialized")
                return []
            
            cursor = self._dbcon.cursor()
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

    def _executeUpdateQuery(self, query: str, params: Optional[Union[Tuple[Any, ...], List[Any], Dict[str, Any]]] = None) -> bool:
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
            if self._dbcon is None:
                logger.error("Database connection is not initialized")
                return False
            
            cursor = self._dbcon.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self._dbcon.commit()
            cursor.close()
            return True
        except Exception as e:
            logger.error("Database error executing UPDATE: %s", str(e))
            if self._dbcon is not None:
                self._dbcon.rollback()
            if cursor is not None:
                cursor.close()
            return False

    @contextmanager
    def _connection(self) -> Generator[None, None, None]:
        """
        Context manager for database connection lifecycle.
        
        Ensures connection is established before operation and closed after operation
        if it was newly created. The connection is lazy-initialized and only closed
        if this context manager created it.
        
        Usage:
            with self._connection():
                # Database operations using helper methods
                self._executeSelectQuery(...)
                self._executeUpdateQuery(...)
        
        Yields:
            None
        
        Raises:
            Exception: If the database connection cannot be established.
        """
        need_close = self._closed  # Only close if we created the connection
        try:
            self._initializeDbConnection()
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
        if not self._closed and self._dbcon is not None:
            try:
                self._dbcon.close()
            except Exception as err:
                logger.warning("Error closing database connection: %s", err)
            finally:
                self._dbcon = None
                self._closed = True
    
    def getTableName(self) -> str:
        """
        Get the database table name.
        
        Returns:
            str: The name of the file activity database table
        """
        return self._table_name