##
# File:  MyConnectionBase.py
# Date:  25-Jan-2013 J. Westbrook
#
# Update:
#  4-Feb-2013 jdw include resource for chemical components data.
# 13-Jul-2014 jdw add config for da_internal database
#  3-Mar-2016 jdw add support for non-standard port on connection -
# 30-Jan-2017 jdw all authentication now taken from configuration file -
# 16-Feb-2017 jdw add resource 'status'
# 20-Apr-2017 jdw adjusting pooling configuration
# 30-Sep-2026 ep  replace sqlalchemy pool.manage() with shared sqlalchemy engines (create_engine) in MyDbPool
##
"""
Base class for managing database connection which handles application specific authentication.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import logging
import sys
from typing import Any, Dict, Optional, TextIO, Union, cast

from wwpdb.utils.config.ConfigInfo import ConfigInfo

from wwpdb.utils.db.MyDbPool import getConnection

logger = logging.getLogger(__name__)
#
# Connection pooling is shared with MyDbUtil.MyDbConnect via MyDbPool -- one SQLAlchemy engine
# (QueuePool) per distinct set of connection arguments.  closeConnection() returns connections to the pool.
#


class MyConnectionBase:
    def __init__(self, siteId: Optional[str] = None, verbose: bool = False, log: TextIO = sys.stderr) -> None:  # noqa: ARG002 pylint: disable=unused-argument
        #
        self.__siteId = siteId
        self._cI = ConfigInfo(self.__siteId)
        self._dbCon: Optional[Any] = None
        self.__authD: Dict[str, Any] = {}
        self.__databaseName: Optional[str] = None
        self.__dbHost: Optional[str] = None
        self.__dbUser: Optional[str] = None
        self.__dbPw: Optional[str] = None
        self.__dbSocket: Optional[str] = None
        self.__dbPort: Optional[Union[int, str]] = None
        self.__dbPort = 3306
        self.__dbServer = "mysql"

    def setResource(self, resourceName: Optional[str] = None) -> None:
        #
        if resourceName == "PRD":
            self.__databaseName = self._cI.get("SITE_REFDATA_PRD_DB_NAME")
            self.__dbHost = self._cI.get("SITE_REFDATA_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_REFDATA_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_REFDATA_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_REFDATA_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_REFDATA_DB_PASSWORD")

        elif resourceName == "CC":
            self.__databaseName = self._cI.get("SITE_REFDATA_CC_DB_NAME")
            self.__dbHost = self._cI.get("SITE_REFDATA_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_REFDATA_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_REFDATA_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_REFDATA_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_REFDATA_DB_PASSWORD")

        elif resourceName == "RCSB_INSTANCE":
            self.__databaseName = self._cI.get("SITE_INSTANCE_DB_NAME")
            self.__dbHost = self._cI.get("SITE_INSTANCE_DB_HOST_NAME")
            self.__dbSocket = self._cI.get("SITE_INSTANCE_DB_SOCKET")
            self.__dbPort = self._cI.get("SITE_INSTANCE_DB_PORT_NUMBER")

            self.__dbUser = self._cI.get("SITE_INSTANCE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_INSTANCE_DB_PASSWORD")

        elif resourceName == "DA_INTERNAL":
            self.__databaseName = self._cI.get("SITE_DA_INTERNAL_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DA_INTERNAL_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DA_INTERNAL_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DA_INTERNAL_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DA_INTERNAL_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DA_INTERNAL_DB_PASSWORD")

        elif resourceName == "DA_INTERNAL_COMBINE":
            self.__databaseName = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_SOCKET")

            self.__dbUser = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DA_INTERNAL_COMBINE_DB_PASSWORD")
        elif resourceName == "DISTRO":
            self.__databaseName = self._cI.get("SITE_DISTRO_DB_NAME")
            self.__dbHost = self._cI.get("SITE_DISTRO_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DISTRO_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DISTRO_DB_SOCKET")
            self.__dbUser = self._cI.get("SITE_DISTRO_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DISTRO_DB_PASSWORD")

        elif resourceName == "STATUS":
            self.__databaseName = self._cI.get("SITE_DB_DATABASE_NAME")
            self.__dbHost = self._cI.get("SITE_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_DB_SOCKET")
            self.__dbUser = self._cI.get("SITE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE_DB_PASSWORD")

        elif resourceName == "MESSAGE":
            self.__databaseName = self._cI.get("SITE_MESSAGE_DB_DATABASE_NAME")
            self.__dbHost = self._cI.get("SITE_MESSAGE_DB_HOST_NAME")
            self.__dbPort = self._cI.get("SITE_MESSAGE_DB_PORT_NUMBER")
            self.__dbSocket = self._cI.get("SITE_MESSAGE_DB_SOCKET")
            self.__dbUser = self._cI.get("SITE_MESSAGE_DB_USER_NAME")
            self.__dbPw = self._cI.get("SITE__MESSAGE_DB_PASSWORD")

        else:
            pass

        if self.__dbSocket is None or len(self.__dbSocket) < 2:
            self.__dbSocket = None

        if self.__dbPort is None:
            self.__dbPort = 3306
        else:
            self.__dbPort = int(str(self.__dbPort))

        logger.info(
            "+MyConnectionBase(setResource) %s resource name %s server %s dns %s host %s user %s socket %s port %r",
            self.__siteId,
            resourceName,
            self.__dbServer,
            self.__databaseName,
            self.__dbHost,
            self.__dbUser,
            self.__dbSocket,
            self.__dbPort,
        )
        #
        self.__authD["DB_NAME"] = self.__databaseName
        self.__authD["DB_HOST"] = self.__dbHost
        self.__authD["DB_USER"] = self.__dbUser
        self.__authD["DB_PW"] = self.__dbPw
        self.__authD["DB_SOCKET"] = self.__dbSocket
        self.__authD["DB_PORT"] = int(str(self.__dbPort))
        self.__authD["DB_SERVER"] = self.__dbServer
        #

    def getAuth(self) -> Dict[str, Any]:
        return self.__authD

    def setAuth(self, authD: Dict[str, Any]) -> None:
        try:
            self.__authD = authD
            self.__databaseName = self.__authD["DB_NAME"]
            self.__dbHost = self.__authD["DB_HOST"]
            self.__dbUser = self.__authD["DB_USER"]
            self.__dbPw = self.__authD["DB_PW"]
            self.__dbSocket = self.__authD["DB_SOCKET"]
            if "DB_PORT" in self.__authD:
                self.__dbPort = int(str(self.__authD["DB_PORT"]))
            else:
                self.__dbPort = 3306
            self.__dbServer = self.__authD["DB_SERVER"]
        except:  # noqa: E722 pylint: disable=bare-except
            pass

    def openConnection(self) -> bool:
        """Create a database connection and return a connection object.

        Returns None on failure
        """
        #
        if self._dbCon is not None:
            # Close an open connection -
            logger.info("+MyDbConnect.connect() WARNING Closing an existing connection.")
            self.closeConnection()

        try:
            connectKw: Dict[str, Any] = {
                "db": "%s" % self.__databaseName,
                "user": "%s" % self.__dbUser,
                "passwd": "%s" % self.__dbPw,
                "host": "%s" % self.__dbHost,
                "port": int(str(self.__dbPort)),
                "local_infile": 1,
            }
            if self.__dbSocket is not None:
                connectKw["unix_socket"] = "%s" % self.__dbSocket
            dbcon = getConnection(connectKw)

            self._dbCon = dbcon
            return True
        except:  # noqa: E722 pylint: disable=bare-except
            logger.exception(
                "+MyDbConnect.connect() Connection error to server %s host %s dsn %s user %s pw %s socket %s port %d \n",
                self.__dbServer,
                self.__dbHost,
                self.__databaseName,
                self.__dbUser,
                self.__dbPw,
                self.__dbSocket,
                self.__dbPort,
            )
            self._dbCon = None

        return False

    def getConnection(self) -> Optional[Any]:
        return self._dbCon

    def closeConnection(self) -> bool:
        """Close db session"""
        if self._dbCon is not None:
            self._dbCon.close()
            self._dbCon = None
            return True
        return False

    def getCursor(self) -> Optional[Any]:
        try:
            # With no open connection this raises (and is logged) as before
            return cast("Any", self._dbCon).cursor()
        except:  # noqa: E722 pylint: disable=bare-except
            logger.exception("+MyConnectionBase(getCursor) failing.\n")

        return None
