##
# File:    SqlLoader.py
# Author: E. Peisach
# Date: 2020-03-20
#
# Updates:
#
#
"""
    Utility class to take dump from db-loader program and using Python load into database
"""
__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "peisach@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.001"

import os
import os.path
import sys

import MySQLdb

from wwpdb.utils.config.ConfigInfo import ConfigInfo
import sqlparse


class _DbConnection(object):
    """Internal class for connecting to mysql server without connection pool present in MyDbUtil"""

    def __init__(self, dbServer="mysql", dbHost="localhost", dbName=None, dbUser=None, dbPw=None,
                 dbSocket=None, dbPort=None, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        self.__lfh = log

        self.__dbName = dbName
        self.__dbUser = dbUser
        self.__dbPw = dbPw
        self.__dbHost = dbHost
        self.__dbSocket = dbSocket

        if dbPort is None:
            self.__dbPort = 3306
        else:
            self.__dbPort = int(str(dbPort))
        #
        self.__dbServer = dbServer

        if dbServer != "mysql":
            self.__lfh.write("+__DbOnnectiont. Unsupported server %s\n" % dbServer)
            sys.exit(1)

        self.__dbcon = None

    def connect(self):
        """Create a database connection and return a connection object.

        Returns None on failure
        """
        #
        if self.__dbcon is not None:
            # Close an open connection -
            self.__lfh.write("__DbConnection.connect() WARNING Closing an existing connection.\n")
            self.close()
        try:
            if self.__dbSocket is None:
                dbcon = MySQLdb.connect(
                    db="%s" % self.__dbName, user="%s" % self.__dbUser, passwd="%s" % self.__dbPw, host="%s" % self.__dbHost, port=self.__dbPort, local_infile=1
                )
            else:
                dbcon = MySQLdb.connect(
                    db="%s" % self.__dbName,
                    user="%s" % self.__dbUser,
                    passwd="%s" % self.__dbPw,
                    host="%s" % self.__dbHost,
                    port=self.__dbPort,
                    unix_socket="%s" % self.__dbSocket,
                    local_infile=1,
                )

            self.__dbcon = dbcon
        except Exception as e:
            self.__lfh.write(
                "+__DbConnect.connect() Connection error to server %s host %s dsn %s user %s pw %s socket %s port %d err %s\n"
                % (self.__dbServer, self.__dbHost, self.__dbName, self.__dbUser, self.__dbPw, self.__dbSocket, self.__dbPort, str(e))
            )
            self.__dbcon = None

        return self.__dbcon

    def close(self):
        """Close any open database connection."""
        if self.__dbcon is not None:
            try:
                self.__dbcon.close()
                self.__dbcon = None
                return True
            except:  # noqa: E722  pylint: disable=bare-except
                pass
        return False


class SqlLoader(object):
    """
    Loads data into database
    """

    def __init__(self, siteId=None, resource="DA_INTERNAL", log=sys.stderr, verbose=False):
        self.__lfh = log
        self.__verbose = verbose
        self.__siteId = siteId

        self.__dbServer = None
        self.__dbHost = None
        self.__dbUser = None
        self.__dbPw = None
        self.__dbPort = None
        self.__dbSocket = None
        self.__dbName = None

        self.__setResource(resource)

    def __setResource(self, resource):
        """Loads resources for access"""

        cI = ConfigInfo(self.__siteId)
        if resource == "DA_INTERNAL":
            self.__dbServer = cI.get("SITE_DB_SERVER")
            self.__dbHost = cI.get("SITE_DB_HOST_NAME")
            self.__dbUser = cI.get("SITE_DB_USER_NAME")
            self.__dbPw = cI.get("SITE_DB_PASSWORD")
            self.__dbPort = str(cI.get("SITE_DB_PORT_NUMBER"))
            self.__dbSocket = cI.get("SITE_DB_SOCKET")
            self.__dbName = cI.get("SITE_DA_INTERNAL_DB_NAME")

        else:
            raise NameError("Unknown resource %s" % resource)

    def __logmsg(self, logFile, msg):
        """Logs a message for output"""
        self.__lfh.write("%s\n" % msg)
        with open(logFile, "a") as fout:
            fout.write("%s\n" % msg)

    def loadSql(self, sql_file, logFile, retry=0):
        """Load sqla data output from db-loader"""

        # Max retries if server goes away
        maxretry = 3

        try:
            with open(sql_file, "r") as fin:
                sqldata = " ".join(fin.read().splitlines())

            sq = sqlparse.split(sqldata)
        except Exception as e:
            self.__lfh.write("DbLoadingApi::__loadSql failure to pare sql file. %s\n" % str(e))
            return False

        # Clear out old log file in case of retry.
        if retry > 0:
            if os.path.exists(logFile):
                os.unlink(logFile)

        db = _DbConnection(
            dbServer=self.__dbServer,
            dbHost=self.__dbHost,
            dbName=self.__dbName,
            dbUser=self.__dbUser,
            dbPw=self.__dbPw,
            dbSocket=self.__dbSocket,
            dbPort=self.__dbPort,
            verbose=self.__verbose,
            log=self.__lfh,
        )

        dbcon = db.connect()
        if dbcon is None:
            self.__lfh.write("DbLoadingApi::__loadSql failure to connect to db\n")
            return False

        try:
            cursor = dbcon.cursor()
        except Exception as e:
            self.__lfh.write("\nFailing to get cursor err: %s!!!\n" % str(e))
            db.close()
            return False

        cnt = 0
        for s in sq:
            cnt += 1
            try:
                cursor.execute(s)
            except dbcon.IntegrityError as e:
                if len(e.args) > 1:
                    logstr = "ERROR %s at cmd %s: %s" % (e.args[0], cnt, e.args[1])
                else:
                    logstr = "ERROR %s at cmd %s" % (str(e), cnt)
                self.__logmsg(logFile, logstr)
                # Continue

            except dbcon.OperationalError as e:
                logstr = "ERROR %s at cmd %s" % (str(e), cnt)
                self.__logmsg(logFile, logstr)

                cursor.close()
                db.close()
                if retry < maxretry:
                    self.__lfh.write("Server issue retry %s\n" % (retry + 1))
                    return self.loadSql(sql_file, logFile, retry=retry + 1)
                else:
                    self.__lfh.write("Too many retry failures\n")
                    return False

            except Exception as e:
                # When line split, upstream looks for ERROR by itself
                logstr = "ERROR %s" % str(e)
                self.__logmsg(logFile, logstr)

        dbcon.commit()
        cursor.close()
        db.close()
        return True
