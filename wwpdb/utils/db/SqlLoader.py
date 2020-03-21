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

import sys

from wwpdb.utils.db.MyDbUtil import MyDbConnect
from wwpdb.utils.config.ConfigInfo import ConfigInfo
import sqlparse


class SqlLoader(object):
    """
    Loads data into database
    """
    def __init__(self, siteId=None, resource="DA_INTERNAL", log=sys.stderr, verbose=False):
        self.__lfh = log
        self.__verbose = verbose
        self.__resources = resource
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

    def loadSql(self, sql_file, logFile):
        """Load sqla data output from db-loader"""

        try:
            with open(sql_file, "r") as fin:
                sqldata = ' '.join(fin.read().splitlines())

            sq = sqlparse.split(sqldata)
        except Exception as e:
            self.__lfh.write("DbLoadingApi::__loadSql failure to pare sql file. %s\n" % str(e))
            return False

        db = MyDbConnect(dbServer=self.__dbServer, dbHost=self.__dbHost, dbName=self.__dbName,
                         dbUser=self.__dbUser, dbPw=self.__dbPw, dbSocket=self.__dbSocket, dbPort=self.__dbPort,
                         verbose=self.__verbose, log=self.__lfh)
        
        dbcon = db.connect()
        if dbcon is None:
            self.__lfh.write("DbLoadingApi::__loadSql failure to connect to db\n")
            return False

        try:
            cursor = dbcon.cursor()
        except:
            self.__lfh.write("\nFailing to get cursor!!!\n")
            dbcon.close()
            return False


        cnt = 0
        for s in sq:
            cnt += 1
            try:
                ret = cursor.execute(s)
            except dbcon.IntegrityError as e:
                if len(e.args) > 1:
                    logstr = "ERROR %s at cmd %s: %s" % (e.args[0], cnt, e.args[1])
                else:
                    logstr = "ERROR %s at cmd %s" %(str(e), cnt)
                self.__logmsg(logFile, logstr)

            except Exception as e:
                logstr = "ERROR: %s" % str(e)
                self.__logmsg(logFile, logstr)


        dbcon.commit()
        cursor.close()
        dbcon.close()

    
