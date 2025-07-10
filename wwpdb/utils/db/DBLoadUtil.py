##
# File:  DBLoadUtil.py
# Date:  06-feb-2015
# Updates:
##
"""
DB loading utility class

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) 2012 wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""

import os
import sys
import traceback

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from wwpdb.utils.config.ConfigInfoApp import ConfigInfoAppCommon
from wwpdb.utils.dp.RcsbDpUtility import RcsbDpUtility  # pylint: disable=import-error,no-name-in-module

# from wwpdb.utils.db.SqlLoader import SqlLoader

__docformat__ = "restructuredtext en"
__author__ = "Zukang Feng"
__email__ = "zfeng@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"


class DBLoadUtil:
    """Class responsible for loading model cif file(s) into da_internal database"""

    def __init__(self, reqObj=None, verbose=False, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__reqObj = reqObj
        self.__sObj = None
        self.__sessionId = None
        self.__sessionPath = None
        self.__siteId = str(self.__reqObj.getValue("WWPDB_SITE_ID"))
        self.__cI = ConfigInfo(self.__siteId)
        self.__cIcommon = ConfigInfoAppCommon(self.__siteId)
        #
        self.__getSession()

    def doLoading(self, fileList):
        """Update content database"""
        if not fileList:
            return
        #
        #
        listfile = self.__getFileName(self.__sessionPath, "filelist", "txt")
        sqlfile = os.path.join(self.__sessionPath, "dbload", "DB_LOADER.sql")
        logfile1 = os.path.join(self.__sessionPath, "dbload", "db-loader.log")
        clogfile1 = os.path.join(self.__sessionPath, "dbload", "sqlload.log")
        #
        self.__genListFile(listfile, fileList)
        self.__getLoadFile(self.__sessionPath, listfile, sqlfile, logfile1)

        try:
            if os.path.exists(sqlfile):
                self.__lfh.write("DBLoadUtil::doLoading() about to load %s with log %s\n" % (sqlfile, clogfile1))
                # sq = SqlLoader(log=self.__lfh, verbose=self.__verbose)
                # sq.loadSql(sqlfile, clogfile1)
                self.__loadData(self.__sessionPath, sqlfile, clogfile1)
            else:
                self.__lfh.write("DBLoadUtil::doLoading() failed to produce load file\n")
        except Exception as e:  # noqa: BLE001
            self.__lfh.write("DbLoadiUtil::doLoading(): failing, with exception %s.\n" % str(e))
            traceback.print_exc(file=self.__lfh)

    def __getFileName(self, path, root, ext):
        """Create unique file name."""
        count = 1
        while True:
            filename = root + "_" + str(count) + "." + ext
            fullname = os.path.join(path, filename)
            if not os.access(fullname, os.F_OK):
                return filename
            #
            count += 1
            #
            return root + "_1." + ext

    def __genListFile(self, filename, filelist):
        """ """
        fn = os.path.join(self.__sessionPath, filename)
        f = open(fn, "w")
        for entryfile in filelist:
            f.write(entryfile + "\n")
        #
        f.close()

    def __getLoadFile(self, sessionPath, listfile, sqlfile, logfile):
        fn = os.path.join(self.__sessionPath, listfile)

        mapping = self.__cIcommon.get_site_da_internal_schema_path()

        self.__lfh.write("DbLoadUtil::__getLoadFile(): listfile %s sqlfile %s logfile %s\n" % (fn, sqlfile, logfile))
        try:
            dp = RcsbDpUtility(tmpPath=sessionPath, siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
            dp.setDebugMode()
            dp.imp(fn)
            dp.addInput(name="mapping_file", value=mapping, type="file")
            dp.addInput(name="file_list", value=True)
            # This code handles model files
            dp.addInput(name="first_block", value=True)

            dp.op("db-loader")
            dp.expLog(logfile)
            dp.exp(sqlfile)
            dp.cleanup()
            return
        except:  # noqa: E722 pylint: disable=bare-except
            self.__lfh.write("DbLoadUtil::__getLoadFile(): failing, with exception.\n")
            traceback.print_exc(file=self.__lfh)

    def __loadData(self, dataDir, sqlfile, logfile):
        dbHost = self.__cI.get("SITE_DB_HOST_NAME")
        dbUser = self.__cI.get("SITE_DB_USER_NAME")
        dbPw = self.__cI.get("SITE_DB_PASSWORD")
        dbPort = self.__cI.get("SITE_DB_PORT_NUMBER")

        cmd = "cd " + dataDir
        cmd += "; " + "mysql -u " + dbUser + " -p" + dbPw + " -h " + dbHost + " -P " + str(dbPort) + " < " + sqlfile + " >& " + logfile
        os.system(cmd)  # noqa: S605

    def __getSession(self):
        """Join existing session or create new session as required."""
        #
        self.__sObj = self.__reqObj.newSessionObj()
        self.__sessionId = self.__sObj.getId()
        self.__sessionPath = self.__sObj.getPath()
        if self.__verbose:
            self.__lfh.write("------------------------------------------------------\n")
            self.__lfh.write("+DBLoadUtil.__getSession() - creating/joining session %s\n" % self.__sessionId)
            self.__lfh.write("+DBLoadUtil.__getSession() - session path %s\n" % self.__sessionPath)
