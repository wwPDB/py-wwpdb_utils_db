##
# File:    StatusHistoryLoaderTests.py
# Author:  J. Westbrook
# Date:    6-Jan-2015
# Version: 0.001
#
# Updates:
#    16-Aug-2015  jdw   add tests to create file inventory table  -
#
##
"""
Tests for creating and loading status history data using PDBx/mmCIF data files
and external schema definition.

These test database connections deferring to authentication details defined
in the environment.   See class MyDbConnect() for the environment requirements.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import time
import unittest
import traceback

from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef
from mmcif.io.IoAdapterPy import IoAdapterPy
from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen
from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader

from wwpdb.utils.testing.Features import Features


@unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
class StatusHistoryLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(StatusHistoryLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__tddFileList = []
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__ioObj = IoAdapterPy(verbose=self.__verbose, log=self.__lfh)

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__msd = StatusHistorySchemaDef(verbose=self.__verbose, log=self.__lfh)
        # self.__databaseName = self.__msd.getDatabaseName()
        self.__databaseName = "da_internal"
        self.open()

    def tearDown(self):
        self.close()

    def open(self, dbUserId=None, dbUserPwd=None):
        myC = MyDbConnect(dbName=self.__databaseName, dbUser=dbUserId, dbPw=dbUserPwd, verbose=self.__verbose, log=self.__lfh)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            self.__dbCon.close()

    def testStatusHistorySchemaCreate(self):
        """Test case -  create table schema using status history schema definition"""
        startTime = time.time()
        self.__lfh.write("\nStarting StatusHistoryLoaderTests testStatusHistorySchemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = StatusHistorySchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

            if self.__verbose:
                self.__lfh.write("\n\n+Status history  table creation SQL string\n %s\n\n" % "\n".join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if self.__verbose:
                self.__lfh.write("\n\n+INFO mysql server returns %r\n" % ret)

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted StatusHistoryLoaderTests testStatusHistorySchemaCreate at %s (%.2f seconds)\n"
            % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testFileInventorySchemaCreate(self):
        """Test case -  create table schema for file inventory table using status history schema definition"""
        startTime = time.time()
        self.__lfh.write("\nStarting StatusHistoryLoaderTests testFileInventorySchemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = StatusHistorySchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = ["PDBX_ARCHIVE_FILE_INVENTORY"]
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

            if self.__verbose:
                self.__lfh.write("\n\n+FileInventory table creation SQL string\n %s\n\n" % "\n".join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if self.__verbose:
                self.__lfh.write("\n\n+INFO mysql server returns %r\n" % ret)

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted StatusHistoryLoaderTests testFileInventorySchemaCreate at %s (%.2f seconds)\n"
            % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testLoadInventoryFile(self):
        """Test case - create batch load files for all chemical component definition data files -"""
        self.__lfh.write("\nStarting StatusHistoryLoaderTests testLoadInventoryFile\n")
        startTime = time.time()
        try:
            loadPathList = ["test_file_inventory.cif"]
            sml = SchemaDefLoader(schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=None, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            containerNameList, tList = sml.makeLoadFiles(loadPathList)
            for tId, fn in tList:
                self.__lfh.write("\nCreated table %s load file %s\n" % (tId, fn))

            endTime1 = time.time()
            self.__lfh.write("\nBatch files created in %.2f seconds\n" % (endTime1 - startTime))
            self.open()
            sdl = SchemaDefLoader(
                schemaDefObj=self.__msd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh
            )

            sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt="all")
            # self.close()
            endTime2 = time.time()
            self.__lfh.write("\nLoad completed in %.2f seconds\n" % (endTime2 - endTime1))
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write(
            "\nCompleted StatusHistoryLoaderTests testLoadInventoryFile at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )


def createHistoryFullSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusHistoryLoaderTests("testStatusHistorySchemaCreate"))
    return suiteSelect


def createFileInventoryLoadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusHistoryLoaderTests("testFileInventorySchemaCreate"))
    suiteSelect.addTest(StatusHistoryLoaderTests("testLoadInventoryFile"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = createHistoryFullSchemaSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = createFileInventoryLoadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
