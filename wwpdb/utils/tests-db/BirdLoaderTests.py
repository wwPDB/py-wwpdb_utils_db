##
# File:    BirdLoaderTests.py
# Author:  J. Westbrook
# Date:    9-Jan-2013
# Version: 0.001
#
# Updates:
#  11-Jan-2013 jdw revise treatment of null values in inserts.
#
#
##
"""
Tests for creating and loading BIRD rdbms database using PDBx/mmCIF data files
and external schema definition.

These test database connections deferring to authentication details defined
in the environment.   See class MyDbConnect() for the environment requirements.

SchemaDefLoader() uses default native Python IoAdapter.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import os
import time
import unittest
import traceback

from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen
from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader
from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery
from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from wwpdb.utils.testing.Features import Features


@unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
class BirdLoaderTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(BirdLoaderTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__tddFileList = []
        self.__lfh = sys.stderr
        self.__verbose = True

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__databaseName = "prdv4"
        self.__topCachePath = "/data/components/prd-v3"
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

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using BIRD schema definition
        """
        startTime = time.time()
        self.__lfh.write("\nStarting BirdLoaderTests testBirdSchemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = BirdSchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))

            if self.__verbose:
                self.__lfh.write("\n\n+BIRD table creation SQL string\n %s\n\n" % "\n".join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if self.__verbose:
                self.__lfh.write("\n\n+INFO mysql server returns %r\n" % ret)

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted BirdLoaderTests testBirdSchemaCreate at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        self.__lfh.write("\nStarting BirdLoaderTests testPrdPathList\n")
        try:
            prd = PdbxPrdIo(verbose=self.__verbose, log=self.__lfh)
            prd.setCachePath(self.__topCachePath)
            self.__loadPathList = prd.makeDefinitionPathList()
            self.__lfh.write("Length of CVS path list %d\n" % len(self.__loadPathList))

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

    def testMakeLoadPrdFiles(self):
        """Test case - for loading BIRD definition data files
        """
        self.__lfh.write("\nStarting BirdLoaderTests resrMakeLoadPrdFiles\n")
        startTime = time.time()
        try:
            self.testPrdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd, verbose=self.__verbose, log=self.__lfh)
            self.__lfh.write("Length of path list %d\n" % len(self.__loadPathList))
            _containerNameList, self.__tddFileList = sml.makeLoadFiles(self.__loadPathList)
            for tId, tPath in self.__tddFileList:
                self.__lfh.write("\nCreate loadable file %s %s\n" % (tId, tPath))
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write("\nCompleted BirdLoaderTests testMakeLoadPrdFiles at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testBirdBatchImport(self):
        """Test case -  import loadable files
        """
        startTime = time.time()
        self.__lfh.write("\nStarting BirdLoaderTests testBirdBatchImport at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            bsd = BirdSchemaDef(verbose=self.__verbose, log=self.__lfh)
            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)

            for tableId in tableIdList:
                fn = tableId + "-loadable.tdd"
                if os.access(fn, os.F_OK):
                    self.__lfh.write("+INFO - Found for %s\n" % fn)
                    tableDefObj = bsd.getTable(tableId)
                    sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=fn, withTruncate=True)
                    if self.__verbose:
                        self.__lfh.write("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)
                    #
                    lfn = tableId + "-load.sql"
                    ofh = open(lfn, "w")
                    ofh.write("%s\n" % sqlImport)
                    ofh.close()
                    #
                    myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
                    myQ.setWarning("error")
                    ret = myQ.sqlCommand(sqlCommandList=[sqlImport])
                    if self.__verbose:
                        self.__lfh.write("\n\n+INFO mysql server returns %r\n" % ret)

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted BirdLoaderTests testBirdBatchImport at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testBirdInsertImport(self):
        """Test case -  import loadable data via SQL inserts
        """
        startTime = time.time()
        self.__lfh.write("\nStarting BirdLoaderTests testBirdInsertImport at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.testPrdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd, verbose=self.__verbose, log=self.__lfh)
            self.__lfh.write("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, _containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            #
            for tableId in tableIdList:
                tableDefObj = bsd.getTable(tableId)
                tableName = tableDefObj.getName()
                tableAttributeIdList = tableDefObj.getAttributeIdList()
                tableAttributeNameList = tableDefObj.getAttributeNameList()

                if tableId in tableDataDict:
                    rowList = tableDataDict[tableId]
                    for row in rowList:
                        vList = []
                        aList = []
                        for tid, nm in zip(tableAttributeIdList, tableAttributeNameList):
                            if len(row[tid]) > 0 and row[id] != r"\N":
                                vList.append(row[tid])
                                aList.append(nm)
                        insertTemplate = myAd.insertTemplateSQL(databaseName, tableName, aList)

                        ok = myQ.sqlTemplateCommand(sqlTemplate=insertTemplate, valueList=vList)
                        if self.__verbose and not ok:
                            # self.__lfh.write("\n\n+ERROR insert fails for table %s row %r\n" % (tableName,vList))
                            # ts=insertTemplate % tuple(vList)
                            # self.__lfh.write("\n%s\n" % ts)
                            pass

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted BirdLoaderTests testBirdInsertImport at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testBirdBatchInsertImport(self):
        """Test case -  import loadable data via SQL inserts -
        """
        startTime = time.time()
        self.__lfh.write("\nStarting BirdLoaderTests testBirdBatchInsertImport at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.testPrdPathList()
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd, verbose=self.__verbose, log=self.__lfh)
            self.__lfh.write("Length of path list %d\n" % len(self.__loadPathList))
            #
            tableDataDict, _containerNameList = sml.fetch(self.__loadPathList)

            databaseName = bsd.getDatabaseName()
            tableIdList = bsd.getTableIdList()

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            #
            for tableId in tableIdList:
                tableDefObj = bsd.getTable(tableId)
                tableName = tableDefObj.getName()
                tableAttributeIdList = tableDefObj.getAttributeIdList()
                tableAttributeNameList = tableDefObj.getAttributeNameList()

                sqlL = []
                if tableId in tableDataDict:
                    rowList = tableDataDict[tableId]
                    for row in rowList:
                        vList = []
                        aList = []
                        for tid, nm in zip(tableAttributeIdList, tableAttributeNameList):
                            if len(row[tid]) > 0 and row[id] != r"\N":
                                vList.append(row[tid])
                                aList.append(nm)
                        sqlL.append((myAd.insertTemplateSQL(databaseName, tableName, aList), vList))

                    ok = myQ.sqlBatchTemplateCommand(sqlL)
                    if self.__verbose and not ok:
                        # self.__lfh.write("\n\n+ERROR batch insert fails for table %s row %r\n" % (tableName,sqlL))
                        pass

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted BirdLoaderTests testBirdBatchInsertImport at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )


def loadBatchFileSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    # suiteSelect.addTest(BirdLoaderTests("testPrdPathList"))
    suiteSelect.addTest(BirdLoaderTests("testMakeLoadPrdFiles"))
    suiteSelect.addTest(BirdLoaderTests("testBirdBatchImport"))
    return suiteSelect


def loadBatchInsertSuite1():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    suiteSelect.addTest(BirdLoaderTests("testBirdInsertImport"))
    return suiteSelect


def loadBatchInsertSuite2():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(BirdLoaderTests("testBirdSchemaCreate"))
    suiteSelect.addTest(BirdLoaderTests("testBirdBatchInsertImport"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = loadBatchFileSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = loadBatchInsertSuite1()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
    mySuite = loadBatchInsertSuite2()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
    #
