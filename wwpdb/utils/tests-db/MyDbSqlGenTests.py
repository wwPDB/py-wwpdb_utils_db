##
#
# File:    MyDbSqlGenTests.py
# Author:  J. Westbrook
# Date:    31-Jan-2012
# Version: 0.001
##
"""
Test cases for SQL command generation  --   no data connections required for these tests --

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import os
import sys
import platform
import unittest
import traceback
import time


from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen, MyDbQuerySqlGen, MyDbConditionSqlGen
from wwpdb.utils.db.MessageSchemaDef import MessageSchemaDef
from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from wwpdb.utils.db.PdbDistroSchemaDef import PdbDistroSchemaDef

HERE = os.path.abspath(os.path.dirname(__file__))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):  # pragma: no cover
    os.makedirs(TESTOUTPUT)


class MyDbSqlGenTests(unittest.TestCase):
    def setUp(self):
        self.__lfh = sys.stdout
        self.__verbose = False

    def tearDown(self):
        pass

    def testMessageSchemaCreate(self):
        """Test case -  create table schema using message schema definition as an example"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testMessageSchemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = MessageSchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))

                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % "\n".join(sqlL))

        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testMessageSchemaCreate at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testMessageImportExport(self):
        """Test case -  import and export commands --"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testMessageImportExport at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = MessageSchemaDef(verbose=self.__verbose, log=self.__lfh)
            databaseName = msd.getDatabaseName()
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)

            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                exportPath = os.path.join(TESTOUTPUT, tableDefObj.getName() + ".tdd")
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table export SQL string\n %s\n\n" % sqlExport)

                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)

        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testMessageImportExport at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testBirdSchemaCreate(self):
        """Test case -  create table schema using message schema definition as an example"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testBirdSchemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = BirdSchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=msd.getDatabaseName(), tableDefObj=tableDefObj))

                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % "\n".join(sqlL))

        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testBirdSchemaCreate at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testBirdImportExport(self):
        """Test case -  import and export commands --"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testBirdImportExport at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            msd = BirdSchemaDef(verbose=self.__verbose, log=self.__lfh)
            databaseName = msd.getDatabaseName()
            tableIdList = msd.getTableIdList()
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)

            for tableId in tableIdList:
                tableDefObj = msd.getTable(tableId)
                exportPath = os.path.join(TESTOUTPUT, tableDefObj.getName() + ".tdd")
                sqlExport = myAd.exportTable(databaseName, tableDefObj, exportPath=exportPath)
                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table export SQL string\n %s\n\n" % sqlExport)

                sqlImport = myAd.importTable(databaseName, tableDefObj, importPath=exportPath)
                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table import SQL string\n %s\n\n" % sqlImport)

        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testBirdImportExport at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testSelect1(self):
        """Test case -  selection everything for a simple condition-"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testSelect1 at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:

            #
            msd = MessageSchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = msd.getTableIdList()
            sqlGen = MyDbQuerySqlGen(schemaDefObj=msd, verbose=self.__verbose, log=self.__lfh)

            for tableId in tableIdList:
                sqlCondition = MyDbConditionSqlGen(schemaDefObj=msd, verbose=self.__verbose, log=self.__lfh)
                sqlCondition.addValueCondition((tableId, "DEP_ID"), "EQ", ("D000001", "CHAR"))
                aIdList = msd.getAttributeIdList(tableId)
                for aId in aIdList:
                    sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))
                sqlGen.setCondition(sqlCondition)
                sqlGen.addOrderByAttributeId(attributeTuple=(tableId, "MESSAGE_ID"))
                sqlS = sqlGen.getSql()
                if self.__verbose:  # pragma: no cover
                    self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)
                sqlGen.clear()
        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testSelect1 at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testSelectDistro(self):
        """Test case -  selection, condition and ordering methods using distro schema"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyDbSqlGenTests testSelectDistro at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            # Celection list  -
            sList = [("PDB_ENTRY_TMP", "PDB_ID"), ("REFINE", "LS_D_RES_LOW"), ("REFINE", "LS_R_FACTOR_R_WORK")]
            # Condition value list -
            cList = [
                (("PDB_ENTRY_TMP", "PDB_ID"), "LIKE", ("x-ray", "char")),
                (("PDB_ENTRY_TMP", "STATUS_CODE"), "EQ", ("REL", "char")),
                (("PDB_ENTRY_TMP", "METHOD"), "NE", ("THEORETICAL_MODEL", "char")),
                (("PDBX_WEBSELECT", "ENTRY_TYPE"), "EQ", ("PROTEIN", "char")),
                (("PDBX_WEBSELECT", "CRYSTAL_TWIN"), "GT", (0, "int")),
                (("PDBX_WEBSELECT", "REFINEMENT_SOFTWARE"), "LIKE", ("REFMAC", "char")),
                (("PDBX_WEBSELECT", "DATE_OF_RCSB_RELEASE"), "GE", (1900, "date")),
                (("PDBX_WEBSELECT", "DATE_OF_RCSB_RELEASE"), "LE", (2014, "date")),
            ]
            #
            gList = [
                ("OR", ("PDBX_WEBSELECT", "METHOD_TO_DETERMINE_STRUCT"), "LIKE", ("MOLECULAR REPLACEMENT", "char")),
                ("OR", ("PDBX_WEBSELECT", "METHOD_TO_DETERMINE_STRUCT"), "LIKE", ("MR", "char")),
            ]
            # attribute ordering list
            oList = [("PDB_ENTRY_TMP", "PDB_ID"), ("REFINE", "LS_D_RES_LOW"), ("REFINE", "LS_R_FACTOR_R_WORK")]

            sd = PdbDistroSchemaDef(verbose=self.__verbose, log=self.__lfh)
            # tableIdList = sd.getTableIdList()
            # aIdList = sd.getAttributeIdList(tableId)
            sqlGen = MyDbQuerySqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)

            sTableIdList = []
            for sTup in sList:
                sqlGen.addSelectAttributeId(attributeTuple=(sTup[0], sTup[1]))
                sTableIdList.append(sTup[0])

            sqlCondition = MyDbConditionSqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
            for cTup in cList:
                sqlCondition.addValueCondition(cTup[0], cTup[1], cTup[2])
            sqlCondition.addGroupValueConditionList(gList, preOp="AND")
            sqlCondition.addTables(sTableIdList)
            #
            sqlGen.setCondition(sqlCondition)
            for oTup in oList:
                sqlGen.addOrderByAttributeId(attributeTuple=oTup)
            sqlS = sqlGen.getSql()
            if self.__verbose:
                self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)  # pragma: no cover
            sqlGen.clear()
        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyDbSqlGenTests testSelectDistro at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))


def suite():  # pragma: no cover
    return unittest.makeSuite(MyDbSqlGenTests, "test")


def suiteMessageSchema():  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testMessageSchemaCreate"))
    suiteSelect.addTest(MyDbSqlGenTests("testMessageImportExport"))
    return suiteSelect


def suiteBirdSchema():  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testBirdSchemaCreate"))
    suiteSelect.addTest(MyDbSqlGenTests("testBirdImportExport"))
    return suiteSelect


def suitesSelect():  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbSqlGenTests("testSelect1"))
    suiteSelect.addTest(MyDbSqlGenTests("testSelectDistro"))
    return suiteSelect


if __name__ == "__main__":  # pragma: no cover
    # Run all tests --
    # unittest.main()
    #
    mySuite = suiteMessageSchema()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteBirdSchema()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suitesSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
