##
# File:    SchemaDefLoaderDbTests.py
# Author:  J. Westbrook
# Date:    12-Jan-2013
# Version: 0.001
#
# Updates:
#
# 13-Jan-2013 jdw  Schema creation and loading tests provided for Bird, Chemical Component and PDBx
#                  entry files.
# 20-Jan-2013 jdw  Add test for materializing sequences data for Bird definitions.
# 21-Jan-2013 jdw  add family definition files with PRD/Family loading
#
##
"""
Tests for creating and loading rdbms database using PDBx/mmCIF data files
and external schema definition.

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
from wwpdb.utils.db.ChemCompSchemaDef import ChemCompSchemaDef
from wwpdb.utils.db.PdbxSchemaDef import PdbxSchemaDef
from wwpdb.utils.db.DaInternalSchemaDef import DaInternalSchemaDef

from mmcif.io.IoAdapterCore import IoAdapterCore

from mmcif_utils.bird.PdbxPrdIo import PdbxPrdIo
from mmcif_utils.bird.PdbxFamilyIo import PdbxFamilyIo
from mmcif_utils.bird.PdbxPrdUtils import PdbxPrdUtils
from mmcif_utils.chemcomp.PdbxChemCompIo import PdbxChemCompIo

from wwpdb.utils.testing.Features import Features


@unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
class SchemaDefLoaderDbTests(unittest.TestCase):
    def __init__(self, methodName="runTest"):
        super(SchemaDefLoaderDbTests, self).__init__(methodName)
        self.__loadPathList = []
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__debug = False

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        # default database
        self.__databaseName = "prdv4"
        self.__birdCachePath = "/data/components/prd-v3"
        self.__birdFamilyCachePath = "/data/components/family-v3"
        self.__ccCachePath = "/data/components/ligand-dict-v3"
        #
        self.__ccFileList = ["BA1T.cif"]
        self.__ccPath = "./data"
        #
        self.__pdbxPath = "../rcsb/data"
        self.__pdbxFileList = ["1cbs.cif", "1o3q.cif", "1xbb.cif", "3of4.cif", "3oqp.cif", "3rer.cif", "3rij.cif", "5hoh.cif"]

        self.__ioObj = IoAdapterCore(verbose=self.__verbose, log=self.__lfh)
        #
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

    def testSchemaCreate(self):
        """  Create table schema for BIRD, chemical component, and PDBx data.
        """
        sd = BirdSchemaDef()
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd = ChemCompSchemaDef()
        self.__schemaCreate(schemaDefObj=sd)
        #
        sd = PdbxSchemaDef()
        self.__schemaCreate(schemaDefObj=sd)

    def testLoadBirdReference(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testLoadBirdReference at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            inputPathList.extend(self.__getPrdFamilyPathList())
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            sdl.load(inputPathList=inputPathList, loadType="batch-file")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testLoadBirdReference at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testReloadBirdReference(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testReloadBirdReference at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPrdPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            sdl.load(inputPathList=inputPathList, loadType="batch-file")
            #
            self.__lfh.write("\n\n\n+INFO BATCH FILE RELOAD TEST --------------------------------------------\n")
            sdl.load(inputPathList=inputPathList, loadType="batch-file", deleteOpt="all")
            self.__lfh.write("\n\n\n+INFO BATCH INSERT RELOAD TEST --------------------------------------------\n")
            sdl.load(inputPathList=inputPathList, loadType="batch-file", deleteOpt="selected")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testReloadBirdReference at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testLoadBirdReferenceWithSequence(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testLoadBirdReferenceWithSequence at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = BirdSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            #
            prd = PdbxPrdIo(verbose=self.__verbose, log=self.__lfh)
            prd.setCachePath(self.__birdCachePath)
            pathList = prd.makeDefinitionPathList()
            #
            for pth in pathList:
                prd.setFilePath(pth)
            self.__lfh.write("PRD repository read completed\n")
            #
            prdU = PdbxPrdUtils(prd, verbose=self.__verbose, log=self.__lfh)
            _rD = prdU.getComponentSequences(addCategory=True)  # noqa: F841
            #
            #
            prdFam = PdbxFamilyIo(verbose=self.__verbose, log=self.__lfh)
            prdFam.setCachePath(self.__birdFamilyCachePath)
            familyPathList = prdFam.makeDefinitionPathList()
            #
            for pth in familyPathList:
                prdFam.setFilePath(pth)
            self.__lfh.write("Family repository read completed\n")
            #
            # combine containers -
            containerList = prd.getCurrentContainerList()
            containerList.extend(prdFam.getCurrentContainerList())
            #
            # Run loader on container list --
            #
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="error", verbose=self.__verbose, log=self.__lfh)
            sdl.load(containerList=containerList, loadType="batch-file", deleteOpt="selected")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testLoadBirdReferenceWithSequence at %s (%.2f seconds)\n"
            % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testLoadChemCompReference(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testLoadChemCompReference at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = ChemCompSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getChemCompPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            sdl.load(inputPathList=inputPathList, loadType="batch-file")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testLoadChemCompReference at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testLoadPdbxFiles(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testLoadPdbxFiles at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = PdbxSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = self.__getPdbxPathList()
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            sdl.load(inputPathList=inputPathList, loadType="batch-insert", deleteOpt="all")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted SchemaDefLoaderDbTest testLoadPdbxFiles at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testLoadChemCompExamples(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testLoadChemCompExamples at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = ChemCompSchemaDef()
            self.__schemaCreate(schemaDefObj=sd)
            inputPathList = [os.path.join(self.__ccPath, fn) for fn in self.__ccFileList]
            sdl = SchemaDefLoader(schemaDefObj=sd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath=".", cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh)
            sdl.load(inputPathList=inputPathList, loadType="batch-insert", deleteOpt="selected")
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testLoadChemCompExamples at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def testGenSchemaDaInternal(self):
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest testGenSchemaDaInternal at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            sd = DaInternalSchemaDef()
            self.__schemaCreateSQL(schemaDefObj=sd)
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted SchemaDefLoaderDbTest testGenSchemaDaInternal at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def __schemaCreateSQL(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __schemaCreateSQL at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            self.__lfh.write("\nSchema creation SQL string\n %s\n\n" % "\n".join(sqlL))

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted SchemaDefLoaderDbTest __schemaCreateSQL at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def __schemaCreate(self, schemaDefObj):
        """Test case -  create table schema using schema definition
        """
        startTime = time.time()
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __schemaCreate at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            tableIdList = schemaDefObj.getTableIdList()
            sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            sqlL = []
            for tableId in tableIdList:
                tableDefObj = schemaDefObj.getTable(tableId)
                sqlL.extend(sqlGen.createTableSQL(databaseName=schemaDefObj.getDatabaseName(), tableDefObj=tableDefObj))

            if self.__debug:
                self.__lfh.write("\nSchema creation SQL string\n %s\n\n" % "\n".join(sqlL))

            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            #
            # Permit warnings to support "drop table if exists" for missing tables.
            #
            myQ.setWarning("default")
            ret = myQ.sqlCommand(sqlCommandList=sqlL)
            if self.__verbose:
                self.__lfh.write("\n\n+INFO mysql server returns %r\n" % ret)

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted SchemaDefLoaderDbTest __schemaCreate at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def __getPdbxPathList(self):
        """Test case -  get the path list of PDBx instance example files -
        """
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __getPdbxPathList\n")
        try:
            loadPathList = [os.path.join(self.__pdbxPath, v) for v in self.__pdbxFileList]
            self.__lfh.write("Length of PDBx file path list %d\n" % len(loadPathList))
            return loadPathList
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

    def __getPrdPathList(self):
        """Test case -  get the path list of PRD definitions in the CVS repository.
        """
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __getPrdPathList\n")
        try:
            refIo = PdbxPrdIo(verbose=self.__verbose, log=self.__lfh)
            refIo.setCachePath(self.__birdCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            self.__lfh.write("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

    def __getPrdFamilyPathList(self):
        """Test case -  get the path list of PRD Family definitions in the CVS repository.
        """
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __getPrdFamilyPathList\n")
        try:
            refIo = PdbxFamilyIo(verbose=self.__verbose, log=self.__lfh)
            refIo.setCachePath(self.__birdFamilyCachePath)
            loadPathList = refIo.makeDefinitionPathList()
            self.__lfh.write("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

    def __getChemCompPathList(self):
        """Test case -  get the path list of definitions in the CVS repository.
        """
        self.__lfh.write("\nStarting SchemaDefLoaderDbTest __getChemCompPathList\n")
        try:
            refIo = PdbxChemCompIo(verbose=self.__verbose, log=self.__lfh)
            refIo.setCachePath(self.__ccCachePath)
            loadPathList = refIo.makeComponentPathList()
            self.__lfh.write("Length of CVS path list %d\n" % len(loadPathList))
            return loadPathList
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()


def createSchemaSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testSchemaCreate"))
    return suiteSelect


def loadReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadChemCompReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadPdbxFiles"))
    return suiteSelect


def loadReferenceWithSequenceSuite():
    suiteSelect = unittest.TestSuite()
    # suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReference"))
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadBirdReferenceWithSequence"))
    return suiteSelect


def reloadReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testReloadBirdReference"))
    return suiteSelect


def loadSpecialReferenceSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testLoadChemCompExamples"))
    return suiteSelect


def genSchemaSQLSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderDbTests("testGenSchemaDaInternal"))
    return suiteSelect


if __name__ == "__main__":
    #
    # mySuite = createSchemaSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)

    # mySuite = loadReferenceSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)

    # mySuite = reloadReferenceSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)

    # mySuite=loadSpecialReferenceSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)

    # mySuite = loadReferenceWithSequenceSuite()
    # unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = genSchemaSQLSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
