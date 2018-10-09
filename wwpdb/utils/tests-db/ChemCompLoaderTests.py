##
# File:    ChemCompLoaderTests.py
# Author:  J. Westbrook
# Date:    7-Nov-2014
# Version: 0.001
#
# Update:
#   10-Nov-2014 -- add scandir.walk() and multiprocess all tasks -
#
##
"""
Tests for loading instance data using schema definition -

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
import scandir
import traceback

from wwpdb.utils.db.MyDbUtil import MyDbConnect
from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader
from wwpdb.utils.db.ChemCompSchemaDef import ChemCompSchemaDef

from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil

# from pdbx_v2.adapter.IoAdapterPy       import IoAdapterPy
from mmcif.io.IoAdapterCore import IoAdapterCore

from wwpdb.utils.testing.Features import Features

@unittest.skipUnless(Features().haveMySqlTestServer(), 'require MySql Test Environment' )
class ChemCompLoaderTests(unittest.TestCase):

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = False
        self.__loadPathList = []
        self.__ioObj = IoAdapterCore(verbose=self.__verbose, log=self.__lfh)
        self.__topCachePath = '../../../../../../reference/components/ligand-dict-v3'
        #

    def tearDown(self):
        pass

    def open(self, dbName=None, dbUserId=None, dbUserPwd=None):
        myC = MyDbConnect(dbName=dbName, dbUser=dbUserId, dbPw=dbUserPwd, verbose=self.__verbose, log=self.__lfh)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            self.__dbCon.close()

    def __makeComponentPathList(self):
        """ Return the list of chemical component definition file paths in the current repository.
        """

        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        pathList = []
        for root, dirs, files in scandir.walk(self.__topCachePath, topdown=False):
            if "REMOVE" in root:
                continue
            for name in files:
                if name.endswith(".cif") and len(name) <= 7:
                    pathList.append(os.path.join(root, name))
        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))
        self.__lfh.write("\nFound %d files in %s\n" % (len(pathList), self.__topCachePath))
        return pathList

    def testListFiles(self):
        """Test case - for loading chemical component definition data files -
        """
        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        try:
            pathList = self.__makeComponentPathList()
            self.__lfh.write("\nFound %d files\n" % len(pathList))
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))

    def testConnect(self):
        """Test case - for creating a test connection
        """
        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        try:
            ccsd = ChemCompSchemaDef()
            self.open(dbName=ccsd.getDatabaseName())
            self.close()
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))

    def testLoadFiles(self):
        """Test case - create batch load files for all chemical component definition data files -
        """
        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        try:
            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose, log=self.__lfh)
            pathList = self.__makeComponentPathList()

            containerNameList, tList = sml.makeLoadFiles(pathList, append=False)
            for tId, fn in tList:
                self.__lfh.write("\nCreated table %s load file %s\n" % (tId, fn))
            #

            endTime1 = time.time()
            self.__lfh.write("\nBatch files created in %.2f seconds\n" % (endTime1 - startTime))
            self.open(dbName=ccsd.getDatabaseName())
            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose, log=self.__lfh)

            sdl.loadBatchFiles(loadList=tList, containerNameList=containerNameList, deleteOpt='all')
            self.close()
            endTime2 = time.time()
            self.__lfh.write("\nLoad completed in %.2f seconds\n" % (endTime2 - endTime1))
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))

    def loadBatchFilesMulti(self, dataList, procName, optionsD, workingDir):
        ccsd = ChemCompSchemaDef()
        self.open(dbName=ccsd.getDatabaseName())
        sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                              warnings='default', verbose=self.__verbose, log=self.__lfh)
        #
        sdl.loadBatchFiles(loadList=dataList, containerNameList=None, deleteOpt=None)
        self.close()
        return dataList, dataList, []

    def makeComponentPathListMulti(self, dataList, procName, optionsD, workingDir):
        """ Return the list of chemical component definition file paths in the current repository.
        """
        pathList = []
        for subdir in dataList:
            dd = os.path.join(self.__topCachePath, subdir)
            for root, dirs, files in scandir.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def testLoadFilesMulti(self):
        """Test case - create batch load files for all chemical component definition data files - (multiproc test)
        """
        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        startTime = time.time()
        numProc = 8
        try:
            dataS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            dataList = [a for a in dataS]
            mpu = MultiProcUtil(verbose=True, log=self.__lfh)
            mpu.set(workerObj=self, workerMethod="makeComponentPathListMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            self.__lfh.write("\nPath list length %d  in %.2f seconds\n" % (len(pathList), endTime0 - startTime))

            # self.__lfh.write("\nPath list %r\n" % pathList[:20])
            # pathList=self.__makeComponentPathList()

            ccsd = ChemCompSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=None, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose, log=self.__lfh)

            #
            mpu = MultiProcUtil(verbose=True, log=self.__lfh)
            mpu.set(workerObj=sml, workerMethod="makeLoadFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=pathList, numProc=numProc, numResults=2)
            #
            containerNameList = retLists[0]
            tList = retLists[1]

            for tId, fn in tList:
                self.__lfh.write("\nCreated table %s load file %s\n" % (tId, fn))
            #

            endTime1 = time.time()
            self.__lfh.write("\nBatch files created in %.2f seconds\n" % (endTime1 - endTime0))
            self.open(dbName=ccsd.getDatabaseName())
            sdl = SchemaDefLoader(schemaDefObj=ccsd, ioObj=self.__ioObj, dbCon=self.__dbCon, workPath='.', cleanUp=False,
                                  warnings='default', verbose=self.__verbose, log=self.__lfh)
            #
            for tId, fn in tList:
                sdl.delete(tId, containerNameList=containerNameList, deleteOpt='all')
            self.close()
            #
            mpu = MultiProcUtil(verbose=True, log=self.__lfh)
            mpu.set(workerObj=self, workerMethod="loadBatchFilesMulti")
            ok, failList, retLists, diagList = mpu.runMulti(dataList=tList, numProc=numProc, numResults=1)

            endTime2 = time.time()
            self.__lfh.write("\nLoad completed in %.2f seconds\n" % (endTime2 - endTime1))
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()
        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))


def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ChemCompLoaderTests("testConnect"))
    # suiteSelect.addTest(ChemCompLoaderTests("testListFiles"))
    # suiteSelect.addTest(ChemCompLoaderTests("testLoadFiles"))
    suiteSelect.addTest(ChemCompLoaderTests("testLoadFilesMulti"))
    return suiteSelect

if __name__ == '__main__':
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
