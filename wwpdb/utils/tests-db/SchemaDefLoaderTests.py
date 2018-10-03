##
# File:    SchemaMapLoaderTests.py
# Author:  J. Westbrook
# Date:    7-Jan-2013
# Version: 0.001
#
# Update:
#
##
"""
Tests for loading instance data using schema definition.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import logging
import sys
import unittest
import traceback
import sys
import os

from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader
from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef

from mmcif.io.IoAdapterPy import IoAdapterPy
#from mmcif.io.IoAdapterCore     import IoAdapterCore

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s')
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

class SchemaDefLoaderTests(unittest.TestCase):

    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = False
        mockTopPath = os.path.join(TOPDIR, 'wwpdb', 'mock-data')
        self.__loadPathList = [os.path.join(mockTopPath, 'PRD', 'PRD_000001.cif'),
                               os.path.join(mockTopPath, 'PRD', 'PRD_000012.cif')]
        self.__ioObj = IoAdapterPy(verbose=self.__verbose, log=self.__lfh)

    def tearDown(self):
        pass

    def testLoadFile(self):
        """Test case - for loading BIRD definition data files
        """
        self.__lfh.write("\nStarting %s %s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name))
        try:
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(schemaDefObj=bsd, ioObj=self.__ioObj, dbCon=None, workPath=HERE, cleanUp=False, warnings='default', verbose=self.__verbose, log=self.__lfh)
            containerNameList, tList = sml.makeLoadFiles(self.__loadPathList)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        self.assertNotEqual(containerNameList, [], "Loading files")
        for tId, fn in tList:
            self.__lfh.write("\nCreated table %s load file %s\n" % (tId, fn))



def loadSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderTests("testLoadFile"))
    return suiteSelect

if __name__ == '__main__':
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
