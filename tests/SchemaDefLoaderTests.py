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
import os
import platform
import sys
import traceback
import unittest

from mmcif.io.IoAdapterPy import IoAdapterPy

from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):  # pragma: no cover
    os.makedirs(TESTOUTPUT)


class SchemaDefLoaderTests(unittest.TestCase):
    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = False
        self.__loadPathList = [os.path.join(HERE, "data", "PRD", "PRD_000001.cif"), os.path.join(HERE, "data", "PRD", "PRD_000012.cif")]
        self.__ioObj = IoAdapterPy(verbose=self.__verbose, log=self.__lfh)

    def tearDown(self):
        pass

    def testLoadFile(self):
        """Test case - for loading BIRD definition data files"""
        self.__lfh.write("\nStarting SchemaDefLoaderTests testLoadFile\n")
        try:
            bsd = BirdSchemaDef()
            sml = SchemaDefLoader(
                schemaDefObj=bsd, ioObj=self.__ioObj, dbCon=None, workPath=TESTOUTPUT, cleanUp=False, warnings="default", verbose=self.__verbose, log=self.__lfh
            )
            containerNameList, tList = sml.makeLoadFiles(self.__loadPathList)
        except:  # noqa: E722  pylint: disable=bare-except  # pragma: no cover
            traceback.print_exc(file=self.__lfh)
            self.fail()

        self.assertNotEqual(containerNameList, [], "Loading files %s" % self.__loadPathList)
        for tId, fn in tList:
            self.__lfh.write("\nCreated table %s load file %s\n" % (tId, fn))


def loadSuite():  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(SchemaDefLoaderTests("testLoadFile"))
    return suiteSelect


if __name__ == "__main__":  # pragma: no cover
    #
    mySuite = loadSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
