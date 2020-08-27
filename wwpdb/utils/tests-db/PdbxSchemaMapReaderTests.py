##
# File:    PdbxSchemaMapReaderTests.py.py
# Author:  J. Westbrook
# Date:    4-Jan-2013
# Version: 0.001
#
# Update:
#  27-Sep-2012  jdw add alternate instance attribute mapping.
#  11=Jan-2013  jdw add table and attribute abbreviation support.
#  12-Jan-2013  jdw add Chemical component and PDBx schema map examples
#  14-Jan-2013  jdw installed in wwpdb.utils.db/
##
"""
Tests for reader of RCSB schema map data files exporting the data structure used by the
wwpdb.utils.db.SchemaMapDef class hierarchy.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import unittest
import traceback
import pprint
import os

# import json
import platform

from wwpdb.utils.db.PdbxSchemaMapReader import PdbxSchemaMapReader

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):  # pragma: no cover
    os.makedirs(TESTOUTPUT)


class PdbxSchemaMapReaderTests(unittest.TestCase):
    def setUp(self):
        self.__lfh = sys.stderr
        self.__verbose = True
        mockTopPath = os.path.join(TOPDIR, "wwpdb", "mock-data")
        schemaPath = os.path.join(mockTopPath, "SCHEMA_MAP")
        self.__pathPrdSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_prd_v5.cif")
        self.__pathPdbxSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_v40.cif")
        self.__pathCcSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_cc.cif")

    def tearDown(self):
        pass

    def testReadPrdMap(self):
        self.__readMap(self.__pathPrdSchemaMapFile, os.path.join(TESTOUTPUT, "prd-def.out"))

    def testReadCcMap(self):
        self.__readMap(self.__pathCcSchemaMapFile, os.path.join(TESTOUTPUT, "cc-def.out"))

    def testReadPdbxMap(self):
        self.__readMap(self.__pathPdbxSchemaMapFile, os.path.join(TESTOUTPUT, "pdbx-def.out"))

    def __readMap(self, mapFilePath, defFilePath):
        """Test case -  read input schema map file and write python schema def data structure -"""
        self.__lfh.write("\nStarting PdbxSchemaMapReaderTests __readap\n")
        try:
            smr = PdbxSchemaMapReader(verbose=self.__verbose, log=self.__lfh)
            smr.read(mapFilePath)
            sd = smr.makeSchemaDef()
            self.assertNotEqual(sd, {}, "Failed to read map")

            # sOut=json.dumps(sd,sort_keys=True,indent=3)
            sOut = pprint.pformat(sd, indent=1, width=120)
            ofh = open(defFilePath, "w")
            ofh.write("\n%s\n" % sOut)
            ofh.close()

            with open(os.devnull, "w") as fout:
                smr.dump(fout)

        except Exception as _e:  # noqa: F841  # pragma: no cover
            traceback.print_exc(file=sys.stderr)
            self.fail()


def schemaSuite():  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPrdMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadCcMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPdbxMap"))
    return suiteSelect


if __name__ == "__main__":  # pragma: no cover
    #
    mySuite = schemaSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
