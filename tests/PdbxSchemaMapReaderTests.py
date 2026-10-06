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

import io
import os

# import json
import platform
import pprint
import shutil
import sys
import tempfile
import traceback
import unittest
from typing import List

from wwpdb.utils.db.PdbxSchemaMapReader import PdbxSchemaMapReader

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):  # pragma: no cover
    os.makedirs(TESTOUTPUT)


class PdbxSchemaMapReaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = sys.stderr
        self.__verbose = True
        schemaPath = os.path.join(HERE, "data", "schema-map")
        self.__pathPrdSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_prd_v5.cif")
        self.__pathPdbxSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_v40.cif")
        self.__pathCcSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_cc.cif")
        self.__pathPrdCcSchemaMapFile = os.path.join(schemaPath, "schema_map_pdbx_cc.cif")
        self.__pathDaInternalSchemaMapFile = os.path.join(schemaPath, "status_rcsb_schema_da.cif")

    def tearDown(self) -> None:
        pass

    def testReadPrdMap(self) -> None:
        self.__readMap(self.__pathPrdSchemaMapFile, os.path.join(TESTOUTPUT, "prd-def.out"))

    def testReadCcMap(self) -> None:
        self.__readMap(self.__pathCcSchemaMapFile, os.path.join(TESTOUTPUT, "cc-def.out"))

    def testReadPrdCcMap(self) -> None:
        self.__readMap(self.__pathPrdCcSchemaMapFile, os.path.join(TESTOUTPUT, "prdcc-def.out"))

    def testReadPdbxMap(self) -> None:
        self.__readMap(self.__pathPdbxSchemaMapFile, os.path.join(TESTOUTPUT, "pdbx-def.out"))

    def testReadDaInternalMap(self) -> None:
        self.__readMap(self.__pathDaInternalSchemaMapFile, os.path.join(TESTOUTPUT, "dainternal-def.out"))

    def __readMap(self, mapFilePath: str, defFilePath: str) -> None:
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

        except Exception as _e:  # noqa: F841,BLE001  # pragma: no cover
            traceback.print_exc(file=sys.stderr)
            self.fail()


def _makeMapFile(path: str) -> None:
    """Write a small synthetic schema map file exercising abbreviations, data types and index handling."""
    defRows: List[str] = []
    mapRows: List[str] = []
    # A table with more than 16 index attributes
    defRows.append("big_table db_id char Y Y 10 0 Y")
    mapRows.append("big_table db_id ? ? 'datablockid()'")
    for ii in range(1, 17):
        defRows.append("big_table k%d int Y Y 10 0 Y" % ii)
        mapRows.append("big_table k%d '_big.k%d' ? ?" % (ii, ii))
    defRows.extend(
        [
            "long_table_name long_attribute_name varchar Y N 80 0 Y",
            "long_table_name notes text N N 70000 0 Y",
            "long_table_name weight float N N 10 3 Y",
            "long_table_name created date N N 10 0 Y",
            "long_table_name stamp datetime N 1 10 0 Y",
            "long_table_name blobby blob N yes 10 0 Y",
            "rcsb_tableinfo tablename char Y Y 10 0 Y",
            "plain x char Y Y 10 0 Y",
            "two_cat a char Y Y 10 0 Y",
            "two_cat b char Y Y 10 0 Y",
        ]
    )
    mapRows.extend(
        [
            "long_table_name long_attribute_name '_lt.long_attribute_name' ? ?",
            "long_table_name notes '_lt.notes' ? ?",
            "long_table_name weight '_lt.weight' ? ?",
            "long_table_name created '_lt.created' ? ?",
            "long_table_name stamp . ? ?",
            "long_table_name blobby '_lt.blobby' ? ?",
            "plain x . ? ?",
            "two_cat a '_c1.a' ? ?",
            "two_cat b '_c2.b' ? ?",
        ]
    )
    content = (
        "data_rcsb_schema\n"
        "loop_\n_rcsb_table.table_name\nbig_table\nlong_table_name\nrcsb_tableinfo\nplain\ntwo_cat\n"
        "loop_\n_rcsb_table_abbrev.table_name\n_rcsb_table_abbrev.table_abbrev\nlong_table_name ltn\n"
        "loop_\n_rcsb_attribute_abbrev.table_name\n_rcsb_attribute_abbrev.attribute_name\n_rcsb_attribute_abbrev.attribute_abbrev\n"
        "long_table_name long_attribute_name lan\n"
        "loop_\n_rcsb_attribute_def.table_name\n_rcsb_attribute_def.attribute_name\n_rcsb_attribute_def.data_type\n_rcsb_attribute_def.index_flag\n"
        "_rcsb_attribute_def.null_flag\n_rcsb_attribute_def.width\n_rcsb_attribute_def.precision\n_rcsb_attribute_def.populated\n"
        + "\n".join(defRows)
        + "\n#\ndata_rcsb_schema_map\n"
        + "loop_\n_rcsb_attribute_map.target_table_name\n_rcsb_attribute_map.target_attribute_name\n_rcsb_attribute_map.source_item_name\n"
        + "_rcsb_attribute_map.condition_id\n_rcsb_attribute_map.function_id\n"
        + "\n".join(mapRows)
        + "\n#\ndata_unexpected\n_some.item 1\n"
    )
    with open(path, "w") as ofh:
        ofh.write(content)


class PdbxSchemaMapReaderSyntheticTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__workPath = tempfile.mkdtemp()
        self.__mapPath = os.path.join(self.__workPath, "map.cif")
        _makeMapFile(self.__mapPath)
        self.__lfh = io.StringIO()

    def tearDown(self) -> None:
        shutil.rmtree(self.__workPath, ignore_errors=True)

    def testMissingFile(self) -> None:
        smr = PdbxSchemaMapReader(verbose=True, log=self.__lfh)
        self.assertTrue(smr.read(os.path.join(self.__workPath, "missing.cif")))
        self.assertIn("+ERROR - error processing schema map file", self.__lfh.getvalue())
        self.assertEqual(smr.makeSchemaDef(), {})

    def testSyntheticMap(self) -> None:
        smr = PdbxSchemaMapReader(verbose=True, log=self.__lfh)
        self.assertTrue(smr.read(self.__mapPath))
        sD = smr.makeSchemaDef()
        log = self.__lfh.getvalue()
        self.assertIn("+ERROR -unanticipated data container unexpected", log)
        self.assertIn("+ERROR - UNKNOWN DATA TYPE blob", log)
        self.assertIn("+WARNING - big_table index list exceeds max length 17", log)
        self.assertIn("+WARNING - No delete attribute for table long_table_name", log)
        self.assertIn("+WARNING - No merge index possible for table plain", log)
        self.assertNotIn("No merge index possible for table long_table_name", log)
        # rcsb_tableinfo is skipped and table abbreviations are applied
        self.assertEqual(sorted(sD.keys()), ["BIG_TABLE", "LTN", "PLAIN", "TWO_CAT"])
        self.assertNotIn("MAP_MERGE_INDICES", sD["PLAIN"])
        #
        big = sD["BIG_TABLE"]
        self.assertEqual(big["TABLE_DELETE_ATTRIBUTE"], "DB_ID")
        self.assertEqual(big["INDICES"]["s1"], {"TYPE": "SEARCH", "ATTRIBUTES": ("DB_ID",)})
        self.assertEqual(len(big["INDICES"]["p1"]["ATTRIBUTES"]), 17)
        self.assertEqual(big["MAP_MERGE_INDICES"], {"big": {"TYPE": "EQUI-JOIN", "ATTRIBUTES": tuple("k%d" % ii for ii in range(1, 17))}})
        self.assertEqual(big["ATTRIBUTE_MAP"]["DB_ID"], (None, None, "datablockid()", None))
        self.assertEqual(big["ATTRIBUTE_INFO"]["K1"]["SQL_TYPE"], "INT")
        #
        ltn = sD["LTN"]
        self.assertEqual(ltn["TABLE_NAME"], "ltn")
        self.assertEqual(ltn["TABLE_ID"], "LTN")
        self.assertNotIn("TABLE_DELETE_ATTRIBUTE", ltn)
        self.assertEqual(ltn["MAP_MERGE_INDICES"], {"lt": {"TYPE": "EQUI-JOIN", "ATTRIBUTES": ("long_attribute_name",)}})
        self.assertEqual(ltn["INDICES"], {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ("LAN",)}})
        self.assertEqual(ltn["ATTRIBUTES"]["LAN"], "lan")
        self.assertEqual(ltn["ATTRIBUTE_MAP"]["LAN"], ("lt", "long_attribute_name", None, None))
        self.assertEqual(ltn["ATTRIBUTE_MAP"]["STAMP"], (None, None, None, None))
        aI = ltn["ATTRIBUTE_INFO"]
        self.assertEqual(aI["LAN"], {"SQL_TYPE": "VARCHAR", "WIDTH": 80, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": True, "ORDER": 1})
        self.assertEqual(aI["NOTES"]["SQL_TYPE"], "TEXT")
        self.assertEqual(aI["WEIGHT"]["SQL_TYPE"], "FLOAT")
        self.assertEqual(aI["WEIGHT"]["PRECISION"], 3)
        self.assertEqual(aI["CREATED"]["SQL_TYPE"], "DATE")
        self.assertEqual(aI["STAMP"]["SQL_TYPE"], "DATETIME")
        # null_flag '1' / 'yes' -> not nullable
        self.assertFalse(aI["STAMP"]["NULLABLE"])
        self.assertFalse(aI["BLOBBY"]["NULLABLE"])
        self.assertIsNone(aI["BLOBBY"]["SQL_TYPE"])
        #
        out = io.StringIO()
        smr.dump(out)
        dump = out.getvalue()
        self.assertIn("Table name list: ['big_table', 'long_table_name', 'rcsb_tableinfo', 'plain', 'two_cat']", dump)
        self.assertIn("Table long_table_name - abbreviation ltn", dump)
        self.assertIn("Table long_table_name - attribute long_attribute_name  abbreviation lan", dump)

    @unittest.expectedFailure
    def testMergeIndicesMultipleCategories(self) -> None:
        """BUG: makeSchemaDef() reassigns d["MAP_MERGE_INDICES"] inside the loop over categories, so only the
        last instance category keeps a merge index when the key attributes come from several categories."""
        smr = PdbxSchemaMapReader(verbose=False, log=self.__lfh)
        smr.read(self.__mapPath)
        sD = smr.makeSchemaDef()
        self.assertEqual(sorted(sD["TWO_CAT"]["MAP_MERGE_INDICES"].keys()), ["c1", "c2"])

    def testQuiet(self) -> None:
        smr = PdbxSchemaMapReader(verbose=False, log=self.__lfh)
        smr.read(self.__mapPath)
        sD = smr.makeSchemaDef()
        self.assertEqual(len(sD), 4)
        self.assertNotIn("+WARNING", self.__lfh.getvalue())


def schemaSuite() -> unittest.TestSuite:  # pragma: no cover
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPrdMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadCcMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPdbxMap"))
    suiteSelect.addTest(PdbxSchemaMapReaderTests("testReadPrdCcMap"))
    suiteSelect.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(PdbxSchemaMapReaderSyntheticTests))
    return suiteSelect


if __name__ == "__main__":  # pragma: no cover
    #
    mySuite = schemaSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
