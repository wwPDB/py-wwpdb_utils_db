##
# File:    SchemaDefBaseTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Tests for the SchemaDefBase and TableDef schema definition accessor classes.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import copy
import io
import unittest
from typing import cast

from wwpdb.utils.db.SchemaDefBase import SchemaDefBase, SchemaDictType, TableDef, TableDefDict

_SCHEMA: SchemaDictType = {
    "SAMPLE_TABLE": {
        "TABLE_ID": "SAMPLE_TABLE",
        "TABLE_NAME": "sample_table",
        "TABLE_TYPE": "transactional",
        "ATTRIBUTES": {
            "STRUCTURE_ID": "Structure_ID",
            "ENTITY_ID": "entity_id",
            "DESCRIPTION": "description",
            "FORMULA_WEIGHT": "formula_weight",
            "CREATED_DATE": "created_date",
            "ROW_SERIAL": "row_serial",
        },
        "ATTRIBUTE_INFO": {
            # Note ORDER deliberately not in insertion order
            "ENTITY_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
            "STRUCTURE_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 15, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
            "DESCRIPTION": {"SQL_TYPE": "text", "WIDTH": 200, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
            "FORMULA_WEIGHT": {"SQL_TYPE": "FLOAT", "WIDTH": 10, "PRECISION": 3, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
            "CREATED_DATE": {"SQL_TYPE": "DATE", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
            "ROW_SERIAL": {"SQL_TYPE": "INT AUTO_INCREMENT", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 6},
        },
        "ATTRIBUTE_MAP": {
            "STRUCTURE_ID": (None, None, "datablockid()", None),
            "ENTITY_ID": ("entity", "id", None, None),
            "DESCRIPTION": ("entity", "pdbx_description", None, None),
            "FORMULA_WEIGHT": ("entity", "formula_weight", None, None),
            "CREATED_DATE": ("entity_extra", "created", None, None),
        },
        "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ("STRUCTURE_ID", "ENTITY_ID")}, "s1": {"TYPE": "SEARCH", "ATTRIBUTES": ("STRUCTURE_ID",)}},
        "MAP_MERGE_INDICES": {"entity": {"TYPE": "EQUI-JOIN", "ATTRIBUTES": ("id",)}},
        "TABLE_DELETE_ATTRIBUTE": "STRUCTURE_ID",
    },
    # Deliberately incomplete (no TABLE_TYPE or INDICES) to exercise the accessor fallbacks
    "OTHER": cast(
        TableDefDict,
        {
            "TABLE_ID": "OTHER",
            "TABLE_NAME": "other",
            "ATTRIBUTES": {"ID": "id"},
            "ATTRIBUTE_INFO": {"ID": {"SQL_TYPE": "INTEGER", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1}},
            "ATTRIBUTE_MAP": {"ID": ("other", "id", None, None)},
        },
    ),
}


class SchemaDefBaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sd = SchemaDefBase(databaseName="testdb", schemaDefDict=copy.deepcopy(_SCHEMA), verbose=False, log=io.StringIO())

    def testDatabaseAndSchema(self) -> None:
        self.assertEqual(self.__sd.getDatabaseName(), "testdb")
        self.assertEqual(self.__sd.getSchema(), _SCHEMA)
        self.assertEqual(self.__sd.getTableIdList(), ["SAMPLE_TABLE", "OTHER"])
        self.assertEqual(self.__sd.getTableName("SAMPLE_TABLE"), "sample_table")
        self.assertEqual(self.__sd.getTableName("OTHER"), "other")

    def testDefaults(self) -> None:
        sd = SchemaDefBase()
        self.assertIsNone(sd.getDatabaseName())
        self.assertIsNone(sd.getSchema())

    def testMissingTable(self) -> None:
        with self.assertRaises(KeyError):
            self.__sd.getTable("NOT_A_TABLE")
        with self.assertRaises(KeyError):
            self.__sd.getTableName("NOT_A_TABLE")

    def testAttributeOrdering(self) -> None:
        expIds = ["STRUCTURE_ID", "ENTITY_ID", "DESCRIPTION", "FORMULA_WEIGHT", "CREATED_DATE", "ROW_SERIAL"]
        self.assertEqual(self.__sd.getAttributeIdList("SAMPLE_TABLE"), expIds)
        self.assertEqual(
            self.__sd.getAttributeNameList("SAMPLE_TABLE"),
            ["Structure_ID", "entity_id", "description", "formula_weight", "created_date", "row_serial"],
        )

    def testQualifiedAttributeName(self) -> None:
        self.assertEqual(self.__sd.getQualifiedAttributeName(("SAMPLE_TABLE", "ENTITY_ID")), "sample_table.entity_id")
        self.assertEqual(self.__sd.getQualifiedAttributeName(tableAttributeTuple=("OTHER", "ID")), "other.id")
        with self.assertRaises(KeyError):
            self.__sd.getQualifiedAttributeName()

    def testDefaultAttributeParameterMap(self) -> None:
        pm = self.__sd.getDefaultAttributeParameterMap("SAMPLE_TABLE")
        self.assertEqual(
            pm,
            [
                ("STRUCTURE_ID", "structureId"),
                ("ENTITY_ID", "entityId"),
                ("DESCRIPTION", "description"),
                ("FORMULA_WEIGHT", "formulaWeight"),
                ("CREATED_DATE", "createdDate"),
            ],
        )
        pmAll = self.__sd.getDefaultAttributeParameterMap("SAMPLE_TABLE", skipAuto=False)
        self.assertEqual(len(pmAll), 6)
        self.assertEqual(pmAll[-1], ("ROW_SERIAL", "rowSerial"))

    @unittest.expectedFailure
    def testDefaultAttributeParameterMapDoubleUnderscore(self) -> None:
        """BUG: attribute ids with a doubled/trailing underscore (e.g. PdbxSchemaDef 'DECAY__') raise IndexError
        inside the camel-case conversion; the bare except then appends identity pairs for ALL attributes,
        producing duplicate entries."""
        schema = copy.deepcopy(_SCHEMA)
        schema["OTHER"]["ATTRIBUTES"] = {"ID": "id", "DECAY__": "decay__"}
        schema["OTHER"]["ATTRIBUTE_INFO"] = {
            "ID": {"SQL_TYPE": "INTEGER", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
            "DECAY__": {"SQL_TYPE": "FLOAT", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
        }
        sd = SchemaDefBase(databaseName="testdb", schemaDefDict=schema)
        pm = sd.getDefaultAttributeParameterMap("OTHER")
        self.assertEqual(len(pm), 2)


class TableDefTests(unittest.TestCase):
    def setUp(self) -> None:
        sd = SchemaDefBase(databaseName="testdb", schemaDefDict=copy.deepcopy(_SCHEMA), verbose=False, log=io.StringIO())
        self.__td = sd.getTable("SAMPLE_TABLE")
        self.__other = sd.getTable("OTHER")

    def testTableInfo(self) -> None:
        td = self.__td
        self.assertIsInstance(td, TableDef)
        self.assertEqual(td.getName(), "sample_table")
        self.assertEqual(td.getType(), "transactional")
        self.assertEqual(td.getId(), "SAMPLE_TABLE")
        self.assertEqual(td.getAttributeIdMap()["STRUCTURE_ID"], "Structure_ID")
        self.assertEqual(td.getAttributeName("ENTITY_ID"), "entity_id")
        self.assertIsNone(td.getAttributeName("NOPE"))
        self.assertIsNone(self.__other.getType())

    def testAttributeInfo(self) -> None:
        td = self.__td
        self.assertEqual(td.getAttributeType("FORMULA_WEIGHT"), "FLOAT")
        self.assertIsNone(td.getAttributeType("NOPE"))
        self.assertEqual(td.getAttributeWidth("STRUCTURE_ID"), 15)
        self.assertIsNone(td.getAttributeWidth("NOPE"))
        self.assertEqual(td.getAttributePrecision("FORMULA_WEIGHT"), 3)
        self.assertIsNone(td.getAttributePrecision("NOPE"))
        self.assertTrue(td.getAttributeNullable("DESCRIPTION"))
        self.assertFalse(td.getAttributeNullable("ENTITY_ID"))
        self.assertIsNone(td.getAttributeNullable("NOPE"))
        self.assertTrue(td.getAttributeIsPrimaryKey("ENTITY_ID"))
        self.assertFalse(td.getAttributeIsPrimaryKey("DESCRIPTION"))
        self.assertIsNone(td.getAttributeIsPrimaryKey("NOPE"))
        self.assertEqual(sorted(td.getPrimaryKeyAttributeIdList()), ["ENTITY_ID", "STRUCTURE_ID"])

    def testTypePredicates(self) -> None:
        td = self.__td
        self.assertTrue(td.isAutoIncrementType("ROW_SERIAL"))
        self.assertFalse(td.isAutoIncrementType("ENTITY_ID"))
        self.assertFalse(td.isAutoIncrementType("NOPE"))
        self.assertTrue(td.isAttributeStringType("ENTITY_ID"))
        # Lower case type names are normalized
        self.assertTrue(td.isAttributeStringType("DESCRIPTION"))
        self.assertFalse(td.isAttributeStringType("FORMULA_WEIGHT"))
        self.assertFalse(td.isAttributeStringType("NOPE"))
        self.assertTrue(td.isAttributeFloatType("FORMULA_WEIGHT"))
        self.assertFalse(td.isAttributeFloatType("ENTITY_ID"))
        self.assertFalse(td.isAttributeFloatType("NOPE"))
        self.assertTrue(td.isAttributeIntegerType("ROW_SERIAL"))
        self.assertTrue(self.__other.isAttributeIntegerType("ID"))
        self.assertFalse(td.isAttributeIntegerType("ENTITY_ID"))
        self.assertFalse(td.isAttributeIntegerType("NOPE"))

    def testOrderedLists(self) -> None:
        td = self.__td
        self.assertEqual(td.getAttributeIdList()[:3], ["STRUCTURE_ID", "ENTITY_ID", "DESCRIPTION"])
        self.assertEqual(td.getAttributeNameList()[:3], ["Structure_ID", "entity_id", "description"])

    def testIndices(self) -> None:
        td = self.__td
        self.assertEqual(sorted(td.getIndexNames()), ["p1", "s1"])
        self.assertEqual(td.getIndexType("p1"), "UNIQUE")
        self.assertIsNone(td.getIndexType("zz"))
        self.assertEqual(tuple(td.getIndexAttributeIdList("p1")), ("STRUCTURE_ID", "ENTITY_ID"))
        self.assertEqual(list(td.getIndexAttributeIdList("zz")), [])
        self.assertEqual(self.__other.getIndexNames(), [])

    def testMapping(self) -> None:
        td = self.__td
        self.assertEqual(td.getMapAttributeInfo("ENTITY_ID"), ("entity", "id", None, None))
        self.assertEqual(td.getMapAttributeInfo("ROW_SERIAL"), ())
        self.assertEqual(td.getMapAttributeIdList(), ["STRUCTURE_ID", "ENTITY_ID", "DESCRIPTION", "FORMULA_WEIGHT", "CREATED_DATE"])
        self.assertEqual(td.getMapAttributeNameList(), ["Structure_ID", "entity_id", "description", "formula_weight", "created_date"])
        self.assertEqual(sorted(td.getMapInstanceCategoryList()), ["entity", "entity_extra"])
        self.assertEqual(td.getMapOtherAttributeIdList(), ["STRUCTURE_ID"])
        self.assertEqual(td.getMapInstanceAttributeList("entity"), ["id", "pdbx_description", "formula_weight"])
        self.assertEqual(td.getMapInstanceAttributeIdList("entity"), ["ENTITY_ID", "DESCRIPTION", "FORMULA_WEIGHT"])
        self.assertEqual(td.getMapInstanceAttributeIdList("missing"), [])
        self.assertEqual(td.getMapAttributeFunction("STRUCTURE_ID"), "datablockid()")
        self.assertIsNone(td.getMapAttributeFunction("ENTITY_ID"))
        self.assertIsNone(td.getMapAttributeFunction("NOPE"))
        self.assertIsNone(td.getMapAttributeFunctionArgs("STRUCTURE_ID"))
        self.assertIsNone(td.getMapAttributeFunctionArgs("NOPE"))
        self.assertEqual(td.getMapAttributeDict()["DESCRIPTION"], "pdbx_description")
        self.assertIsNone(td.getMapAttributeDict()["STRUCTURE_ID"])

    def testMergeIndices(self) -> None:
        td = self.__td
        self.assertEqual(tuple(td.getMapMergeIndexAttributes("entity")), ("id",))
        self.assertEqual(list(td.getMapMergeIndexAttributes("entity_extra")), [])
        self.assertEqual(td.getMapMergeIndexType("entity"), "EQUI-JOIN")
        # Note: a missing merge index type returns an empty list rather than None
        self.assertEqual(td.getMapMergeIndexType("missing"), [])

    def testNullValuesAndWidths(self) -> None:
        td = self.__td
        self.assertEqual(td.getSqlNullValue("ENTITY_ID"), "")
        self.assertEqual(td.getSqlNullValue("CREATED_DATE"), r"\N")
        self.assertEqual(td.getSqlNullValue("FORMULA_WEIGHT"), r"\N")
        self.assertEqual(td.getSqlNullValue("NOPE"), r"\N")
        nD = td.getSqlNullValueDict()
        self.assertEqual(nD["DESCRIPTION"], "")
        self.assertEqual(nD["CREATED_DATE"], r"\N")
        self.assertEqual(nD["ROW_SERIAL"], r"\N")
        wD = td.getStringWidthDict()
        self.assertEqual(wD["STRUCTURE_ID"], 15)
        self.assertEqual(wD["DESCRIPTION"], 200)
        self.assertEqual(wD["FORMULA_WEIGHT"], 0)

    def testDeleteAttribute(self) -> None:
        self.assertEqual(self.__td.getDeleteAttributeId(), "STRUCTURE_ID")
        self.assertEqual(self.__td.getDeleteAttributeName(), "Structure_ID")
        self.assertIsNone(self.__other.getDeleteAttributeId())
        self.assertIsNone(self.__other.getDeleteAttributeName())

    def testEmptyTableDef(self) -> None:
        td = TableDef()
        self.assertIsNone(td.getName())
        self.assertIsNone(td.getId())
        self.assertIsNone(td.getType())
        self.assertEqual(td.getAttributeIdMap(), {})
        self.assertEqual(td.getPrimaryKeyAttributeIdList(), [])
        self.assertEqual(td.getMapAttributeIdList(), [])
        self.assertEqual(td.getMapAttributeNameList(), [])
        self.assertEqual(td.getMapInstanceCategoryList(), [])
        self.assertEqual(td.getMapOtherAttributeIdList(), [])
        self.assertEqual(td.getMapInstanceAttributeList("x"), [])
        self.assertEqual(list(td.getMapMergeIndexAttributes("x")), [])
        with self.assertRaises(KeyError):
            td.getAttributeIdList()
        with self.assertRaises(KeyError):
            td.getAttributeNameList()
        with self.assertRaises(KeyError):
            td.getMapAttributeDict()
        with self.assertRaises(KeyError):
            td.getSqlNullValueDict()
        with self.assertRaises(KeyError):
            td.getStringWidthDict()


if __name__ == "__main__":
    unittest.main()
