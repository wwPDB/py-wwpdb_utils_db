##
# File:    MyQueryDirectivesMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Unit tests for parsing query directives and producing SQL  --  no database connection required.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import logging
import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import ANY, MagicMock, call, patch

from wwpdb.utils.db.MyQueryDirectives import MyQueryDirectives
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef

LOGGER_NAME = "wwpdb.utils.db.MyQueryDirectives"


def _attrInfo(sqlType: str, width: int = 0, precision: int = 0, nullable: bool = True, primaryKey: bool = False, order: int = 1) -> Dict[str, Any]:
    return {"SQL_TYPE": sqlType, "WIDTH": width, "PRECISION": precision, "NULLABLE": nullable, "PRIMARY_KEY": primaryKey, "ORDER": order}


def _makeSchemaDef() -> SchemaDefBase:
    schemaDict: Dict[str, Any] = {
        "TA": {
            "TABLE_ID": "TA",
            "TABLE_NAME": "table_a",
            "TABLE_TYPE": "transactional",
            "ATTRIBUTES": {"ID": "id", "NAME": "name", "VAL": "val", "AMT": "amt"},
            "ATTRIBUTE_INFO": {
                "ID": _attrInfo("INT", nullable=False, primaryKey=True, order=1),
                "NAME": _attrInfo("VARCHAR", width=20, order=2),
                "VAL": _attrInfo("FLOAT", width=10, order=3),
                "AMT": _attrInfo("DECIMAL", width=10, precision=2, order=4),
            },
            "INDICES": {},
        },
        "TB": {
            "TABLE_ID": "TB",
            "TABLE_NAME": "table_b",
            "TABLE_TYPE": "MyISAM",
            "ATTRIBUTES": {"ID": "id", "X": "x"},
            "ATTRIBUTE_INFO": {
                "ID": _attrInfo("INT", nullable=False, primaryKey=True, order=1),
                "X": _attrInfo("TEXT", order=2),
            },
            "INDICES": {},
        },
    }
    return SchemaDefBase(databaseName="testdb", schemaDefDict=schemaDict, verbose=False)


class MyQueryDirectivesMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sd = _makeSchemaDef()
        self.__mqd = MyQueryDirectives(schemaDefObj=self.__sd, verbose=False)

    def __build(self, qdL: List[str], domD: Optional[Dict[str, Any]] = None, **kwargs: Any) -> Optional[str]:
        sql: Optional[str] = self.__mqd.build(queryDirL=qdL, domD=domD, **kwargs)
        return sql

    def testEmpty(self) -> None:
        self.assertIsNone(self.__mqd.build())
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([], 0))

    def testSelectAndOrder(self) -> None:
        sql = self.__build(
            [
                "SELECT_ITEM:2:ITEM:DOM_REF:y",
                "SELECT_ITEM:1:ITEM:ta.id",
                "ORDER_ITEM:1:ITEM:ta.id:SORT_ORDER:DECREASING",
                "ORDER_ITEM:2:ITEM:DOM_REF:y:SORT_ORDER:BOGUS",
            ],
            {"y": "ta.name"},
        )
        self.assertEqual(sql, "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n ORDER BY table_a.id DESC,table_a.name ASC \n;")
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([("TA", "ID"), ("TA", "NAME")], 2))

    def testSortOrderVariants(self) -> None:
        for token, expected in [("ASC", "ASC"), ("ASCENDING", "ASC"), ("INCREASING", "ASC"), ("DESC", "DESC"), ("DESCENDING", "DESC"), ("DECREASING", "DESC")]:
            sql = self.__build(["SELECT_ITEM:1:ITEM:ta.id", "ORDER_ITEM:1:ITEM:ta.val:SORT_ORDER:%s" % token])
            self.assertEqual(sql, "SELECT table_a.id \n FROM testdb.table_a \n ORDER BY table_a.val %s \n;" % expected)  # noqa: S608

    def testCustomSeparators(self) -> None:
        sql = self.__build(["SELECT_ITEM;1;ITEM;DOM_REF_1;k"], {"k": "ta.id#ta.name"}, queryDirSeparator=";", domRefSeparator="#")
        self.assertEqual(sql, "SELECT table_a.name \n FROM testdb.table_a \n;")

    def testValueConditions(self) -> None:
        sql = self.__build(
            [
                "SELECT_ITEM:1:ITEM:ta.id",
                "VALUE_CONDITION:1:LOP:AND:ITEM:ta.name:COP:like:VALUE:DOM_REF:n",
                "VALUE_CONDITION:2:LOP:OR:ITEM:ta.val:COP:GT:VALUE:2.5",
                # missing dom reference -> condition is skipped
                "VALUE_CONDITION:3:LOP:AND:ITEM:ta.amt:COP:GT:VALUE:DOM_REF:missing",
            ],
            {"n": "%x%"},
        )
        self.assertEqual(
            sql,
            "SELECT table_a.id \n FROM testdb.table_a \n WHERE  (  \n table_a.name LIKE '%x%' \n ) \n OR \n (  \n table_a.val > 2.5 \n )  \n;",
        )
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([("TA", "ID")], 1))

    def testAppendValueConditionsToSelect(self) -> None:
        sql = self.__build(
            [
                "SELECT_ITEM:1:ITEM:ta.id",
                "VALUE_CONDITION:1:LOP:AND:ITEM:ta.name:COP:EQ:VALUE:bob",
                "JOIN_CONDITION:2:LOP:AND:L_ITEM:ta.name:COP:EQ:R_ITEM:tb.x",
            ],
            appendValueConditonsToSelect=True,
        )
        self.assertIsNotNone(sql)
        self.assertTrue(str(sql).startswith("SELECT table_a.id,table_a.name \n"))
        # join conditions are not appended to the selection
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([("TA", "ID"), ("TA", "NAME")], 1))

    def testAppendValueConditionsWithoutSelection(self) -> None:
        """Documents current behaviour: appending to an empty selection raises (max() of an empty sequence)."""
        with self.assertRaises(ValueError):
            self.__build(["VALUE_CONDITION:1:LOP:AND:ITEM:ta.name:COP:EQ:VALUE:bob"], appendValueConditonsToSelect=True)

    def testValueListCondition(self) -> None:
        qdL = ["SELECT_ITEM:1:ITEM:ta.id", "VALUE_LIST_CONDITION:1:LOP:AND:ITEM:ta.name:COP:EQ:VALUE_LOP:OR:VALUE_LIST:DOM_REF:v"]
        sql = self.__build(qdL, {"v": ["a", "b"]}, appendValueConditonsToSelect=True)
        self.assertEqual(
            sql,
            "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n WHERE  (  \n (  \n table_a.name = 'a' \n ) \n OR \n (  \n table_a.name = 'b' \n ) \n )  \n;",
        )
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([("TA", "ID"), ("TA", "NAME")], 1))
        # single element list and scalar values are treated as a one element value list
        expected = "SELECT table_a.id \n FROM testdb.table_a \n WHERE  (  \n (  \n table_a.name = 'a' \n ) \n )  \n;"
        self.assertEqual(self.__build(qdL, {"v": ["a"]}), expected)
        self.assertEqual(self.__build(qdL, {"v": "a"}), expected)
        # empty value -> condition skipped
        self.assertEqual(self.__build(qdL, {"v": []}), "SELECT table_a.id \n FROM testdb.table_a \n;")

    def testValueListIndexedDomRef(self) -> None:
        qdL = ["SELECT_ITEM:1:ITEM:ta.id", "VALUE_LIST_CONDITION:1:LOP:AND:ITEM:ta.val:COP:GE:VALUE_LOP:AND:VALUE_LIST:DOM_REF_1:v"]
        sql = self.__build(qdL, {"v": ["a|1", "b|2"]})
        self.assertEqual(
            sql,
            "SELECT table_a.id \n FROM testdb.table_a \n WHERE  (  \n (  \n table_a.val >= 1 \n ) \n AND \n (  \n table_a.val >= 2 \n ) \n )  \n;",
        )

    def testIndexedDomRefVariants(self) -> None:
        qdL = ["SELECT_ITEM:1:ITEM:DOM_REF_1:k"]
        self.assertEqual(self.__build(qdL, {"k": "ta.id|ta.name"}), "SELECT table_a.name \n FROM testdb.table_a \n;")
        self.assertEqual(self.__build(qdL, {"k": ["ta.id|ta.val"]}), "SELECT table_a.val \n FROM testdb.table_a \n;")
        # missing or empty references produce no selection
        self.assertIsNone(self.__build(qdL, {}))
        self.assertIsNone(self.__build(qdL, {"k": ""}))
        self.assertIsNone(self.__build(qdL, {"k": None}))

    def testJoinCondition(self) -> None:
        sql = self.__build(["SELECT_ITEM:1:ITEM:ta.id", "JOIN_CONDITION:1:LOP:AND:L_ITEM:ta.name:COP:ne:R_ITEM:tb.x"])
        self.assertIsNotNone(sql)
        lines = str(sql).split("\n")
        self.assertEqual(sorted(lines[1].strip()[len("FROM ") :].split(",")), ["testdb.table_a", "testdb.table_b"])
        # automatic primary key join first, then the explicit join
        self.assertEqual(lines[2:], [" WHERE  (  ", " table_a.id = table_b.id ", " ) ", " AND ", " (  ", " table_a.name != table_b.x ", " )  ", ";"])

    def testKeyedConditionList(self) -> None:
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF_0:k",
            "SELECT_ITEM:2:ITEM:DOM_REF_1:k",
            "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:key",
            "CONDITION_LIST:1:KEY:big:LOP:OR:ITEM:ta.val:COP:GT:VALUE:100",
            "CONDITION_LIST:1:KEY:big:LOP:OR:ITEM:ta.amt:COP:GT:VALUE:5",
            "CONDITION_LIST:1:KEY:small:LOP:AND:ITEM:ta.val:COP:LT:VALUE:1",
            # missing value -> ignored
            "CONDITION_LIST:1:KEY:small:LOP:AND:ITEM:ta.val:COP:LT:VALUE:DOM_REF:nothere",
        ]
        domD = {"k": "ta.id|ta.name", "key": "big"}
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            sql = self.__build(qdL, domD, appendValueConditonsToSelect=True)
        self.assertEqual(
            sql,
            "SELECT table_a.id,table_a.name,table_a.val,table_a.amt \n FROM testdb.table_a \n WHERE  (  \n (  \n table_a.val > 100 \n ) \n OR \n"
            " (  \n table_a.amt > 5 \n ) \n )  \n;",
        )
        self.assertEqual(self.__mqd.getAttributeSelectList(), ([("TA", "ID"), ("TA", "NAME"), ("TA", "VAL"), ("TA", "AMT")], 2))
        self.assertTrue(any("condListId 1 keyValue 'big'" in msg for msg in cm.output))
        # Unknown key value or unknown condition list -> empty group and no WHERE content beyond grouping
        domD["key"] = "unknown"
        self.assertEqual(self.__build(qdL, domD), "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n;")
        qdL[2] = "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:9:VALUE:big"
        self.assertEqual(self.__build(qdL, domD), "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n;")
        # Keyed condition without a value is skipped
        qdL[2] = "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:nothere"
        self.assertEqual(self.__build(qdL, domD), "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n;")

    def testStatusHistoryDirectives(self) -> None:
        mqd = MyQueryDirectives(schemaDefObj=StatusHistorySchemaDef(verbose=False))
        domD = {"multiselect": "pdbx_database_status_history.entry_id|pdbx_database_status_history.status_code_end", "beginstat1": "auth"}
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF_0:multiselect",
            "SELECT_ITEM:2:ITEM:DOM_REF_1:multiselect",
            "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:2:VALUE:DOM_REF:beginstat1",
            "CONDITION_LIST:2:KEY:auth:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:AUTH",
            "CONDITION_LIST:2:KEY:hold:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:HOLD",
            "ORDER_ITEM:1:ITEM:DOM_REF_0:multiselect:SORT_ORDER:INCREASING",
        ]
        sql = mqd.build(queryDirL=qdL, domD=domD)
        self.assertEqual(
            sql,
            "SELECT pdbx_database_status_history.entry_id,pdbx_database_status_history.status_code_end \n FROM da_internal.pdbx_database_status_history \n"
            " WHERE  (  \n (  \n pdbx_database_status_history.status_code_begin = 'AUTH' \n ) \n )  \n ORDER BY pdbx_database_status_history.entry_id ASC \n;",
        )

    def testIncompleteDirectivesQuiet(self) -> None:
        """Incomplete condition definitions abort parsing of the remaining directives without raising."""
        for bad in [
            "VALUE_CONDITION:1:LOP:AND:FOO:ta.name:COP:EQ:VALUE:x",
            "VALUE_LIST_CONDITION:1:LOP:AND:ITEM:ta.name:COP:EQ:FOO:OR:VALUE_LIST:x",
            "JOIN_CONDITION:1:LOP:AND:L_ITEM:ta.id:COP:EQ:FOO:tb.id",
            "CONDITION_LIST:1:FOO:k:LOP:OR:ITEM:ta.val:COP:GT:VALUE:1",
            "VALUE_KEYED_CONDITION:1:LOP:AND:FOO:1:VALUE:x",
        ]:
            sql = self.__build(["SELECT_ITEM:1:ITEM:ta.id", bad, "SELECT_ITEM:2:ITEM:ta.name"])
            self.assertEqual(sql, "SELECT table_a.id \n FROM testdb.table_a \n;", bad)

    def testIncompleteDirectiveVerboseLogging(self) -> None:
        mqd = MyQueryDirectives(schemaDefObj=self.__sd, verbose=True)
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            sql = mqd.build(
                queryDirL=[
                    "SELECT_ITEM:1:ITEM:ta.id",
                    "SELECT_ITEM:2:ITEM:DOM_REF:none",
                    "ORDER_ITEM:1:ITEM:DOM_REF:none:SORT_ORDER:ASC",
                    "JOIN_CONDITION:1:LOP:AND:L_ITEM:ta.id",
                ],
                domD={},
            )
        self.assertEqual(sql, "SELECT table_a.id \n FROM testdb.table_a \n;")
        out = "\n".join(cm.output)
        self.assertIn("dom dictionary length domD 0", out)
        self.assertIn("selection incomplete at i = 4", out)
        self.assertIn("orderby incomplete at i = 8", out)
        self.assertIn("fails with index", out)
        self.assertIn("Join condition incomplete", out)
        self.assertIn("__parseTokenList failure", out)
        self.assertIn("sql: SELECT table_a.id", out)

    def testVerboseSummaryLogging(self) -> None:
        mqd = MyQueryDirectives(schemaDefObj=self.__sd, verbose=True)
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            mqd.build(
                queryDirL=[
                    "SELECT_ITEM:1:ITEM:ta.id",
                    "ORDER_ITEM:1:ITEM:ta.id:SORT_ORDER:ASC",
                    "VALUE_KEYED_CONDITION:2:LOP:AND:CONDITION_LIST_ID:1:VALUE:k",
                    "CONDITION_LIST:1:KEY:k:LOP:OR:ITEM:ta.val:COP:GT:VALUE:1",
                ]
            )
        out = "\n".join(cm.output)
        self.assertIn("select 1  ('TA', 'ID')", out)
        self.assertIn("order  1  (('TA', 'ID'), 'ASC')", out)
        self.assertIn("keycondD  2  (1, 'k', 'AND')", out)
        self.assertIn("condListD 1  'k'", out)
        self.assertIn("type 'cType': 'group'", out)

    def testDomSubstitutionFailureVerbose(self) -> None:
        """A non-sized dom value aborts dom substitution; remaining directives are dropped."""
        mqd = MyQueryDirectives(schemaDefObj=self.__sd, verbose=True)
        with self.assertLogs(LOGGER_NAME, level="INFO") as cm:
            sql = mqd.build(queryDirL=["SELECT_ITEM:1:ITEM:ta.id", "SELECT_ITEM:2:ITEM:DOM_REF:n"], domD={"n": 5})
        self.assertEqual(sql, "SELECT table_a.id \n FROM testdb.table_a \n;")
        self.assertIn("Failed in queryDirSub", "\n".join(cm.output))

    def testDomSubstitutionFailureQuiet(self) -> None:
        with patch("wwpdb.utils.db.MyQueryDirectives.logger") as mockLogger:
            sql = self.__build(["SELECT_ITEM:1:ITEM:ta.id", "SELECT_ITEM:2:ITEM:DOM_REF_0:n"], {"n": 5})
        self.assertEqual(sql, "SELECT table_a.id \n FROM testdb.table_a \n;")
        mockLogger.error.assert_not_called()
        mockLogger.exception.assert_not_called()

    def testSqlGeneratorInteractions(self) -> None:
        """Verify the calls made on the (mocked) SQL generator classes."""
        with patch("wwpdb.utils.db.MyQueryDirectives.MyDbQuerySqlGen") as mockQGen, patch("wwpdb.utils.db.MyQueryDirectives.MyDbConditionSqlGen") as mockCGen:
            qGen = mockQGen.return_value
            cGen = mockCGen.return_value
            qGen.getSql.return_value = "SELECT 1;"
            sql = self.__build(
                [
                    "SELECT_ITEM:1:ITEM:ta.id",
                    "VALUE_CONDITION:1:LOP:AND:ITEM:ta.name:COP:eq:VALUE:bob",
                    "VALUE_LIST_CONDITION:2:LOP:OR:ITEM:ta.val:COP:lt:VALUE_LOP:and:VALUE_LIST:DOM_REF:v",
                    "JOIN_CONDITION:3:LOP:AND:L_ITEM:ta.id:COP:EQ:R_ITEM:tb.id",
                    "ORDER_ITEM:1:ITEM:ta.val:SORT_ORDER:DESC",
                ],
                {"v": ["1", "2"]},
            )
        self.assertEqual(sql, "SELECT 1;")
        mockQGen.assert_called_once_with(schemaDefObj=self.__sd, verbose=False, log=ANY)
        qGen.addSelectAttributeId.assert_called_once_with(attributeTuple=("TA", "ID"))
        cGen.addValueCondition.assert_called_once_with(lhsTuple=("TA", "NAME"), opCode="EQ", rhsTuple=("bob", "VARCHAR"), preOp="AND")
        cGen.addGroupValueConditionList.assert_called_once_with(
            [("AND", ("TA", "VAL"), "LT", ("1", "FLOAT")), ("AND", ("TA", "VAL"), "LT", ("2", "FLOAT"))], preOp="OR"
        )
        cGen.addJoinCondition.assert_called_once_with(lhsTuple=("TA", "ID"), opCode="EQ", rhsTuple=("TB", "ID"), preOp="AND")
        cGen.addTables.assert_called_once_with(["TA"])
        qGen.setCondition.assert_called_once_with(cGen)
        qGen.addOrderByAttributeId.assert_called_once_with(attributeTuple=("TA", "VAL"), sortFlag="DESC")
        self.assertEqual(qGen.method_calls[-2:], [call.getSql(), call.clear()])

    def testSchemaLookupWithMock(self) -> None:
        """Attribute types used for value conditions are looked up from the schema definition."""
        sd = MagicMock(spec=SchemaDefBase)
        tObj = MagicMock()
        tObj.getAttributeType.return_value = "CHAR"
        sd.getTable.return_value = tObj
        with patch("wwpdb.utils.db.MyQueryDirectives.MyDbQuerySqlGen"), patch("wwpdb.utils.db.MyQueryDirectives.MyDbConditionSqlGen") as mockCGen:
            mqd = MyQueryDirectives(schemaDefObj=sd)
            mqd.build(queryDirL=["SELECT_ITEM:1:ITEM:t.a", "VALUE_CONDITION:1:LOP:AND:ITEM:t.b:COP:EQ:VALUE:z"])
        sd.getTable.assert_called_once_with("T")
        tObj.getAttributeType.assert_called_once_with("B")
        mockCGen.return_value.addValueCondition.assert_called_once_with(lhsTuple=("T", "B"), opCode="EQ", rhsTuple=("z", "CHAR"), preOp="AND")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    unittest.main()
