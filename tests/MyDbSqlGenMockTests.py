##
# File:    MyDbSqlGenMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Unit tests for the SQL generators in MyDbSqlGen  --  no database connection required.

Real schema definitions (StatusHistorySchemaDef and a small in-test schema) are used as inputs
and mocks are used where a test needs to observe interactions with the schema definition object.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import io
import unittest
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock

from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen, MyDbConditionSqlGen, MyDbQuerySqlGen
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase, TableDef
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef


def _attrInfo(sqlType: str, width: int = 0, precision: int = 0, nullable: bool = True, primaryKey: bool = False, order: int = 1) -> Dict[str, Any]:
    return {"SQL_TYPE": sqlType, "WIDTH": width, "PRECISION": precision, "NULLABLE": nullable, "PRIMARY_KEY": primaryKey, "ORDER": order}


def _makeSchemaDict() -> Dict[str, Any]:
    """A small two table schema sharing the primary key attribute ID."""
    return {
        "TA": {
            "TABLE_ID": "TA",
            "TABLE_NAME": "table_a",
            "TABLE_TYPE": "transactional",
            "ATTRIBUTES": {"ID": "id", "NAME": "name", "VAL": "val", "DT": "dt", "AMT": "amt", "BLOB": "blob"},
            "ATTRIBUTE_INFO": {
                "ID": _attrInfo("INT", nullable=False, primaryKey=True, order=1),
                "NAME": _attrInfo("VARCHAR", width=20, order=2),
                "VAL": _attrInfo("FLOAT", width=10, order=3),
                "DT": _attrInfo("DATE", width=10, order=4),
                "AMT": _attrInfo("DECIMAL", width=10, precision=2, nullable=False, order=5),
                "BLOB": _attrInfo("BLOB", order=6),
            },
            "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ["ID"]}, "i1": {"TYPE": "SEARCH", "ATTRIBUTES": ["NAME", "DT"]}},
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
        "TC": {
            "TABLE_ID": "TC",
            "TABLE_NAME": "table_c",
            "TABLE_TYPE": "MyISAM",
            "ATTRIBUTES": {"CODE": "code", "N": "n"},
            "ATTRIBUTE_INFO": {
                "CODE": _attrInfo("CHAR", width=4, nullable=False, order=1),
                "N": _attrInfo("BIGINT", order=2),
            },
            "INDICES": {},
        },
    }


class MyDbAdminSqlGenMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sd = StatusHistorySchemaDef(verbose=False)
        self.__tObj = self.__sd.getTable("PDBX_DATABASE_STATUS_HISTORY")
        self.__testSd = SchemaDefBase(databaseName="testdb", schemaDefDict=_makeSchemaDict(), verbose=False)
        self.__gen = MyDbAdminSqlGen()

    def testTruncate(self) -> None:
        self.assertEqual(self.__gen.truncateTableSQL("db", "tb"), "TRUNCATE TABLE db.tb; ")

    def testIdUpdateTemplateSingleCondition(self) -> None:
        sql = self.__gen.idUpdateTemplateSQL("db", self.__tObj, ["PDB_ID", "DELTA_DAYS", "DATE_BEGIN"], ["ENTRY_ID"])
        self.assertEqual(sql, "UPDATE db.pdbx_database_status_history SET  pdb_id=%s, delta_days=%s, date_begin=%s WHERE ( entry_id=%s);")
        # Template accepts values for update and condition placeholders
        self.assertEqual(sql.count("%s"), 4)

    def testIdUpdateTemplateNoCondition(self) -> None:
        self.assertEqual(self.__gen.idUpdateTemplateSQL("db", self.__tObj, ["PDB_ID"]), "UPDATE db.pdbx_database_status_history SET  pdb_id=%s;")
        self.assertEqual(self.__gen.idUpdateTemplateSQL("db", self.__tObj), "UPDATE db.pdbx_database_status_history SET ;")

    def testIdUpdateTemplateMultipleConditionsCurrent(self) -> None:
        """Documents current output: multiple conditions are joined with ',' (see expected-failure test below)."""
        sql = self.__gen.idUpdateTemplateSQL("db", self.__tObj, ["PDB_ID"], ["ENTRY_ID", "ORDINAL"])
        self.assertEqual(sql, "UPDATE db.pdbx_database_status_history SET  pdb_id=%s WHERE ( entry_id=%s, ordinal=%s);")

    @unittest.expectedFailure
    def testIdUpdateTemplateMultipleConditionsUseAnd(self) -> None:
        """BUG: multiple WHERE conditions should be combined with AND (as in idDeleteTemplateSQL), not ','."""
        sql = self.__gen.idUpdateTemplateSQL("db", self.__tObj, ["PDB_ID"], ["ENTRY_ID", "ORDINAL"])
        self.assertEqual(sql, "UPDATE db.pdbx_database_status_history SET  pdb_id=%s WHERE ( entry_id=%s AND  ordinal=%s);")

    def testIdUpdateTemplateNonNumericType(self) -> None:
        # BLOB is neither string, float nor integer - exercises the alternate placeholder branch
        tObj = self.__testSd.getTable("TA")
        sql = self.__gen.idUpdateTemplateSQL("db", tObj, ["BLOB", "DT"], ["ID"])
        self.assertEqual(sql, "UPDATE db.table_a SET  blob=%s, dt=%s WHERE ( id=%s);")

    def testIdInsertTemplate(self) -> None:
        sql = self.__gen.idInsertTemplateSQL("db", self.__tObj, ["ORDINAL", "ENTRY_ID", "DATE_BEGIN"])
        self.assertEqual(sql, "INSERT INTO db.pdbx_database_status_history (ordinal,entry_id,date_begin) VALUES (%s,%s,%s);")
        self.assertEqual(self.__gen.idInsertTemplateSQL("db", self.__tObj), "INSERT INTO db.pdbx_database_status_history () VALUES ();")
        tObj = self.__testSd.getTable("TA")
        self.assertEqual(self.__gen.idInsertTemplateSQL("db", tObj, ["BLOB"]), "INSERT INTO db.table_a (blob) VALUES (%s);")

    def testIdDeleteTemplate(self) -> None:
        sql = self.__gen.idDeleteTemplateSQL("db", self.__tObj, ["ENTRY_ID", "ORDINAL"])
        self.assertEqual(sql, "DELETE FROM  db.pdbx_database_status_history WHERE ( entry_id=%s AND  ordinal=%s);")
        self.assertEqual(self.__gen.idDeleteTemplateSQL("db", self.__tObj), "DELETE FROM  db.pdbx_database_status_history;")

    def testInsertTemplate(self) -> None:
        self.assertEqual(self.__gen.insertTemplateSQL("db", "tb", ["a", "b"]), "INSERT INTO db.tb (a,b) VALUES (%s,%s);")
        self.assertEqual(self.__gen.insertTemplateSQL("db", "tb"), "INSERT INTO db.tb () VALUES ();")

    def testDeleteTemplate(self) -> None:
        self.assertEqual(self.__gen.deleteTemplateSQL("db", "tb", ["a", "b"]), "DELETE FROM db.tb WHERE  a=%s AND  b=%s;")
        # No guard against an empty attribute list
        self.assertEqual(self.__gen.deleteTemplateSQL("db", "tb"), "DELETE FROM db.tb WHERE ;")

    def testDeleteFromList(self) -> None:
        sqlL = self.__gen.deleteFromListSQL("db", "tb", "id", ["1", "2", "3"], chunkSize=2)
        self.assertEqual(sqlL, ["DELETE FROM db.tb WHERE id IN ('1','2'); ", "DELETE FROM db.tb WHERE id IN ('3'); "])
        # Default chunk size of 10
        sqlL = self.__gen.deleteFromListSQL("db", "tb", "id", [str(i) for i in range(25)])
        self.assertEqual(len(sqlL), 3)
        self.assertEqual(sqlL[2], "DELETE FROM db.tb WHERE id IN ('20','21','22','23','24'); ")
        self.assertEqual(self.__gen.deleteFromListSQL("db", "tb", "id", []), [])

    def testCreateDatabase(self) -> None:
        self.assertEqual(self.__gen.createDatabaseSQL("db"), ["DROP DATABASE IF EXISTS db;", "CREATE DATABASE db;"])

    def testCreateTableStatusHistory(self) -> None:
        sqlL = self.__gen.createTableSQL("da_internal", self.__tObj)
        self.assertEqual(len(sqlL), 7)
        self.assertEqual(sqlL[0], "USE da_internal;")
        self.assertEqual(sqlL[1], "DROP TABLE IF EXISTS pdbx_database_status_history;")
        createL = sqlL[2].split("\n")
        self.assertEqual(createL[0], "CREATE TABLE pdbx_database_status_history (")
        self.assertEqual(createL[1], "%-40s %-16s  %s," % ("ordinal", "INT UNSIGNED", "not null"))
        self.assertEqual(createL[2], "%-40s %-16s  %s," % ("entry_id", "CHAR(15)", "not null"))
        self.assertEqual(createL[6], "%-40s %-16s  %s," % ("status_code_begin", "VARCHAR(24)", "not null"))
        self.assertEqual(createL[-2], "PRIMARY KEY (ordinal,entry_id)")
        self.assertEqual(createL[-1], ") ENGINE InnoDB;")
        self.assertEqual(
            sqlL[3:],
            [
                "CREATE UNIQUE INDEX p1 on pdbx_database_status_history ( ordinal, entry_id );",
                "CREATE  INDEX i1 on pdbx_database_status_history ( entry_id );",
                "CREATE  INDEX i2 on pdbx_database_status_history ( annotator );",
                "CREATE  INDEX i3 on pdbx_database_status_history ( entry_id, status_code_begin, status_code_end );",
            ],
        )

    def testCreateTableTypesAndUnknownTypeVerbose(self) -> None:
        lfh = io.StringIO()
        gen = MyDbAdminSqlGen(verbose=True, log=lfh)
        sqlL = gen.createTableSQL("testdb", self.__testSd.getTable("TA"))
        self.assertEqual(sqlL[0], "USE testdb;")
        self.assertEqual(sqlL[1], "DROP TABLE IF EXISTS table_a;")
        expected = "\n".join(
            [
                "CREATE TABLE table_a (",
                "%-40s %-16s  %s," % ("id", "INT", "not null"),
                "%-40s %-16s  %s," % ("name", "VARCHAR(20)", "    null default null"),
                "%-40s %-16s  %s," % ("val", "FLOAT", "    null default null"),
                "%-40s %-16s  %s," % ("dt", "DATE", "    null default null"),
                "%-40s %-16s  %s," % ("amt", "DECIMAL(10,2)", "not null"),
                "PRIMARY KEY (id)",
                ") ENGINE InnoDB;",
            ]
        )
        self.assertEqual(sqlL[2], expected)
        self.assertEqual(sqlL[3:], ["CREATE UNIQUE INDEX p1 on table_a ( id );", "CREATE  INDEX i1 on table_a ( name, dt );"])
        # The unsupported BLOB column is skipped and reported to the log handle
        self.assertNotIn("blob", sqlL[2])
        self.assertIn("unknown sqlType BLOB", lfh.getvalue())

    def testCreateTableUnknownTypeQuiet(self) -> None:
        lfh = io.StringIO()
        gen = MyDbAdminSqlGen(verbose=False, log=lfh)
        sqlL = gen.createTableSQL("testdb", self.__testSd.getTable("TA"))
        self.assertNotIn("blob", sqlL[2])
        self.assertEqual(lfh.getvalue(), "")

    def testCreateTableMyISAM(self) -> None:
        sqlL = self.__gen.createTableSQL("testdb", self.__testSd.getTable("TB"))
        self.assertEqual(len(sqlL), 3)
        self.assertEqual(
            sqlL[2],
            "\n".join(
                [
                    "CREATE TABLE table_b (",
                    "%-40s %-16s  %s," % ("id", "INT", "not null"),
                    "%-40s %-16s  %s," % ("x", "TEXT", "    null default null"),
                    "PRIMARY KEY (id)",
                    ") ENGINE MyISAM;",
                ]
            ),
        )

    def testCreateTableNoPrimaryKeyCurrent(self) -> None:
        """Documents current output: without a primary key the last column keeps its trailing comma."""
        sqlL = self.__gen.createTableSQL("testdb", self.__testSd.getTable("TC"))
        createL = sqlL[2].split("\n")
        self.assertEqual(createL[1], "%-40s %-16s  %s," % ("code", "CHAR(4)", "not null"))
        self.assertEqual(createL[2], "%-40s %-16s  %s," % ("n", "BIGINT", "    null default null"))
        self.assertEqual(createL[3], ") ENGINE MyISAM;")

    @unittest.expectedFailure
    def testCreateTableNoPrimaryKeyValidSql(self) -> None:
        """BUG: a table without a primary key produces 'col ...,\\n) ENGINE' - a dangling comma that MySQL rejects."""
        sqlL = self.__gen.createTableSQL("testdb", self.__testSd.getTable("TC"))
        self.assertNotIn(",\n)", sqlL[2])

    def testCreateTableWithMockTableDef(self) -> None:
        """Interaction test using a mocked table definition."""
        tObj = MagicMock(spec=TableDef)
        tObj.getName.return_value = "mtab"
        tObj.getType.return_value = None
        tObj.getAttributeIdList.return_value = ["A", "B", "C"]
        tObj.getAttributeName.side_effect = lambda aId: aId.lower()
        tObj.getAttributeType.side_effect = {"A": "INTEGER", "B": "NUMERIC", "C": "LONGTEXT"}.get
        tObj.getAttributeWidth.return_value = 8
        tObj.getAttributePrecision.return_value = 3
        tObj.getAttributeNullable.return_value = False
        tObj.getAttributeIsPrimaryKey.side_effect = lambda aId: aId == "A"
        tObj.getIndexNames.return_value = ["ix"]
        tObj.getIndexType.return_value = "search"
        tObj.getIndexAttributeIdList.return_value = ["B"]
        sqlL = self.__gen.createTableSQL("mdb", tObj)
        self.assertEqual(sqlL[:2], ["USE mdb;", "DROP TABLE IF EXISTS mtab;"])
        self.assertIn("%-40s %-16s  %s," % ("b", "NUMERIC(8,3)", "not null"), sqlL[2])
        self.assertIn("%-40s %-16s  %s," % ("c", "LONGTEXT", "not null"), sqlL[2])
        self.assertTrue(sqlL[2].endswith("PRIMARY KEY (a)\n) ENGINE MyISAM;"))
        self.assertEqual(sqlL[3], "CREATE  INDEX ix on mtab ( b );")
        tObj.getIndexAttributeIdList.assert_called_once_with("ix")

    def testExportTable(self) -> None:
        cols = "ordinal,entry_id,pdb_id,date_begin,date_end,status_code_begin,status_code_end,annotator,details,delta_days"
        sql = self.__gen.exportTable("db", self.__tObj, "/tmp/x.tdd")
        self.assertEqual(
            sql,
            "SELECT %s \n INTO OUTFILE /tmp/x.tdd \nFIELDS TERMINATED BY '&##&\\t' \nLINES  TERMINATED BY '$##$\\n' \nFROM db.pdbx_database_status_history \n;"
            % cols,
        )
        sql = self.__gen.exportTable("db", self.__tObj, "/tmp/x.tdd", withDoubleQuotes=True)
        self.assertIn("\n OPTIONALLY ENCLOSED BY '\"' \n", sql)

    def testImportTable(self) -> None:
        cols = "ordinal,entry_id,pdb_id,date_begin,date_end,status_code_begin,status_code_end,annotator,details,delta_days"
        sql = self.__gen.importTable("db", self.__tObj, "/tmp/x.tdd")
        self.assertEqual(
            sql,
            "LOAD DATA LOCAL INFILE '/tmp/x.tdd'  INTO TABLE  db.pdbx_database_status_history  FIELDS TERMINATED BY '&##&\\t'  "
            "LINES  TERMINATED BY '$##$\\n'   (%s)  ;" % cols,
        )
        sql = self.__gen.importTable("db", self.__tObj, "/tmp/x.tdd", withTruncate=True, withDoubleQuotes=True)
        self.assertTrue(sql.startswith("TRUNCATE TABLE db.pdbx_database_status_history;  LOAD DATA LOCAL INFILE '/tmp/x.tdd' "))
        self.assertIn(" OPTIONALLY ENCLOSED BY '\"' ", sql)


class MyDbQuerySqlGenMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sd = SchemaDefBase(databaseName="testdb", schemaDefDict=_makeSchemaDict(), verbose=False)

    def testEmptySelect(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        self.assertIsNone(qGen.getSql())

    def testSimpleSelect(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        self.assertTrue(qGen.addSelectAttributeId(("TA", "ID")))
        self.assertTrue(qGen.addSelectAttributeId(("TA", "NAME")))
        self.assertEqual(qGen.getSql(), "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n;")

    def testSetDatabaseAndClear(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.setDatabase("otherdb")
        qGen.addSelectAttributeId(("TB", "X"))
        self.assertEqual(qGen.getSql(), "SELECT table_b.x \n FROM otherdb.table_b \n;")
        # clear() resets selections and the database name to the schema default
        qGen.clear()
        self.assertIsNone(qGen.getSql())
        qGen.addSelectAttributeId(("TB", "X"))
        self.assertEqual(qGen.getSql(), "SELECT table_b.x \n FROM testdb.table_b \n;")

    def testSelectLimit(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.addSelectAttributeId(("TA", "ID"))
        self.assertFalse(qGen.addSelectLimit())
        self.assertFalse(qGen.addSelectLimit("x", 1))
        self.assertEqual(qGen.getSql(), "SELECT table_a.id \n FROM testdb.table_a \n;")
        self.assertTrue(qGen.addSelectLimit("5", 10))
        self.assertEqual(qGen.getSql(), "SELECT table_a.id \n FROM testdb.table_a \n LIMIT 5, 10 \n;")

    def testOrderingAndCondition(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.addSelectAttributeId(("TA", "ID"))
        qGen.addSelectAttributeId(("TA", "NAME"))
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.addValueCondition(("TA", "NAME"), "EQ", ("bob", "char"))
        cGen.addValueCondition(("TA", "VAL"), "GT", (1.5, "float"), preOp="OR")
        cGen.addValueCondition(("TA", "DT"), "LE", ("2020-01-01", "date"))
        self.assertTrue(qGen.setCondition(cGen))
        # default sort order is DESC until changed
        self.assertTrue(qGen.addOrderByAttributeId(("TA", "ID")))
        qGen.setOrderBySortOrder("ASC")
        qGen.addOrderByAttributeId(("TA", "NAME"))
        qGen.addOrderByAttributeId(("TA", "VAL"), sortFlag="DESC")
        qGen.addSelectLimit(0, 10)
        expected = (
            "SELECT table_a.id,table_a.name \n FROM testdb.table_a \n WHERE  (  \n table_a.name = 'bob' \n ) \n OR \n (  \n table_a.val > 1.5 \n ) \n AND \n"
            " (  \n table_a.dt <= '2020-01-01' \n )  \n ORDER BY table_a.id DESC,table_a.name ASC,table_a.val DESC \n LIMIT 0, 10 \n;"
        )
        self.assertEqual(qGen.getSql(), expected)

    def testSetOrderBySortOrderDefault(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.addSelectAttributeId(("TA", "ID"))
        qGen.setOrderBySortOrder()
        qGen.addOrderByAttributeId(("TA", "ID"))
        self.assertEqual(qGen.getSql(), "SELECT table_a.id \n FROM testdb.table_a \n ORDER BY table_a.id ASC \n;")

    def testEmptyConditionOmitsWhere(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.addSelectAttributeId(("TA", "ID"))
        qGen.setCondition(MyDbConditionSqlGen(self.__sd))
        self.assertEqual(qGen.getSql(), "SELECT table_a.id \n FROM testdb.table_a \n;")

    def testMultiTableAutoJoin(self) -> None:
        qGen = MyDbQuerySqlGen(self.__sd)
        qGen.addSelectAttributeId(("TA", "ID"))
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.addValueCondition(("TB", "X"), "LIKE", ("%z%", "char"))
        qGen.setCondition(cGen)
        sql = qGen.getSql()
        self.assertIsNotNone(sql)
        lines = str(sql).split("\n")
        self.assertEqual(lines[0], "SELECT table_a.id ")
        # Table order in the FROM clause derives from a set
        self.assertTrue(lines[1].startswith(" FROM "))
        self.assertEqual(sorted(lines[1].strip()[len("FROM ") :].split(",")), ["testdb.table_a", "testdb.table_b"])
        # Selected tables are not passed to the condition object, so no key join is generated ...
        self.assertEqual(lines[2:], [" WHERE  (  ", " table_b.x LIKE '%z%' ", " )  ", ";"])
        # ... unless the caller registers them on the condition explicitly
        cGen.clear()
        cGen.addValueCondition(("TB", "X"), "LIKE", ("%z%", "char"))
        cGen.addTables(["TA"])
        lines = str(qGen.getSql()).split("\n")
        self.assertIn(" table_b.id = table_a.id ", lines)
        self.assertIn(" table_b.x LIKE '%z%' ", lines)

    def testWithMockSchema(self) -> None:
        sd = MagicMock(spec=SchemaDefBase)
        sd.getDatabaseName.return_value = "mockdb"
        sd.getQualifiedAttributeName.side_effect = lambda aTup: "%s.%s" % (aTup[0].lower(), aTup[1].lower())
        sd.getTableName.side_effect = lambda tId: "t_" + tId.lower()
        qGen = MyDbQuerySqlGen(sd)
        qGen.addSelectAttributeId(("T1", "A1"))
        qGen.addOrderByAttributeId(("T1", "A1"), sortFlag="ASC")
        cond = MagicMock(spec=MyDbConditionSqlGen)
        cond.getSql.return_value = "t1.a1 = 1"
        cond.getTableIdList.return_value = ["T1"]
        qGen.setCondition(cond)
        self.assertEqual(qGen.getSql(), "SELECT t1.a1 \n FROM mockdb.t_t1 \n WHERE t1.a1 = 1 \n ORDER BY t1.a1 ASC \n;")
        cond.getSql.assert_called_once_with()
        sd.getTableName.assert_called_once_with("T1")


class MyDbConditionSqlGenMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sd = SchemaDefBase(databaseName="testdb", schemaDefDict=_makeSchemaDict(), verbose=False)

    def testAddValueConditionStructure(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd)
        self.assertEqual(cGen.addValueCondition(("TA", "NAME"), "EQ", ("bob", "char")), 1)
        # Duplicate conditions are ignored
        self.assertEqual(cGen.addValueCondition(("TA", "NAME"), "EQ", ("bob", "char")), 1)
        self.assertEqual(cGen.addValueCondition(("TA", "VAL"), "GE", (2, "int"), preOp="NONE"), 2)
        self.assertEqual(
            cGen.get(),
            [
                ("LOG_OP", "AND"),
                ("GROUPING", "BEGIN"),
                ("VALUE_CONDITION", ("TA", "NAME"), "EQ", ("bob", "char")),
                ("GROUPING", "END"),
                ("GROUPING", "BEGIN"),
                ("VALUE_CONDITION", ("TA", "VAL"), "GE", (2, "int")),
                ("GROUPING", "END"),
            ],
        )
        self.assertEqual(cGen.getTableIdList(), ["TA"])

    def testOperators(self) -> None:
        opL: List[Tuple[str, str]] = [
            ("EQ", "="),
            ("NE", "!="),
            ("GE", ">="),
            ("GT", ">"),
            ("LT", "<"),
            ("LE", "<="),
            ("LIKE", "LIKE"),
            ("NOT LIKE", "NOT LIKE"),
            ("IS", "IS"),
            ("IS NOT", "IS NOT"),
        ]
        for opCode, sqlOp in opL:
            cGen = MyDbConditionSqlGen(self.__sd)
            cGen.addValueCondition(("TA", "VAL"), opCode, (3, "int"))
            self.assertEqual(cGen.getSql(), " (  \n table_a.val %s 3 \n ) " % sqlOp)

    def testValueQuoting(self) -> None:
        for vType, quoted in [("char", True), ("VARCHAR", True), ("date", True), ("datetime", True), ("int", False), ("float", False), ("other", False)]:
            cGen = MyDbConditionSqlGen(self.__sd)
            cGen.addValueCondition(("TA", "NAME"), "EQ", ("v", vType))
            exp = " table_a.name = 'v' " if quoted else " table_a.name = v "
            self.assertEqual(cGen.getSql().split("\n")[1], exp)

    def testGroupValueConditionList(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd, addKeyJoinFlag=False)
        self.assertEqual(cGen.addGroupValueConditionList([]), 0)
        self.assertEqual(cGen.get(), [])
        n = cGen.addGroupValueConditionList(
            [("OR", ("TA", "NAME"), "LIKE", ("%a%", "char")), ("OR", ("TA", "NAME"), "NOT LIKE", ("%b%", "char"))],
            preOp="NOT",
        )
        self.assertEqual(n, 2)
        self.assertEqual(cGen.getSql(), " (  \n (  \n table_a.name LIKE '%a%' \n ) \n OR \n (  \n table_a.name NOT LIKE '%b%' \n ) \n ) ")
        self.assertEqual(cGen.getTableIdList(), ["TA"])

    def testJoinCondition(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd, addKeyJoinFlag=False)
        self.assertEqual(cGen.addJoinCondition(("TA", "ID"), "EQ", ("TB", "ID")), 1)
        self.assertEqual(cGen.addJoinCondition(("TA", "ID"), "EQ", ("TB", "ID")), 1)
        self.assertEqual(cGen.addJoinCondition(("TA", "NAME"), "NE", ("TB", "X"), preOp="OR"), 2)
        self.assertEqual(cGen.getTableIdList(), ["TA", "TB"])
        self.assertEqual(cGen.getSql(), " (  \n table_a.id = table_b.id \n ) \n OR \n (  \n table_a.name != table_b.x \n ) ")

    def testLeadingLogicalOpSuppressed(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.addLogicalOp("OR")
        cGen.addBeginGroup()
        cGen.addEndGroup()
        self.assertEqual(cGen.getSql(), " (  \n ) ")

    def testNotLogicalOpIsDropped(self) -> None:
        """Documents current behaviour: a 'NOT' logical operation is not rendered."""
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.addValueCondition(("TA", "ID"), "EQ", (1, "int"))
        cGen.addLogicalOp("NOT")
        self.assertEqual(cGen.getSql(), " (  \n table_a.id = 1 \n ) ")

    def testUnknownConditionTypeIgnored(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd)
        self.assertTrue(cGen.set([("UNKNOWN_THING", "x"), ("GROUPING", "BEGIN"), ("GROUPING", "OTHER"), ("GROUPING", "END")]))
        self.assertEqual(cGen.getTableIdList(), [])
        self.assertEqual(cGen.getSql(), " (  \n ) ")

    def testSetAndClear(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd, addKeyJoinFlag=False)
        self.assertFalse(cGen.set())
        cL = [
            ("VALUE_CONDITION", ("TA", "ID"), "EQ", (1, "int")),
            ("LOG_OP", "AND"),
            ("JOIN_CONDITION", ("TA", "ID"), "EQ", ("TB", "ID")),
            ("VALUE_LIST_CONDITION", ("TC", "CODE"), "IN", (["a"], "char")),
        ]
        self.assertTrue(cGen.set(cL))
        self.assertEqual(cGen.get(), cL)
        self.assertEqual(cGen.getTableIdList(), ["TA", "TB", "TC"])
        self.assertTrue(cGen.clear())
        self.assertEqual(cGen.get(), [])
        self.assertEqual(cGen.getTableIdList(), [])
        self.assertEqual(cGen.getSql(), "")

    def testAddTables(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd)
        self.assertTrue(cGen.addTables(["TA", "TB", "TA"]))
        self.assertEqual(cGen.getTableIdList(), ["TA", "TB"])

    def testKeyAttributeEquiJoin(self) -> None:
        cGen = MyDbConditionSqlGen(self.__sd, addKeyJoinFlag=False)
        self.assertEqual(cGen.addKeyAttributeEquiJoinConditions(), 0)
        cGen.addValueCondition(("TA", "NAME"), "EQ", ("bob", "char"))
        cGen.addTables(["TB", "TC"])
        # three tables -> three pairs; only TA/TB share a primary key attribute
        self.assertEqual(cGen.addKeyAttributeEquiJoinConditions(), 3)
        self.assertEqual(cGen.getSql(), " (  \n table_a.id = table_b.id \n ) \n AND \n (  \n table_a.name = 'bob' \n ) ")

    def testKeyJoinVerboseLogging(self) -> None:
        lfh = io.StringIO()
        cGen = MyDbConditionSqlGen(self.__sd, verbose=True, log=lfh)
        cGen.addTables(["TA", "TB"])
        self.assertEqual(cGen.getSql(), " (  \n table_a.id = table_b.id \n ) ")
        self.assertIn("lTable TA rTable TB  common keys {'ID'}", lfh.getvalue())

    def testKeyJoinWithMockSchema(self) -> None:
        sd = MagicMock(spec=SchemaDefBase)
        t1 = MagicMock(spec=TableDef)
        t1.getPrimaryKeyAttributeIdList.return_value = ["K"]
        t2 = MagicMock(spec=TableDef)
        t2.getPrimaryKeyAttributeIdList.return_value = ["K", "Z"]
        sd.getTable.side_effect = {"T1": t1, "T2": t2}.get
        sd.getQualifiedAttributeName.side_effect = lambda tableAttributeTuple: ".".join(tableAttributeTuple).lower()
        cGen = MyDbConditionSqlGen(sd)
        cGen.addTables(["T1", "T2"])
        self.assertEqual(cGen.getSql(), " (  \n t1.k = t2.k \n ) ")
        self.assertEqual([c.args for c in sd.getTable.call_args_list], [("T1",), ("T2",)])

    @unittest.expectedFailure
    def testExplicitJoinDuplicatingKeyJoin(self) -> None:
        """BUG: when an explicit join equals an automatic key join, addKeyAttributeEquiJoinConditions() drops the
        duplicate JOIN_CONDITION but keeps its LOG_OP/GROUPING entries, producing an empty '( )' group (invalid SQL).
        """
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.addJoinCondition(("TA", "ID"), "EQ", ("TB", "ID"))
        self.assertNotIn("(  \n )", cGen.getSql())

    @unittest.expectedFailure
    def testValueListCondition(self) -> None:
        """BUG: VALUE_LIST_CONDITION rendering is broken: 'IN' is not in the operator table (KeyError) and
        the value list is never joined (`vS = ",".join` is not called) so the bound method repr would be emitted.
        """
        cGen = MyDbConditionSqlGen(self.__sd)
        cGen.set([("VALUE_LIST_CONDITION", ("TA", "NAME"), "EQ", (["a", "b"], "char"))])
        self.assertIn("'a','b'", cGen.getSql())


if __name__ == "__main__":
    unittest.main()
