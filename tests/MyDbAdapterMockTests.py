##
# File:    MyDbAdapterMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for MyDbAdapter (no MySQL server or site configuration required).

MyDbAdapter provides its interface to derived classes as protected methods,  so the tests
exercise it through a minimal derived adapter in the same way as application adapters do.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import io
import time
import unittest
from typing import Any, Dict, List, Optional, TextIO, Tuple
from unittest import mock

import MySQLdb

from wwpdb.utils.db.MyDbAdapter import MyDbAdapter
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase, SchemaDictType

_SCHEMA: SchemaDictType = {
    "T1": {
        "TABLE_ID": "T1",
        "TABLE_NAME": "t1",
        "TABLE_TYPE": "transactional",
        "ATTRIBUTES": {"ORD": "ord", "NAME": "name", "VAL": "val"},
        "ATTRIBUTE_INFO": {
            "ORD": {"SQL_TYPE": "INT UNSIGNED AUTO_INCREMENT", "WIDTH": 0, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
            "NAME": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": False, "ORDER": 2},
            "VAL": {"SQL_TYPE": "INT", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
        },
        "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ["ORD"]}},
    },
}

_CONFIG: Dict[str, Any] = {
    "SITE_DB_HOST_NAME": "cfghost",
    "SITE_DB_PORT_NUMBER": "3316",
    "SITE_DB_DATABASE_NAME": "cfgdb",
    "SITE_DB_USER_NAME": "cfguser",
    "SITE_DB_PASSWORD": "cfgpw",
    "SITE_DB_SERVER": "mysql",
    "SITE_DB_SOCKET": None,
}

_LOGNAME = "wwpdb.utils.db.MyDbAdapter"


class ExampleAdapter(MyDbAdapter):
    """Minimal application adapter exposing the MyDbAdapter interface"""

    def __init__(self, verbose: bool = False, log: Optional[TextIO] = None) -> None:
        super().__init__(
            schemaDefObj=SchemaDefBase(databaseName="testdb", schemaDefDict=_SCHEMA), verbose=verbose, log=log if log is not None else io.StringIO()
        )
        self._setAttributeParameterMap("T1", self._getDefaultAttributeParameterMap("T1"))

    def setDebug(self, flag: bool = True) -> None:
        self._setDebug(flag)

    def setDataStore(self, name: str) -> None:
        self._setDataStore(name)

    def setDefaults(self, contextId: Optional[str], valueD: Dict[str, Any]) -> bool:
        ret: bool = self._setParameterDefaultValues(contextId, valueD)
        return ret

    def getDefaults(self, contextId: Optional[str]) -> Dict[str, Any]:
        ret: Dict[str, Any] = self._getParameterDefaultValues(contextId)
        return ret

    def setAttributeMap(self, tableId: str, mapL: List[Tuple[str, str]]) -> bool:
        ret: bool = self._setAttributeParameterMap(tableId, mapL)
        return ret

    def getAttributeMap(self, tableId: Optional[str]) -> List[Tuple[str, str]]:
        ret: List[Tuple[str, str]] = self._getAttributeParameterMap(tableId)
        return ret

    def getDefaultAttributeMap(self, tableId: str) -> List[Tuple[str, str]]:
        ret: List[Tuple[str, str]] = self._getDefaultAttributeParameterMap(tableId)
        return ret

    def setConstraintMap(self, tableId: str, mapL: List[Tuple[str, str]]) -> bool:
        ret: bool = self._setConstraintParameterMap(tableId, mapL)
        return ret

    def getConstraintMap(self, tableId: Optional[str]) -> List[Tuple[str, str]]:
        ret: List[Tuple[str, str]] = self._getConstraintParameterMap(tableId)
        return ret

    def open(self, **kwargs: Any) -> bool:
        ret: bool = self._open(**kwargs)
        return ret

    def close(self) -> None:
        self._close()

    def createSchema(self) -> bool:
        ret: bool = self._createSchema()
        return ret

    def secondsSinceEpoch(self) -> float:
        ret: float = self._getSecondsSinceEpoch()
        return ret

    def insert(self, tableId: str, contextId: Optional[str], **kwargs: Any) -> bool:
        ret: bool = self._insertRequest(tableId, contextId, **kwargs)
        return ret

    def update(self, tableId: str, contextId: Optional[str], **kwargs: Any) -> bool:
        ret: bool = self._updateRequest(tableId, contextId, **kwargs)
        return ret

    def select(self, tableId: str, **kwargs: Any) -> List[Dict[str, Any]]:
        ret: List[Dict[str, Any]] = self._select(tableId, **kwargs)
        return ret

    def delete(self, tableId: str, **kwargs: Any) -> bool:
        ret: bool = self._deleteRequest(tableId, **kwargs)
        return ret


class MyDbAdapterMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__siteP = mock.patch("wwpdb.utils.db.MyDbAdapter.getSiteId", return_value="TESTSITE")
        self.__mockGetSiteId = self.__siteP.start()
        self.__ciP = mock.patch("wwpdb.utils.db.MyDbAdapter.ConfigInfo")
        self.__mockConfigInfo = self.__ciP.start()
        self.__mockConfigInfo.return_value.get.side_effect = lambda key, default=None: _CONFIG.get(key, default)
        self.__gcP = mock.patch("wwpdb.utils.db.MyDbUtil.getConnection")
        self.__mockGetConnection = self.__gcP.start()
        self.__dbCon = mock.MagicMock(name="dbcon")
        self.__curs = self.__dbCon.cursor.return_value
        self.__mockGetConnection.return_value = self.__dbCon

    def tearDown(self) -> None:
        self.__gcP.stop()
        self.__ciP.stop()
        self.__siteP.stop()

    def __setRows(self, rows: List[Tuple[Any, ...]]) -> None:
        self.__curs.fetchone.side_effect = list(rows) + [None]  # noqa: RUF005

    def __executedSql(self) -> List[Any]:
        return [c[0] for c in self.__curs.execute.call_args_list]

    # ---- construction and parameter maps
    def testConstructorUsesSiteConfig(self) -> None:
        ExampleAdapter()
        self.__mockGetSiteId.assert_called_once_with(defaultSiteId="WWPDB_DEPLOY_MACOSX")
        self.__mockConfigInfo.assert_called_once_with("TESTSITE")

    def testDefaultAttributeParameterMap(self) -> None:
        ad = ExampleAdapter()
        # auto-increment attribute is skipped and names are camel cased
        self.assertEqual(ad.getDefaultAttributeMap("T1"), [("NAME", "name"), ("VAL", "val")])
        self.assertEqual(ad.getAttributeMap("T1"), [("NAME", "name"), ("VAL", "val")])

    def testAttributeAndConstraintMaps(self) -> None:
        ad = ExampleAdapter()
        self.assertEqual(ad.getAttributeMap("NOTABLE"), [])
        self.assertEqual(ad.getAttributeMap(None), [])
        self.assertEqual(ad.getConstraintMap("T1"), [])
        self.assertEqual(ad.getConstraintMap(None), [])
        self.assertTrue(ad.setAttributeMap("T1", [("NAME", "nm")]))
        self.assertEqual(ad.getAttributeMap("T1"), [("NAME", "nm")])
        self.assertTrue(ad.setConstraintMap("T1", [("NAME", "nm")]))
        self.assertEqual(ad.getConstraintMap("T1"), [("NAME", "nm")])

    def testParameterDefaults(self) -> None:
        ad = ExampleAdapter()
        self.assertEqual(ad.getDefaults("ctx"), {})
        self.assertEqual(ad.getDefaults(None), {})
        valueD: Dict[str, Any] = {"val": [1, 2]}
        self.assertTrue(ad.setDefaults("ctx", valueD))
        # a deep copy is stored
        valueD["val"].append(3)
        self.assertEqual(ad.getDefaults("ctx"), {"val": [1, 2]})
        self.assertEqual(ad.getDefaults("other"), {})

    def testSecondsSinceEpoch(self) -> None:
        ad = ExampleAdapter()
        t0 = time.time()
        t1 = ad.secondsSinceEpoch()
        self.assertIsInstance(t1, float)
        self.assertGreaterEqual(t1, t0)
        self.assertLessEqual(t1, time.time())

    # ---- open/close
    def testOpenFromConfiguration(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.open())
        self.__mockGetConnection.assert_called_once_with(
            {"db": "cfgdb", "user": "cfguser", "passwd": "cfgpw", "host": "cfghost", "port": 3316, "local_infile": 1}
        )
        ad.close()
        self.__dbCon.close.assert_called_once_with()
        # A second close is a no-op
        ad.close()
        self.__dbCon.close.assert_called_once_with()

    def testOpenExplicitArguments(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.open(dbServer="mysql", dbHost="h", dbName="d", dbUser="u", dbPw="p", dbSocket="/s/sock", dbPort=3999))
        self.__mockGetConnection.assert_called_once_with(
            {"db": "d", "user": "u", "passwd": "p", "host": "h", "port": 3999, "local_infile": 1, "unix_socket": "/s/sock"}
        )

    def testOpenFailure(self) -> None:
        self.__mockGetConnection.side_effect = MySQLdb.OperationalError(2002, "Can't connect")
        log = io.StringIO()
        ad = ExampleAdapter(log=log)
        self.assertFalse(ad.open())
        self.assertIn("Connection error", log.getvalue())

    # ---- createSchema
    def testCreateSchema(self) -> None:
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="INFO") as cm:
            self.assertTrue(ad.createSchema())
        sqlL = self.__executedSql()
        self.assertEqual(sqlL[0], ("USE testdb;",))
        self.assertEqual(sqlL[1], ("DROP TABLE IF EXISTS t1;",))
        self.assertTrue(sqlL[2][0].startswith("CREATE TABLE t1 ("))
        self.assertEqual(sqlL[3], ("CREATE UNIQUE INDEX p1 on t1 ( ord );",))
        self.__dbCon.commit.assert_called_once_with()
        # The connection opened for the operation is closed afterwards
        self.__dbCon.close.assert_called_once_with()
        self.assertIn("for tableId T1 server returns: True", "\n".join(cm.output))

    def testCreateSchemaUsesOpenConnection(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.open())
        self.assertTrue(ad.createSchema())
        self.__dbCon.close.assert_not_called()
        self.assertEqual(self.__mockGetConnection.call_count, 1)

    def testCreateSchemaDebug(self) -> None:
        ad = ExampleAdapter()
        ad.setDebug()
        with self.assertLogs(_LOGNAME, level="DEBUG") as cm:
            self.assertTrue(ad.createSchema())
        out = "\n".join(cm.output)
        self.assertIn("Starting _createSchema", out)
        self.assertIn("SQL: USE testdb;", out)
        self.assertIn("Completed at", out)

    def testCreateSchemaServerError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.OperationalError(1044, "Access denied")
        ad = ExampleAdapter()
        self.assertFalse(ad.createSchema())
        self.__dbCon.commit.assert_not_called()

    def testCreateSchemaConnectionFailure(self) -> None:
        """A failure to connect is logged and reported as failure"""
        self.__mockGetConnection.side_effect = MySQLdb.OperationalError(2002, "Can't connect")
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="ERROR") as cm:
            self.assertFalse(ad.createSchema())
        self.assertIn("table create error", cm.output[0])

    def testSetDataStore(self) -> None:
        ad = ExampleAdapter()
        ad.setDataStore("otherdb")
        self.assertTrue(ad.createSchema())
        self.assertEqual(self.__executedSql()[0], ("USE otherdb;",))

    # ---- insert
    def testInsertRequest(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.insert("T1", None, name="abc", val=5, ignored="x"))
        self.__curs.execute.assert_called_once_with("INSERT INTO testdb.t1 (name,val) VALUES (%s,%s);", ["abc", 5])
        self.__dbCon.commit.assert_called_once_with()
        self.__dbCon.close.assert_called_once_with()

    def testInsertRequestDefaultsAndNulls(self) -> None:
        """Unspecified values come from the context defaults or are set to the SQL null value"""
        ad = ExampleAdapter()
        ad.setDefaults("ctx", {"val": 42, "name": None})
        self.assertTrue(ad.insert("T1", "ctx", name=None))
        nullName = SchemaDefBase(databaseName="testdb", schemaDefDict=_SCHEMA).getTable("T1").getSqlNullValue("NAME")
        self.__curs.execute.assert_called_once_with("INSERT INTO testdb.t1 (name,val) VALUES (%s,%s);", [nullName, 42])
        self.__curs.execute.reset_mock()
        nullVal = SchemaDefBase(databaseName="testdb", schemaDefDict=_SCHEMA).getTable("T1").getSqlNullValue("VAL")
        self.assertTrue(ad.insert("T1", None, name="n"))
        self.__curs.execute.assert_called_once_with("INSERT INTO testdb.t1 (name,val) VALUES (%s,%s);", ["n", nullVal])

    def testInsertRequestDebug(self) -> None:
        ad = ExampleAdapter()
        ad.setDebug(True)
        with self.assertLogs(_LOGNAME, level="DEBUG") as cm:
            self.assertTrue(ad.insert("T1", None, name="abc", val=5))
        out = "\n".join(cm.output)
        self.assertIn("_insertRequest insert template sql=INSERT INTO testdb.t1", out)
        self.assertIn("_insertRequest completed", out)

    def testInsertRequestServerError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.IntegrityError(1062, "Duplicate entry")
        ad = ExampleAdapter()
        self.assertFalse(ad.insert("T1", None, name="abc", val=5))
        self.__dbCon.rollback.assert_called_once_with()

    def testInsertRequestUnknownTable(self) -> None:
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="ERROR") as cm:
            self.assertFalse(ad.insert("NOTABLE", None, name="abc"))
        self.assertIn("insert operation error", cm.output[0])
        self.__curs.execute.assert_not_called()

    # ---- update
    def testUpdateRequest(self) -> None:
        ad = ExampleAdapter()
        ad.setConstraintMap("T1", [("NAME", "name")])
        self.assertTrue(ad.update("T1", None, name="abc", val=7))
        self.__curs.execute.assert_called_once_with("UPDATE testdb.t1 SET  val=%s WHERE ( name=%s);", [7, "abc"])
        self.__dbCon.commit.assert_called_once_with()
        self.__dbCon.close.assert_called_once_with()

    def testUpdateRequestDefaults(self) -> None:
        """Unspecified update values are taken from the context defaults; unset values are not updated"""
        ad = ExampleAdapter()
        ad.setConstraintMap("T1", [("NAME", "name")])
        ad.setDefaults("ctx", {"val": 9})
        self.assertTrue(ad.update("T1", "ctx", name="abc"))
        self.__curs.execute.assert_called_once_with("UPDATE testdb.t1 SET  val=%s WHERE ( name=%s);", [9, "abc"])

    def testUpdateRequestDebug(self) -> None:
        ad = ExampleAdapter()
        ad.setConstraintMap("T1", [("NAME", "name")])
        ad.setDebug()
        with self.assertLogs(_LOGNAME, level="DEBUG") as cm:
            self.assertTrue(ad.update("T1", None, name="abc", val=7))
        out = "\n".join(cm.output)
        self.assertIn("update sql: UPDATE testdb.t1", out)
        self.assertIn("Completed _updateRequest", out)

    def testUpdateRequestUnknownTable(self) -> None:
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="ERROR") as cm:
            self.assertFalse(ad.update("NOTABLE", None, name="abc"))
        self.assertIn("update operation error", cm.output[0])

    # ---- select
    def testSelectWithConstraints(self) -> None:
        self.__setRows([(1, "abc", 5), (2, "abc", 6)])
        ad = ExampleAdapter()
        rL = ad.select("T1", name="abc", val=5, notAnAttribute="x")
        self.assertEqual(rL, [{"ORD": 1, "NAME": "abc", "VAL": 5}, {"ORD": 2, "NAME": "abc", "VAL": 6}])
        sql = self.__curs.execute.call_args[0][0]
        self.assertTrue(sql.startswith("SELECT t1.ord,t1.name,t1.val"))
        self.assertIn("FROM testdb.t1", sql)
        self.assertIn("t1.name = 'abc'", sql)
        self.assertIn("t1.val = 5", sql)
        self.assertNotIn("ORDER BY", sql)
        self.__dbCon.close.assert_called_once_with()

    def testSelectAllOrderedByKey(self) -> None:
        self.__setRows([(1, "a", None)])
        ad = ExampleAdapter()
        self.assertEqual(ad.select("T1"), [{"ORD": 1, "NAME": "a", "VAL": None}])
        sql = self.__curs.execute.call_args[0][0]
        self.assertNotIn("WHERE", sql)
        self.assertIn("ORDER BY t1.ord", sql)

    def testSelectDebug(self) -> None:
        self.__setRows([(1, "a", 2)])
        ad = ExampleAdapter()
        ad.setDebug()
        with self.assertLogs(_LOGNAME, level="DEBUG") as cm:
            self.assertEqual(len(ad.select("T1")), 1)
        out = "\n".join(cm.output)
        self.assertIn("_select selection sql: SELECT", out)
        self.assertIn("_select result set row 0", out)
        self.assertIn("Completed _select", out)

    def testSelectServerError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.ProgrammingError(1146, "Table doesn't exist")
        ad = ExampleAdapter()
        self.assertEqual(ad.select("T1"), [])

    def testSelectUnknownTable(self) -> None:
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="ERROR") as cm:
            self.assertEqual(ad.select("NOTABLE"), [])
        self.assertIn("_select  operation error", cm.output[0])

    # ---- delete
    def testDeleteRequest(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.delete("T1", name="abc", val=None))
        self.__curs.execute.assert_called_once_with("DELETE FROM  testdb.t1 WHERE ( name=%s);", ["abc"])
        self.__dbCon.commit.assert_called_once_with()
        self.__dbCon.close.assert_called_once_with()

    def testDeleteRequestUsesOpenConnection(self) -> None:
        ad = ExampleAdapter()
        self.assertTrue(ad.open())
        self.assertTrue(ad.delete("T1", name="abc"))
        self.assertTrue(ad.delete("T1", val=3))
        self.assertEqual(self.__mockGetConnection.call_count, 1)
        self.__dbCon.close.assert_not_called()
        ad.close()
        self.__dbCon.close.assert_called_once_with()

    def testDeleteRequestDebug(self) -> None:
        ad = ExampleAdapter()
        ad.setDebug()
        with self.assertLogs(_LOGNAME, level="DEBUG") as cm:
            self.assertTrue(ad.delete("T1", name="abc"))
        out = "\n".join(cm.output)
        self.assertIn("_deleteRequest delete sql: DELETE FROM  testdb.t1", out)
        self.assertIn("Completed _deleteRequest", out)

    def testDeleteRequestServerError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.OperationalError(1205, "Lock wait timeout")
        ad = ExampleAdapter()
        self.assertFalse(ad.delete("T1", name="abc"))
        self.__dbCon.rollback.assert_called_once_with()

    def testDeleteRequestUnknownTable(self) -> None:
        ad = ExampleAdapter(verbose=True)
        with self.assertLogs(_LOGNAME, level="ERROR") as cm:
            self.assertFalse(ad.delete("NOTABLE", name="abc"))
        self.assertIn("delete operation error", cm.output[0])


if __name__ == "__main__":
    unittest.main()
