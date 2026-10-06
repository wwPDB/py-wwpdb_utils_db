##
# File:    MyDbUtilMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for MyDbConnect and MyDbQuery (no MySQL server required).
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import io
import os
import unittest
import warnings
from typing import Any, Dict, List, Optional, Sequence
from unittest import mock

import MySQLdb

from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery

_ENV_KEYS = ["MYSQL_DB_NAME", "MYSQL_DB_USER", "MYSQL_DB_PW", "MYSQL_DB_HOST", "MYSQL_DB_SOCKET", "MYSQL_DB_PORT"]


def _cleanEnv(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Return a copy of the environment without the MYSQL_DB_* settings plus any extra settings"""
    env = {k: v for k, v in os.environ.items() if k not in _ENV_KEYS}
    if extra:
        env.update(extra)
    return env


class MyDbConnectMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = io.StringIO()
        self.__gcP = mock.patch("wwpdb.utils.db.MyDbUtil.getConnection")
        self.__mockGetConnection = self.__gcP.start()
        self.__dbCon = mock.MagicMock(name="dbcon")
        self.__mockGetConnection.return_value = self.__dbCon
        self.__envP = mock.patch.dict(os.environ, _cleanEnv(), clear=True)
        self.__envP.start()

    def tearDown(self) -> None:
        self.__envP.stop()
        self.__gcP.stop()

    def __connectKw(self) -> Dict[str, Any]:
        kw: Dict[str, Any] = self.__mockGetConnection.call_args[0][0]
        return kw

    def testConnectExplicitArguments(self) -> None:
        myC = MyDbConnect(dbHost="dbhost", dbName="dbname", dbUser="dbuser", dbPw="dbpw", dbSocket="/tmp/my.sock", dbPort="3307", log=self.__lfh)
        self.assertIs(myC.connect(), self.__dbCon)
        self.__mockGetConnection.assert_called_once_with(
            {"db": "dbname", "user": "dbuser", "passwd": "dbpw", "host": "dbhost", "port": 3307, "local_infile": 1, "unix_socket": "/tmp/my.sock"}
        )

    def testConnectDefaults(self) -> None:
        """With no arguments or environment settings the host is localhost, port 3306 and no socket"""
        myC = MyDbConnect(log=self.__lfh)
        self.assertIs(myC.connect(), self.__dbCon)
        self.assertEqual(self.__connectKw(), {"db": "None", "user": "None", "passwd": "None", "host": "localhost", "port": 3306, "local_infile": 1})

    def testConnectEnvironment(self) -> None:
        """Unspecified arguments are taken from the environment"""
        env = {
            "MYSQL_DB_NAME": "envdb",
            "MYSQL_DB_USER": "envuser",
            "MYSQL_DB_PW": "envpw",
            "MYSQL_DB_HOST": "envhost",
            "MYSQL_DB_SOCKET": "/env/sock",
            "MYSQL_DB_PORT": "3333",
        }
        with mock.patch.dict(os.environ, env):
            myC = MyDbConnect(dbHost=None, log=self.__lfh)
        myC.connect()
        self.assertEqual(
            self.__connectKw(),
            {"db": "envdb", "user": "envuser", "passwd": "envpw", "host": "envhost", "port": 3333, "local_infile": 1, "unix_socket": "/env/sock"},
        )

    def testConnectArgumentsOverrideEnvironment(self) -> None:
        env = {"MYSQL_DB_NAME": "envdb", "MYSQL_DB_SOCKET": "/env/sock", "MYSQL_DB_PORT": "3333"}
        with mock.patch.dict(os.environ, env):
            myC = MyDbConnect(dbName="argdb", dbSocket="/arg/sock", dbPort=4444, log=self.__lfh)
        myC.connect()
        kw = self.__connectKw()
        self.assertEqual(kw["db"], "argdb")
        self.assertEqual(kw["unix_socket"], "/arg/sock")
        self.assertEqual(kw["port"], 4444)

    def testUnsupportedServer(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            MyDbConnect(dbServer="oracle", log=self.__lfh)
        self.assertEqual(cm.exception.code, 1)
        self.assertIn("Unsupported server oracle", self.__lfh.getvalue())

    def testSetAuth(self) -> None:
        myC = MyDbConnect(dbName="orig", log=self.__lfh)
        myC.setAuth({"DB_NAME": "adb", "DB_HOST": "ahost", "DB_USER": "auser", "DB_PW": "apw", "DB_SOCKET": None, "DB_SERVER": "mysql", "DB_PORT": 3390})
        myC.connect()
        self.assertEqual(self.__connectKw(), {"db": "adb", "user": "auser", "passwd": "apw", "host": "ahost", "port": 3390, "local_infile": 1})

    def testSetAuthDefaultPort(self) -> None:
        myC = MyDbConnect(dbPort=4000, log=self.__lfh)
        myC.setAuth({"DB_NAME": "adb", "DB_HOST": "ahost", "DB_USER": "auser", "DB_PW": "apw", "DB_SOCKET": "/a/sock", "DB_SERVER": "mysql"})
        myC.connect()
        kw = self.__connectKw()
        self.assertEqual(kw["port"], 3306)
        self.assertEqual(kw["unix_socket"], "/a/sock")

    def testSetAuthStringPort(self) -> None:
        myC = MyDbConnect(log=self.__lfh)
        myC.setAuth({"DB_NAME": "adb", "DB_HOST": "ahost", "DB_USER": "auser", "DB_PW": "apw", "DB_SOCKET": None, "DB_SERVER": "mysql", "DB_PORT": "3391"})
        myC.connect()
        self.assertEqual(self.__connectKw()["port"], 3391)

    def testSetAuthMissingKey(self) -> None:
        """An incomplete authentication dictionary is reported on the log"""
        myC = MyDbConnect(dbName="orig", dbUser="origuser", log=self.__lfh)
        myC.setAuth({"DB_NAME": "adb"})
        self.assertIn("+MyDbConnect.setAuth failing", self.__lfh.getvalue())
        self.assertIn("KeyError", self.__lfh.getvalue())
        myC.connect()
        kw = self.__connectKw()
        # Values preceding the missing key are applied
        self.assertEqual(kw["db"], "adb")
        self.assertEqual(kw["user"], "origuser")

    def testConnectFailure(self) -> None:
        self.__mockGetConnection.side_effect = MySQLdb.OperationalError(2002, "Can't connect")
        myC = MyDbConnect(dbHost="h", dbName="d", dbUser="u", dbPw="p", log=self.__lfh)
        self.assertIsNone(myC.connect())
        self.assertIn("+MyDbConnect.connect() Connection error to server mysql host h dsn d user u", self.__lfh.getvalue())
        self.assertFalse(myC.close())

    # Bug: setAuth() stores DB_PORT without integer conversion, so a string port causes the "%d" format in the
    # connect() error handler to raise TypeError instead of returning None.
    @unittest.expectedFailure
    def testConnectFailureStringPortFromAuth(self) -> None:
        self.__mockGetConnection.side_effect = MySQLdb.OperationalError(2002, "Can't connect")
        myC = MyDbConnect(log=self.__lfh)
        myC.setAuth({"DB_NAME": "adb", "DB_HOST": "ahost", "DB_USER": "auser", "DB_PW": "apw", "DB_SOCKET": None, "DB_SERVER": "mysql", "DB_PORT": "3391"})
        self.assertIsNone(myC.connect())

    def testReconnectClosesExisting(self) -> None:
        first = mock.MagicMock(name="first")
        second = mock.MagicMock(name="second")
        self.__mockGetConnection.side_effect = [first, second]
        myC = MyDbConnect(log=self.__lfh)
        self.assertIs(myC.connect(), first)
        self.assertIs(myC.connect(), second)
        first.close.assert_called_once_with()
        second.close.assert_not_called()
        self.assertIn("WARNING Closing an existing connection", self.__lfh.getvalue())

    def testClose(self) -> None:
        myC = MyDbConnect(log=self.__lfh)
        self.assertFalse(myC.close())
        myC.connect()
        self.assertTrue(myC.close())
        self.__dbCon.close.assert_called_once_with()
        self.assertFalse(myC.close())

    def testCloseError(self) -> None:
        """An exception closing the connection returns False"""
        self.__dbCon.close.side_effect = MySQLdb.OperationalError("gone")
        myC = MyDbConnect(log=self.__lfh)
        myC.connect()
        self.assertFalse(myC.close())
        # The connection is retained, so a further close is attempted
        self.assertFalse(myC.close())
        self.assertEqual(self.__dbCon.close.call_count, 2)


class MyDbQueryMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = io.StringIO()
        self.__dbCon = mock.MagicMock(name="dbcon")
        self.__curs = self.__dbCon.cursor.return_value
        # Isolate any changes made to the warnings filters by MyDbQuery
        self.__warnCtx = warnings.catch_warnings()
        self.__warnCtx.__enter__()  # pylint: disable=unnecessary-dunder-call

    def tearDown(self) -> None:
        self.__warnCtx.__exit__(None, None, None)

    def __query(self, verbose: bool = True) -> MyDbQuery:
        return MyDbQuery(dbcon=self.__dbCon, verbose=verbose, log=self.__lfh)

    def __setRows(self, rows: Sequence[Any]) -> None:
        self.__curs.fetchone.side_effect = list(rows) + [None]  # noqa: RUF005

    # ---- setWarning
    def testSetWarning(self) -> None:
        myQ = self.__query()
        for action in ["error", "ignore", "default"]:
            self.assertTrue(myQ.setWarning(action))
        self.assertFalse(myQ.setWarning("bogus"))

    def testWarningActionError(self) -> None:
        """With setWarning('error') a MySQL warning raised during execution fails the command"""
        self.__curs.execute.side_effect = lambda *args: warnings.warn("truncated", MySQLdb.Warning, stacklevel=1)
        myQ = self.__query()
        myQ.setWarning("error")
        self.assertFalse(myQ.sqlCommand(["UPDATE t SET a=1"]))
        self.assertIn("MySQL message is:\ntruncated", self.__lfh.getvalue())
        self.__dbCon.commit.assert_not_called()

    def testWarningActionIgnore(self) -> None:
        """With setWarning('ignore') MySQL warnings do not fail the command"""
        self.__curs.execute.side_effect = lambda *args: warnings.warn("truncated", MySQLdb.Warning, stacklevel=1)
        myQ = self.__query()
        self.assertTrue(myQ.setWarning("ignore"))
        self.assertTrue(myQ.sqlCommand(["UPDATE t SET a=1"]))
        self.__dbCon.commit.assert_called_once_with()

    def testWarningActionDefaultAfterInvalid(self) -> None:
        """An invalid warning action reverts to the default action - warnings are reported but do not fail"""
        self.__curs.execute.side_effect = lambda *args: warnings.warn("truncated", MySQLdb.Warning, stacklevel=1)
        myQ = self.__query()
        myQ.setWarning("error")
        self.assertFalse(myQ.setWarning("bogus"))
        with warnings.catch_warnings(record=True) as wL:
            self.assertTrue(myQ.sqlCommand(["UPDATE t SET a=1"]))
        self.assertTrue(any(issubclass(w.category, MySQLdb.Warning) for w in wL))

    # ---- sqlCommand
    def testSqlCommand(self) -> None:
        myQ = self.__query()
        self.assertTrue(myQ.sqlCommand(["CREATE TABLE a (x int)", "INSERT INTO a VALUES (1)"]))
        self.assertEqual(self.__curs.execute.call_args_list, [mock.call("CREATE TABLE a (x int)"), mock.call("INSERT INTO a VALUES (1)")])
        self.__dbCon.commit.assert_called_once_with()
        self.__curs.close.assert_called_once_with()
        self.assertEqual(self.__lfh.getvalue(), "")

    def testSqlCommandError(self) -> None:
        self.__curs.execute.side_effect = [None, MySQLdb.ProgrammingError(1064, "syntax error")]
        myQ = self.__query()
        self.assertFalse(myQ.sqlCommand(["SELECT 1", "SELEC 2"]))
        out = self.__lfh.getvalue()
        self.assertIn("SQL command failed for:\nSELEC 2", out)
        self.assertIn("syntax error", out)
        self.__dbCon.commit.assert_not_called()
        self.__dbCon.rollback.assert_not_called()
        self.__curs.close.assert_called_once_with()

    def testSqlCommandErrorQuiet(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.ProgrammingError(1064, "syntax error")
        myQ = self.__query(verbose=False)
        self.assertFalse(myQ.sqlCommand(["SELEC 2"]))
        self.assertEqual(self.__lfh.getvalue(), "")
        self.__curs.close.assert_called_once_with()

    def testSqlCommandWarning(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.Warning("data truncated")
        myQ = self.__query()
        self.assertFalse(myQ.sqlCommand(["INSERT INTO a VALUES (1)"]))
        out = self.__lfh.getvalue()
        self.assertIn("data truncated", out)
        self.assertIn("generated warnings for command:\nINSERT INTO a VALUES (1)", out)
        self.__curs.close.assert_called_once_with()

    def testSqlCommandOtherException(self) -> None:
        self.__curs.execute.side_effect = ValueError("unexpected")
        myQ = self.__query()
        self.assertFalse(myQ.sqlCommand(["DROP TABLE a"]))
        out = self.__lfh.getvalue()
        self.assertIn("SQL command failed for:\nDROP TABLE a", out)
        self.assertIn("ValueError: unexpected", out)
        self.__curs.close.assert_called_once_with()

    # Bug: if dbcon.cursor() raises, the exception handlers call curs.close() on an unbound local and
    # an UnboundLocalError escapes instead of returning False (same pattern in all MyDbQuery command methods).
    @unittest.expectedFailure
    def testSqlCommandCursorFailure(self) -> None:
        self.__dbCon.cursor.side_effect = MySQLdb.OperationalError(2006, "server has gone away")
        myQ = self.__query()
        self.assertFalse(myQ.sqlCommand(["SELECT 1"]))

    # ---- sqlTemplateCommand
    def testSqlTemplateCommand(self) -> None:
        myQ = self.__query()
        self.assertTrue(myQ.sqlTemplateCommand(sqlTemplate="INSERT INTO a (x,y) VALUES (%s,%s)", valueList=[1, "b"]))
        self.__curs.execute.assert_called_once_with("INSERT INTO a (x,y) VALUES (%s,%s)", [1, "b"])
        self.__dbCon.commit.assert_called_once_with()
        self.__curs.close.assert_called_once_with()

    def testSqlTemplateCommandDefaultValues(self) -> None:
        myQ = self.__query()
        self.assertTrue(myQ.sqlTemplateCommand(sqlTemplate="DELETE FROM a"))
        self.__curs.execute.assert_called_once_with("DELETE FROM a", [])

    def testSqlTemplateCommandError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.IntegrityError(1062, "Duplicate entry")
        myQ = self.__query()
        self.assertFalse(myQ.sqlTemplateCommand(sqlTemplate="INSERT INTO a (x) VALUES (%s)", valueList=["dup"]))
        out = self.__lfh.getvalue()
        self.assertIn("Duplicate entry", out)
        self.assertIn("SQL command failed for:\nINSERT INTO a (x) VALUES (dup)", out)
        self.__dbCon.rollback.assert_called_once_with()
        self.__dbCon.commit.assert_not_called()
        self.__curs.close.assert_called_once_with()

    def testSqlTemplateCommandWarning(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.Warning("truncated")
        myQ = self.__query()
        self.assertFalse(myQ.sqlTemplateCommand(sqlTemplate="INSERT INTO a (x) VALUES (%s)", valueList=["long"]))
        self.assertIn("generated warnings for command:\nINSERT INTO a (x) VALUES (long)", self.__lfh.getvalue())
        self.__dbCon.rollback.assert_called_once_with()

    def testSqlTemplateCommandOtherException(self) -> None:
        self.__curs.execute.side_effect = TypeError("bad value")
        myQ = self.__query()
        self.assertFalse(myQ.sqlTemplateCommand(sqlTemplate="INSERT INTO a (x) VALUES (%s)", valueList=[3]))
        out = self.__lfh.getvalue()
        self.assertIn("INSERT INTO a (x) VALUES (3)", out)
        self.assertIn("TypeError: bad value", out)
        self.__dbCon.rollback.assert_called_once_with()

    def testSqlTemplateCommandErrorQuiet(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.Error("boom")
        myQ = self.__query(verbose=False)
        self.assertFalse(myQ.sqlTemplateCommand(sqlTemplate="DELETE FROM a"))
        self.assertEqual(self.__lfh.getvalue(), "")
        self.__dbCon.rollback.assert_called_once_with()

    # ---- sqlBatchTemplateCommand
    def testSqlBatchTemplateCommand(self) -> None:
        myQ = self.__query()
        tvL = [("INSERT INTO a (x) VALUES (%s)", [1]), ("INSERT INTO a (x) VALUES (%s)", (2,))]
        self.assertTrue(myQ.sqlBatchTemplateCommand(tvL, prependSqlList=["DELETE FROM a WHERE x=1;", "DELETE FROM a WHERE x=2;"]))
        self.assertEqual(
            self.__curs.execute.call_args_list,
            [
                mock.call("DELETE FROM a WHERE x=1;\nDELETE FROM a WHERE x=2;"),
                mock.call("INSERT INTO a (x) VALUES (%s)", [1]),
                mock.call("INSERT INTO a (x) VALUES (%s)", (2,)),
            ],
        )
        self.__dbCon.commit.assert_called_once_with()
        self.__curs.close.assert_called_once_with()

    def testSqlBatchTemplateCommandNoPrepend(self) -> None:
        myQ = self.__query()
        self.assertTrue(myQ.sqlBatchTemplateCommand([("INSERT INTO a (x) VALUES (%s)", [1])], prependSqlList=[]))
        self.__curs.execute.assert_called_once_with("INSERT INTO a (x) VALUES (%s)", [1])
        self.assertTrue(myQ.sqlBatchTemplateCommand([]))
        self.assertEqual(self.__dbCon.commit.call_count, 2)

    def testSqlBatchTemplateCommandError(self) -> None:
        self.__curs.execute.side_effect = [None, MySQLdb.DataError(1406, "Data too long")]
        myQ = self.__query()
        tvL = [("INSERT INTO a (x) VALUES (%s)", ["ok"]), ("INSERT INTO a (x) VALUES (%s)", ["bad"])]
        self.assertFalse(myQ.sqlBatchTemplateCommand(tvL))
        out = self.__lfh.getvalue()
        self.assertIn("Data too long", out)
        self.assertIn("SQL command failed for:\nINSERT INTO a (x) VALUES (bad)", out)
        self.__dbCon.rollback.assert_called_once_with()
        self.__dbCon.commit.assert_not_called()
        self.__curs.close.assert_called_once_with()

    def testSqlBatchTemplateCommandPrependError(self) -> None:
        """A failure in the prepended SQL is reported against an empty template"""
        self.__curs.execute.side_effect = MySQLdb.ProgrammingError("bad delete")
        myQ = self.__query()
        self.assertFalse(myQ.sqlBatchTemplateCommand([("INSERT INTO a (x) VALUES (%s)", [1])], prependSqlList=["DELETE FRM a"]))
        self.assertEqual(self.__curs.execute.call_count, 1)
        self.__dbCon.rollback.assert_called_once_with()

    def testSqlBatchTemplateCommandWarning(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.Warning("truncated")
        myQ = self.__query()
        self.assertFalse(myQ.sqlBatchTemplateCommand([("INSERT INTO a (x) VALUES (%s)", ["w"])]))
        self.assertIn("generated warnings for command:\nINSERT INTO a (x) VALUES (w)", self.__lfh.getvalue())
        self.__dbCon.rollback.assert_called_once_with()

    def testSqlBatchTemplateCommandOtherException(self) -> None:
        self.__curs.execute.side_effect = KeyError("k")
        myQ = self.__query()
        self.assertFalse(myQ.sqlBatchTemplateCommand([("INSERT INTO a (x) VALUES (%s)", ["z"])]))
        out = self.__lfh.getvalue()
        self.assertIn("generated exception for command:\nINSERT INTO a (x) VALUES (z)", out)
        self.assertIn("KeyError", out)
        self.__dbCon.rollback.assert_called_once_with()

    def testSqlBatchTemplateCommandQuiet(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.Error("boom")
        myQ = self.__query(verbose=False)
        self.assertFalse(myQ.sqlBatchTemplateCommand([("INSERT INTO a (x) VALUES (%s)", ["z"])]))
        self.assertEqual(self.__lfh.getvalue(), "")

    # ---- sqlCommand2
    def testSqlCommand2(self) -> None:
        myQ = self.__query()
        self.assertTrue(myQ.sqlCommand2("SET @a=1"))
        self.__curs.execute.assert_called_once_with("SET @a=1")
        self.__curs.close.assert_called_once_with()
        self.__dbCon.commit.assert_not_called()

    def testSqlCommand2Errors(self) -> None:
        for exc, expectQuery in [
            (MySQLdb.ProgrammingError("prog error"), False),
            (MySQLdb.OperationalError("op error"), True),
            (MySQLdb.IntegrityError("other error"), True),
        ]:
            with self.subTest(exc=type(exc).__name__):
                self.__lfh.seek(0)
                self.__lfh.truncate()
                self.__curs.reset_mock()
                self.__curs.execute.side_effect = exc
                myQ = self.__query()
                self.assertEqual(myQ.sqlCommand2("BAD SQL"), [])
                out = self.__lfh.getvalue()
                self.assertIn(str(exc), out)
                self.assertEqual("SQL command failed for:\nBAD SQL" in out, expectQuery)
                self.__curs.close.assert_called_once_with()

    def testSqlCommand2OtherException(self) -> None:
        self.__curs.execute.side_effect = RuntimeError("other")
        myQ = self.__query()
        self.assertEqual(myQ.sqlCommand2("BAD SQL"), [])
        out = self.__lfh.getvalue()
        self.assertIn("SQL command failed for:\nBAD SQL", out)
        self.assertIn("RuntimeError: other", out)

    def testSqlCommand2WarningIsError(self) -> None:
        """Warnings issued during execution are treated as errors"""
        self.__curs.execute.side_effect = lambda *args: warnings.warn("note", UserWarning, stacklevel=1)
        myQ = self.__query(verbose=False)
        self.assertEqual(myQ.sqlCommand2("SET @a=1"), [])
        self.__curs.close.assert_called_once_with()

    # ---- selectRows
    def testSelectRows(self) -> None:
        self.__setRows([(1, "a"), (2, "b")])
        myQ = self.__query()
        self.assertEqual(myQ.selectRows("SELECT x,y FROM a"), [(1, "a"), (2, "b")])
        self.__curs.execute.assert_called_once_with("SELECT x,y FROM a")
        self.__curs.close.assert_called_once_with()

    def testSelectRowsEmpty(self) -> None:
        self.__setRows([])
        myQ = self.__query()
        self.assertEqual(myQ.selectRows("SELECT x FROM a"), [])

    def testSelectRowsErrors(self) -> None:
        for exc, expectQuery in [
            (MySQLdb.ProgrammingError("prog error"), False),
            (MySQLdb.OperationalError("op error"), True),
            (MySQLdb.InternalError("other error"), True),
            (RuntimeError("unexpected"), True),
        ]:
            with self.subTest(exc=type(exc).__name__):
                self.__lfh.seek(0)
                self.__lfh.truncate()
                self.__curs.reset_mock()
                self.__curs.execute.side_effect = exc
                myQ = self.__query()
                self.assertEqual(myQ.selectRows("SELECT bad"), [])
                out = self.__lfh.getvalue()
                self.assertEqual("SQL command failed for:\nSELECT bad" in out, expectQuery)
                self.__curs.close.assert_called_once_with()

    def testSelectRowsFetchError(self) -> None:
        """An error part way through fetching discards the partial result"""
        self.__curs.fetchone.side_effect = [(1,), MySQLdb.OperationalError("lost connection")]
        myQ = self.__query(verbose=False)
        self.assertEqual(myQ.selectRows("SELECT x FROM a"), [])
        self.assertEqual(self.__lfh.getvalue(), "")

    # ---- simpleQuery
    def testSimpleQuery(self) -> None:
        self.__setRows([("a", 1)])
        myQ = self.__query()
        rL = myQ.simpleQuery(selectList=["n", "v"], fromList=["t1", "t2"], condition=" WHERE t1.id=t2.id", orderList=[("v", "INTEGER"), ("n", "CHAR")])
        self.assertEqual(rL, [("a", 1)])
        expected = "SELECT n,v FROM t1,t2 WHERE t1.id=t2.id ORDER BY CAST(v AS INTEGER) , CAST(n AS CHAR) "
        self.__curs.execute.assert_called_once_with(expected)
        self.assertIn("Query: %s\n" % expected, self.__lfh.getvalue())
        self.__curs.close.assert_called_once_with()

    def testSimpleQueryAppendsToReturnObject(self) -> None:
        self.__setRows([(2,), (3,)])
        myQ = self.__query(verbose=False)
        returnObj: List[Any] = [(1,)]
        rL = myQ.simpleQuery(selectList=["x"], fromList=["a"], returnObj=returnObj)
        self.assertIs(rL, returnObj)
        self.assertEqual(rL, [(1,), (2,), (3,)])
        self.__curs.execute.assert_called_once_with("SELECT x FROM a")
        self.assertEqual(self.__lfh.getvalue(), "")

    def testSimpleQueryDefaults(self) -> None:
        self.__setRows([])
        myQ = self.__query(verbose=False)
        self.assertEqual(myQ.simpleQuery(), [])
        self.__curs.execute.assert_called_once_with("SELECT  FROM ")

    def testSimpleQueryErrorPropagates(self) -> None:
        """simpleQuery() does not catch database errors"""
        self.__curs.execute.side_effect = MySQLdb.ProgrammingError("bad")
        myQ = self.__query(verbose=False)
        with self.assertRaises(MySQLdb.ProgrammingError):
            myQ.simpleQuery(selectList=["x"], fromList=["a"])

    # ---- testSelectQuery
    def testTestSelectQuery(self) -> None:
        self.__setRows([(7,)])
        myQ = self.__query()
        self.assertTrue(myQ.testSelectQuery(7))
        self.__curs.execute.assert_called_once_with("select 7")

    def testTestSelectQueryMismatch(self) -> None:
        self.__setRows([(8,)])
        self.assertFalse(self.__query().testSelectQuery(7))

    def testTestSelectQueryNoRows(self) -> None:
        self.__setRows([])
        self.assertFalse(self.__query(verbose=False).testSelectQuery(7))

    def testTestSelectQueryError(self) -> None:
        self.__curs.execute.side_effect = MySQLdb.OperationalError("gone")
        self.assertFalse(self.__query(verbose=False).testSelectQuery(7))


if __name__ == "__main__":
    unittest.main()
