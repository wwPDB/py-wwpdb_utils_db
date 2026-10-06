##
# File:    FileActivityDbCoreMockTests.py
# Date:    2026-10-06
#
# Updates:
#
##
"""
Mock based test cases for FileActivityDbCore - no database server required.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import unittest
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

from wwpdb.utils.db.FileActivityDbCore import FileActivityDbCore

MODNAME = "wwpdb.utils.db.FileActivityDbCore"


def _configGet(values: Dict[str, Any]) -> Any:
    def _get(key: str, default: Optional[Any] = None) -> Any:
        return values.get(key, default)

    return _get


class FileActivityDbCoreMockTests(unittest.TestCase):
    """Tests for FileActivityDbCore with ConfigInfo and MyDbConnect mocked."""

    def setUp(self) -> None:
        self.cfgValues: Dict[str, Any] = {
            "SITE_FILE_ACTIVITY_DB_NAME": "fadb",
            "SITE_FILE_ACTIVITY_DB_HOST_NAME": "dbhost",
            "SITE_FILE_ACTIVITY_DB_NUMBER": "3306",
            "SITE_FILE_ACTIVITY_DB_SOCKET": "/tmp/sock",  # noqa: S108
            "SITE_FILE_ACTIVITY_DB_USER_NAME": "user",
            "SITE_FILE_ACTIVITY_DB_PASSWORD": "pw",
        }
        cfgPatcher = patch(MODNAME + ".ConfigInfo")
        self.mockConfigCls = cfgPatcher.start()
        self.addCleanup(cfgPatcher.stop)
        self.mockConfigCls.return_value.get.side_effect = _configGet(self.cfgValues)

        connPatcher = patch(MODNAME + ".MyDbConnect")
        self.mockConnectCls = connPatcher.start()
        self.addCleanup(connPatcher.stop)
        self.mockDbcon = MagicMock(name="dbcon")
        self.mockCursor = MagicMock(name="cursor")
        self.mockDbcon.cursor.return_value = self.mockCursor
        self.mockConnectCls.return_value.connect.return_value = self.mockDbcon

        self.log = MagicMock()
        self.core = FileActivityDbCore(siteId="SITE_X", verbose=True, log=self.log)

    def testTableNameDefault(self) -> None:
        """Table name defaults when not configured."""
        self.assertEqual(self.core.getTableName(), "file_activity_log")

    def testTableNameConfigured(self) -> None:
        """Table name comes from site configuration."""
        self.cfgValues["SITE_FILE_ACTIVITY_DB_TABLE_NAME"] = "custom_table"
        core = FileActivityDbCore()
        self.assertEqual(core.getTableName(), "custom_table")

    def testNoConnectionBeforeUse(self) -> None:
        """Construction does not connect; queries without a connection fail gracefully."""
        self.mockConnectCls.assert_not_called()
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.core.executeSelectQuery("SELECT 1"), [])
            self.assertFalse(self.core.executeUpdateQuery("DELETE FROM t"))
        self.assertEqual(len(cm.output), 2)
        self.assertTrue(all("not initialized" in m for m in cm.output))

    def testConnectionLifecycle(self) -> None:
        """connection() opens with configured parameters and closes on exit."""
        with self.core.connection():
            self.mockConnectCls.assert_called_once_with(
                dbServer="mysql",
                dbHost="dbhost",
                dbName="fadb",
                dbUser="user",
                dbPw="pw",
                dbPort="3306",
                dbSocket="/tmp/sock",  # noqa: S108
                verbose=True,
                log=self.log,
            )
            self.mockDbcon.close.assert_not_called()
        self.mockDbcon.close.assert_called_once_with()
        # After close the connection is gone
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.core.executeSelectQuery("SELECT 1"), [])

    def testNestedConnectionReused(self) -> None:
        """Nested connection() contexts reuse the outer connection and only the outer closes it."""
        with self.core.connection():
            with self.core.connection():
                pass
            self.mockDbcon.close.assert_not_called()
            self.assertEqual(self.mockConnectCls.call_count, 1)
        self.mockDbcon.close.assert_called_once_with()

    def testConnectionClosedOnException(self) -> None:
        """Connection is closed when the body raises."""
        with self.assertRaises(KeyError), self.core.connection():
            raise KeyError("x")  # noqa: EM101
        self.mockDbcon.close.assert_called_once_with()

    def testConnectFailureNoHandle(self) -> None:
        """A falsy connection handle raises an exception."""
        self.mockConnectCls.return_value.connect.return_value = None
        with self.assertLogs(MODNAME, level="ERROR") as cm, self.assertRaises(Exception) as ctx:  # noqa: PT027,SIM117
            with self.core.connection():
                pass
        self.assertIn("Failed to establish database connection", str(ctx.exception))
        self.assertIn("Unable to connect", cm.output[0])

    def testConnectFailureException(self) -> None:
        """An exception from MyDbConnect is logged and re-raised."""
        self.mockConnectCls.side_effect = RuntimeError("no server")
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(RuntimeError), self.core.connection():
            pass

    def testExecuteSelectWithParams(self) -> None:
        """SELECT with params passes them to the cursor and returns the rows."""
        self.mockCursor.fetchall.return_value = [(1, "a"), (2, "b")]
        with self.core.connection():
            rows = self.core.executeSelectQuery("SELECT a FROM t WHERE x = %s", ("v",))
        self.assertEqual(rows, [(1, "a"), (2, "b")])
        self.mockCursor.execute.assert_called_once_with("SELECT a FROM t WHERE x = %s", ("v",))
        self.mockCursor.close.assert_called_once_with()

    def testExecuteSelectNoParams(self) -> None:
        """SELECT without (or with empty) params calls execute with query only."""
        self.mockCursor.fetchall.return_value = []
        with self.core.connection():
            self.assertEqual(self.core.executeSelectQuery("SELECT 1"), [])
            self.assertEqual(self.core.executeSelectQuery("SELECT 2", ()), [])
        self.assertEqual([c.args for c in self.mockCursor.execute.call_args_list], [("SELECT 1",), ("SELECT 2",)])

    def testExecuteSelectError(self) -> None:
        """Errors in SELECT are logged, cursor closed and empty list returned."""
        self.mockCursor.execute.side_effect = RuntimeError("syntax")
        with self.core.connection(), self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.core.executeSelectQuery("BAD"), [])
        self.assertIn("syntax", cm.output[0])
        self.mockCursor.close.assert_called_once_with()

    def testExecuteSelectCursorError(self) -> None:
        """Error obtaining a cursor returns an empty list."""
        self.mockDbcon.cursor.side_effect = RuntimeError("gone")
        with self.core.connection(), self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.core.executeSelectQuery("SELECT 1"), [])
        self.mockCursor.close.assert_not_called()

    def testExecuteUpdate(self) -> None:
        """UPDATE executes, commits and closes the cursor."""
        with self.core.connection():
            self.assertTrue(self.core.executeUpdateQuery("DELETE FROM t WHERE a = %s", ["x"]))
            self.assertTrue(self.core.executeUpdateQuery("TRUNCATE TABLE t"))
        self.assertEqual(self.mockCursor.execute.call_args_list[0].args, ("DELETE FROM t WHERE a = %s", ["x"]))
        self.assertEqual(self.mockCursor.execute.call_args_list[1].args, ("TRUNCATE TABLE t",))
        self.assertEqual(self.mockDbcon.commit.call_count, 2)
        self.assertEqual(self.mockCursor.close.call_count, 2)
        self.mockDbcon.rollback.assert_not_called()

    def testExecuteUpdateError(self) -> None:
        """Errors in UPDATE roll back and return False."""
        self.mockCursor.execute.side_effect = RuntimeError("dup")
        with self.core.connection(), self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertFalse(self.core.executeUpdateQuery("INSERT", {"a": 1}))
        self.assertIn("dup", cm.output[0])
        self.mockDbcon.rollback.assert_called_once_with()
        self.mockDbcon.commit.assert_not_called()
        self.mockCursor.close.assert_called_once_with()

    def testExecuteUpdateCursorError(self) -> None:
        """Failure to get a cursor in UPDATE rolls back without closing a cursor."""
        self.mockDbcon.cursor.side_effect = RuntimeError("gone")
        with self.core.connection(), self.assertLogs(MODNAME, level="ERROR"):
            self.assertFalse(self.core.executeUpdateQuery("INSERT"))
        self.mockDbcon.rollback.assert_called_once_with()
        self.mockCursor.close.assert_not_called()

    def testCloseErrorLogged(self) -> None:
        """An error when closing is logged as a warning and the connection discarded."""
        self.mockDbcon.close.side_effect = RuntimeError("close failed")
        with self.assertLogs(MODNAME, level="WARNING") as cm, self.core.connection():
            pass
        self.assertIn("close failed", cm.output[0])
        # A subsequent connection reconnects
        self.mockDbcon.close.side_effect = None
        with self.core.connection():
            pass
        self.assertEqual(self.mockConnectCls.call_count, 2)

    def testCloseWhenNotOpen(self) -> None:
        """close() on an unopened core does nothing."""
        self.core.close()
        self.mockDbcon.close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
