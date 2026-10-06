##
# File:    FileActivityUtilMockTests.py
# Date:    2026-10-06
#
# Updates:
#
##
"""
Mock based test cases for FileActivityUtil and its command line interface main().
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import logging
import os
import shutil
import tempfile
import unittest
from io import StringIO
from typing import List
from unittest.mock import MagicMock, patch

from wwpdb.utils.db.FileActivityDb import FileActivityDb
from wwpdb.utils.db.FileActivityUtil import FileActivityUtil, main

MODNAME = "wwpdb.utils.db.FileActivityUtil"


class _RootLoggerGuard(unittest.TestCase):
    """Restores root logger state which FileActivityUtil modifies."""

    def setUp(self) -> None:
        root = logging.getLogger()
        level = root.level
        handlers = list(root.handlers)

        def _restore() -> None:
            root.setLevel(level)
            root.handlers[:] = handlers

        self.addCleanup(_restore)
        errPatcher = patch("sys.stderr", new=StringIO())
        self.stderr = errPatcher.start()
        self.addCleanup(errPatcher.stop)


class FileActivityUtilMockTests(_RootLoggerGuard):
    """Tests of FileActivityUtil methods with an injected mock database."""

    def setUp(self) -> None:
        super().setUp()
        self.db = MagicMock(spec=FileActivityDb)
        self.db.getFileActivity.return_value = []
        self.util = FileActivityUtil(db=self.db)

    def testDefaultDbCreated(self) -> None:
        with patch(MODNAME + ".FileActivityDb") as mockCls:
            util = FileActivityUtil(verbose=True)
        mockCls.assert_called_once_with(verbose=True)
        self.assertIs(util.db, mockCls.return_value)
        self.assertIs(self.util.db, self.db)

    def testPurgeAllData(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.util.purgeAllData([]), 1)
        self.db.purgeAllData.assert_not_called()
        self.assertEqual(self.util.purgeAllData("--confirmed"), 0)
        self.db.purgeAllData.assert_called_once_with(confirmed=True)

    def testPurgeAllDataBadArgs(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.purgeAllData("--confirmed --bogus"), 1)
        self.assertIn("Argument parsing error", cm.output[0])
        self.db.purgeAllData.assert_not_called()

    def testPurgeAllDataError(self) -> None:
        self.db.purgeAllData.side_effect = RuntimeError("purge boom")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.purgeAllData(["--confirmed"]), 1)
        self.assertIn("purge boom", cm.output[0])

    def testPurgeDataSetData(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.util.purgeDataSetData("--deposition-id D_1"), 1)
        self.assertEqual(self.util.purgeDataSetData("--deposition-id D_1 --confirmed"), 0)
        self.db.purgeDataSetData.assert_called_once_with(deposition_id="D_1", confirmed=True)

    def testPurgeDataSetDataErrors(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.purgeDataSetData(["--confirmed"]), 1)
        self.assertIn("Argument parsing error", cm.output[0])
        self.db.purgeDataSetData.side_effect = RuntimeError("ds boom")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.purgeDataSetData(["--deposition-id", "D_1", "--confirmed"]), 1)
        self.assertIn("ds boom", cm.output[0])

    def testDisplayActivity(self) -> None:
        self.assertEqual(self.util.displayActivity("--hours 3"), 0)
        self.db.displayActivity.assert_called_with(hours=3, days=None)
        self.assertEqual(self.util.displayActivity(["--days", "2"]), 0)
        self.db.displayActivity.assert_called_with(hours=None, days=2)

    def testDisplayActivityErrors(self) -> None:
        for args in ["", "--hours 1 --days 1", "--hours x"]:
            with self.subTest(args=args), self.assertLogs(MODNAME, level="ERROR"):
                self.assertEqual(self.util.displayActivity(args), 1)
        self.db.displayActivity.assert_not_called()
        self.db.displayActivity.side_effect = RuntimeError("disp boom")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.displayActivity("--hours 1"), 1)
        self.assertIn("disp boom", cm.output[0])

    def testPopulateFromDirectory(self) -> None:
        self.assertEqual(self.util.populateFromDirectory("--load-dir /some/dir"), 0)
        self.db.populateFromDirectory.assert_called_with("/some/dir", ignore_storage_types=["session", "wf-instance"])
        self.assertEqual(self.util.populateFromDirectory(["--load-dir", "/d", "--ignore-storage-types", " session, ,deposit "]), 0)
        self.db.populateFromDirectory.assert_called_with("/d", ignore_storage_types=["session", "deposit"])

    @unittest.expectedFailure
    def testPopulateFromDirectoryEmptyIgnore(self) -> None:
        """BUG: documented 'empty string processes all types' passes None, which FileActivityDb maps to the default ignore list."""
        self.assertEqual(self.util.populateFromDirectory(["--load-dir", "/d", "--ignore-storage-types", ""]), 0)
        self.db.populateFromDirectory.assert_called_with("/d", ignore_storage_types=[])

    def testPopulateFromDirectoryErrors(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.util.populateFromDirectory([]), 1)
        self.db.populateFromDirectory.side_effect = ValueError("Invalid directory")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.populateFromDirectory("--load-dir /nope"), 1)
        self.assertIn("Invalid directory", cm.output[0])

    def testGetActivity(self) -> None:
        self.db.getFileActivity.return_value = ["/a/f1", "/a/f2"]
        with patch("sys.stdout", new=StringIO()) as out:
            self.assertEqual(self.util.getActivity("--days 2 --deposition-ids D_1,D_2 --file-types model"), 0)
        self.assertEqual(out.getvalue(), "/a/f1\n/a/f2\n")
        self.db.getFileActivity.assert_called_once_with(hours=None, days=2, deposition_ids="D_1,D_2", file_types="model", formats="ALL", storage_types="ALL")

    def testGetActivityNoResults(self) -> None:
        with patch("sys.stdout", new=StringIO()) as out:
            self.assertEqual(
                self.util.getActivity(["--hours", "1", "--deposition-ids", "ALL", "--file-types", "ALL", "--formats", "cif", "--storage-types", "deposit"]),
                0,
            )
        self.assertEqual(out.getvalue(), "")
        self.db.getFileActivity.assert_called_once_with(hours=1, days=None, deposition_ids="ALL", file_types="ALL", formats="cif", storage_types="deposit")

    def testGetActivityErrors(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.util.getActivity("--hours 1 --deposition-ids ALL"), 1)
        self.db.getFileActivity.side_effect = ValueError("Invalid deposition-ids range")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertEqual(self.util.getActivity("--hours 1 --deposition-ids x-y --file-types ALL"), 1)
        self.assertIn("Invalid deposition-ids range", cm.output[0])

    def testVerboseCreatesNewDb(self) -> None:
        """-v replaces a non-verbose database with a verbose one exactly once."""
        with patch(MODNAME + ".FileActivityDb") as mockCls:
            self.assertEqual(self.util.displayActivity("--hours 1 -v"), 0)
            self.assertEqual(self.util.displayActivity("--hours 2 --verbose"), 0)
        mockCls.assert_called_once_with(verbose=True)
        self.assertIs(self.util.db, mockCls.return_value)
        self.db.displayActivity.assert_not_called()
        self.assertEqual(mockCls.return_value.displayActivity.call_count, 2)
        self.assertEqual(logging.getLogger().level, logging.DEBUG)

    def testVerboseAlreadyVerbose(self) -> None:
        """An already verbose utility keeps its database."""
        util = FileActivityUtil(db=self.db, verbose=True)
        with patch(MODNAME + ".FileActivityDb") as mockCls:
            self.assertEqual(util.purgeAllData("--confirmed -v"), 0)
        mockCls.assert_not_called()
        self.db.purgeAllData.assert_called_once_with(confirmed=True)


class FileActivityUtilMainTests(_RootLoggerGuard):
    """Tests of the command line entry point with FileActivityDb mocked."""

    def setUp(self) -> None:
        super().setUp()
        dbPatcher = patch(MODNAME + ".FileActivityDb")
        self.mockDbCls = dbPatcher.start()
        self.addCleanup(dbPatcher.stop)
        self.db = self.mockDbCls.return_value
        self.db.getFileActivity.return_value = ["/x/y"]
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, True)
        self.stdout = ""

    def runMain(self, argv: List[str]) -> int:
        with patch("sys.argv", ["file-activity", *argv]), patch("sys.stdout", new=StringIO()) as out:
            ret = main()
        self.stdout = out.getvalue()
        return ret

    def testNoCommand(self) -> None:
        self.assertEqual(self.runMain([]), 1)
        self.assertIn("File Activity Database Utility", self.stdout)
        self.mockDbCls.assert_not_called()

    def testBadArgsExit(self) -> None:
        with self.assertRaises(SystemExit):
            self.runMain(["display"])

    def testDisplay(self) -> None:
        self.assertEqual(self.runMain(["display", "--hours", "5"]), 0)
        self.mockDbCls.assert_called_once_with(verbose=False)
        self.db.displayActivity.assert_called_once_with(hours=5, days=None)

    def testDisplayDaysVerbose(self) -> None:
        self.assertEqual(self.runMain(["display", "--days", "7", "-v"]), 0)
        self.mockDbCls.assert_called_once_with(verbose=True)
        self.db.displayActivity.assert_called_once_with(hours=None, days=7)
        self.assertEqual(logging.getLogger().level, logging.DEBUG)

    @unittest.expectedFailure
    def testDisplayZeroHours(self) -> None:
        """BUG: '--hours 0' is falsy and is converted to '--days None', which fails argument parsing."""
        self.assertEqual(self.runMain(["display", "--hours", "0"]), 0)

    def testQuery(self) -> None:
        self.assertEqual(self.runMain(["query", "--hours", "6", "--deposition-ids", "D_1-D_9", "--file-types", "model", "--formats", "cif"]), 0)
        self.db.getFileActivity.assert_called_once_with(hours=6, days=None, deposition_ids="D_1-D_9", file_types="model", formats="cif", storage_types="ALL")
        self.assertEqual(self.stdout, "/x/y\n")

    def testQueryDaysVerbose(self) -> None:
        self.assertEqual(self.runMain(["query", "--days", "2", "--deposition-ids", "ALL", "--file-types", "ALL", "--storage-types", "archive", "-v"]), 0)
        self.db.getFileActivity.assert_called_once_with(hours=None, days=2, deposition_ids="ALL", file_types="ALL", formats="ALL", storage_types="archive")

    def testLoad(self) -> None:
        self.assertEqual(self.runMain(["load", "--load-dir", self.tmpdir, "--ignore-storage-types", "session", "-v"]), 0)
        self.db.populateFromDirectory.assert_called_once_with(self.tmpdir, ignore_storage_types=["session"])

    def testLoadDefaultIgnore(self) -> None:
        self.assertEqual(self.runMain(["load", "--load-dir", self.tmpdir]), 0)
        self.db.populateFromDirectory.assert_called_once_with(self.tmpdir, ignore_storage_types=["session", "wf-instance"])

    @unittest.expectedFailure
    def testLoadDirWithSpace(self) -> None:
        """BUG: main() rebuilds arguments as a string which is split on whitespace, breaking paths containing spaces."""
        d = os.path.join(self.tmpdir, "a b")
        os.makedirs(d)
        self.assertEqual(self.runMain(["load", "--load-dir", d]), 0)
        self.db.populateFromDirectory.assert_called_once_with(d, ignore_storage_types=["session", "wf-instance"])

    def testPurge(self) -> None:
        self.assertEqual(self.runMain(["purge", "--confirmed", "-v"]), 0)
        self.db.purgeAllData.assert_called_once_with(confirmed=True)

    def testPurgeFailure(self) -> None:
        self.db.purgeAllData.side_effect = RuntimeError("x")
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertEqual(self.runMain(["purge", "--confirmed"]), 1)

    def testPurgeDataset(self) -> None:
        self.assertEqual(self.runMain(["purge-dataset", "--deposition-id", "D_8000210018", "--confirmed", "-v"]), 0)
        self.db.purgeDataSetData.assert_called_once_with(deposition_id="D_8000210018", confirmed=True)


if __name__ == "__main__":
    unittest.main()
