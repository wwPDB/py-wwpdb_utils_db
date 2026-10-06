##
# File:    FileActivityDbMockTests.py
# Date:    2026-10-06
#
# Updates:
#
##
"""
Mock based test cases for FileActivityDb - the database core, site configuration and
path reconstruction are mocked so no database server or site configuration is required.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import os
import shutil
import tempfile
import unittest
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import patch

from wwpdb.utils.db.FileActivityDb import FileActivityDb

MODNAME = "wwpdb.utils.db.FileActivityDb"
TABLE = "file_activity_log"


class FileActivityDbMockTests(unittest.TestCase):
    """Tests for FileActivityDb with FileActivityDbCore, PathInfo and ConfigInfoAppCommon mocked."""

    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmpdir, True)

        corePatcher = patch(MODNAME + ".FileActivityDbCore")
        self.mockCoreCls = corePatcher.start()
        self.addCleanup(corePatcher.stop)
        self.core = self.mockCoreCls.return_value
        self.core.getTableName.return_value = TABLE
        self.core.executeSelectQuery.return_value = []
        self.core.executeUpdateQuery.return_value = True

        piPatcher = patch(MODNAME + ".PathInfo")
        self.mockPiCls = piPatcher.start()
        self.addCleanup(piPatcher.stop)
        self.pathInfo = self.mockPiCls.return_value

        cfgPatcher = patch(MODNAME + ".ConfigInfoAppCommon")
        self.mockCfgCls = cfgPatcher.start()
        self.addCleanup(cfgPatcher.stop)
        self.mockCfgCls.return_value.get_file_activity_db_support.return_value = True

        self.log = StringIO()
        self.db = FileActivityDb(siteId="SITE_X", verbose=True, log=self.log)

    # ---------------------------------------------------------------- helpers
    def setTracking(self, enabled: Any) -> None:
        self.mockCfgCls.return_value.get_file_activity_db_support.return_value = enabled

    def makeFile(self, dirPath: str, name: str, mtime: float = 1700000000.0) -> str:
        os.makedirs(dirPath, exist_ok=True)
        path = os.path.join(dirPath, name)
        with open(path, "w") as ofh:
            ofh.write("x")
        os.utime(path, (mtime, mtime))
        return path

    def insertParams(self) -> List[Tuple[Any, ...]]:
        """Parameter tuples of all INSERT statements issued."""
        out: List[Tuple[Any, ...]] = []
        for c in self.core.executeUpdateQuery.call_args_list:
            if "INSERT INTO" in c.args[0]:
                out.append(c.args[1])
        return out

    def lastSelect(self) -> Tuple[str, Optional[Tuple[Any, ...]]]:
        args = self.core.executeSelectQuery.call_args_list[0].args
        return args[0], (args[1] if len(args) > 1 else None)

    # ---------------------------------------------------------- construction
    def testConstruction(self) -> None:
        """Dependencies are constructed with the site id; default site id comes from getSiteId."""
        self.mockCoreCls.assert_called_once_with(siteId="SITE_X", verbose=True, log=self.log)
        self.mockPiCls.assert_called_once_with(siteId="SITE_X", verbose=True, log=self.log)
        with patch(MODNAME + ".getSiteId", return_value="SITE_DEFAULT") as mockGs:
            FileActivityDb()
        mockGs.assert_called_once_with()
        self.assertEqual(self.mockCoreCls.call_args.kwargs["siteId"], "SITE_DEFAULT")

    def testContextManagerAndClose(self) -> None:
        """Context manager returns self and closes the core on exit."""
        with self.db as db:
            self.assertIs(db, self.db)
            self.core.close.assert_not_called()
        self.core.close.assert_called_once_with()
        self.db.close()
        self.assertEqual(self.core.close.call_count, 2)

    # ------------------------------------------------------------- tracking
    def testIsTrackingEnabled(self) -> None:
        """Tracking flag is taken from ConfigInfoAppCommon."""
        self.assertTrue(self.db.isTrackingEnabled())
        self.mockCfgCls.assert_called_with("SITE_X")
        for val in [False, None, ""]:
            with self.subTest(val=val):
                self.setTracking(val)
                self.assertFalse(self.db.isTrackingEnabled())

    def testIsTrackingEnabledError(self) -> None:
        """Configuration errors disable tracking with a warning."""
        self.mockCfgCls.side_effect = RuntimeError("cfg broken")
        with self.assertLogs(MODNAME, level="WARNING") as cm:
            self.assertFalse(self.db.isTrackingEnabled())
        self.assertIn("cfg broken", cm.output[0])

    # ---------------------------------------------------------- logActivity
    def testLogActivityDisabled(self) -> None:
        """Disabled tracking is treated as success and touches nothing."""
        self.setTracking(False)
        self.assertTrue(self.db.logActivity("/x/D_1000000001_model_P1.cif.V1"))
        self.core.connection.assert_not_called()

    def testLogActivityTransient(self) -> None:
        """session and wf-instance files are skipped."""
        for st in ["session", "wf-instance"]:
            with self.subTest(st=st):
                self.assertTrue(self.db.logActivity("/x/D_1000000001_model_P1.cif.V1", storage_type=st))
        self.core.connection.assert_not_called()
        self.assertIn("Skipping transient directory file", self.log.getvalue())

    def testLogActivitySuccess(self) -> None:
        """A valid file is inserted using the file's modification time."""
        mtime = 1700000000.0
        path = self.makeFile(self.tmpdir, "D_1000000001_model_P1.cif.V2", mtime)
        self.assertTrue(self.db.logActivity(path, storage_type="deposit"))
        expDate = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")  # noqa: DTZ006
        self.assertEqual(self.insertParams(), [("D_1000000001", "model", "pdbx", 1, 2, "deposit", expDate)])
        sql, params = self.lastSelect()
        self.assertIn("SELECT version_number, created_date FROM %s" % TABLE, sql)
        self.assertEqual(params, ("D_1000000001", "model", "pdbx", 1, "deposit"))
        self.assertIn("Successfully logged", self.log.getvalue())

    def testLogActivityInvalidName(self) -> None:
        """A badly named file is not logged."""
        self.assertFalse(self.db.logActivity("/x/notonedep.txt", timestamp=datetime(2024, 1, 1)))
        self.core.executeUpdateQuery.assert_not_called()
        self.assertIn("Failed to log", self.log.getvalue())

    def testLogActivityException(self) -> None:
        """Unexpected errors while adding the record are caught and reported."""
        with patch(MODNAME + ".FileMetadataParser") as mockParserCls:
            mockParserCls.return_value.parseFilePath.side_effect = RuntimeError("parse exploded")
            db = FileActivityDb(siteId="SITE_X", verbose=True, log=self.log)
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertFalse(db.logActivity("/x/D_1000000001_model_P1.cif.V1"))
        self.assertIn("parse exploded", cm.output[0])
        self.assertIn("Error logging file", self.log.getvalue())

    # -------------------------------------------------------- addFileRecord
    def testAddFileRecordDisabled(self) -> None:
        self.setTracking(False)
        self.assertTrue(self.db.addFileRecord("/x/D_1000000001_model_P1.cif.V1"))
        self.core.connection.assert_not_called()

    def testAddFileRecordVersionLogic(self) -> None:
        """Only newer versions than those stored are written."""
        ts = datetime(2024, 5, 6, 7, 8, 9)
        cases = [([], True), ([(1, "2024-01-01 00:00:00")], True), ([(3, "2024-01-01 00:00:00")], False), ([(5, "2024-01-01 00:00:00")], False)]
        for existing, expectInsert in cases:
            with self.subTest(existing=existing):
                self.core.executeUpdateQuery.reset_mock()
                self.core.executeSelectQuery.return_value = existing
                self.assertTrue(self.db.addFileRecord("/a/D_1000000001_model_P1.cif.V3", timestamp=ts))
                if expectInsert:
                    self.assertEqual(self.insertParams(), [("D_1000000001", "model", "pdbx", 1, 3, "archive", "2024-05-06 07:08:09")])
                    self.assertIn("ON DUPLICATE KEY UPDATE", self.core.executeUpdateQuery.call_args.args[0])
                else:
                    self.core.executeUpdateQuery.assert_not_called()

    def testAddFileRecordNoVersion(self) -> None:
        """Files without a version suffix are stored as version 1."""
        self.assertTrue(self.db.addFileRecord("/a/D_1000000001_model_P1.cif", timestamp=datetime(2024, 1, 1)))
        self.assertEqual(self.insertParams()[0][4], 1)

    def testAddFileRecordInvalid(self) -> None:
        self.assertFalse(self.db.addFileRecord("/a/junk.txt", timestamp=datetime(2024, 1, 1)))
        self.core.connection.assert_not_called()

    def testAddFileRecordDbError(self) -> None:
        """Database connection errors give False."""
        self.core.connection.side_effect = RuntimeError("db down")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertFalse(self.db.addFileRecord("/a/D_1000000001_model_P1.cif.V1", timestamp=datetime(2024, 1, 1)))
        self.assertIn("db down", cm.output[0])

    # ------------------------------------------------- populateFromDirectory
    def testPopulateDisabled(self) -> None:
        self.setTracking(False)
        with patch("sys.stdout", new=StringIO()) as out, self.assertLogs(MODNAME, level="WARNING"):
            self.db.populateFromDirectory(self.tmpdir)
        self.assertIn("File activity tracking is disabled", out.getvalue())
        self.core.connection.assert_not_called()

    def testPopulateInvalidDirectory(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(ValueError):
            self.db.populateFromDirectory(os.path.join(self.tmpdir, "missing"))

    def testPopulateArchive(self) -> None:
        """Only the latest version per key is loaded; junk and top level files are ignored."""
        d1 = os.path.join(self.tmpdir, "D_1000000001")
        d2 = os.path.join(self.tmpdir, "D_1000000002")
        for v in (1, 3, 2):
            self.makeFile(d1, "D_1000000001_model_P1.cif.V%d" % v)
        self.makeFile(d1, "D_1000000001_validation-report_P1.pdf.V1")
        self.makeFile(d1, "junk.txt")
        self.makeFile(d1, "D_123_type_PX.cif.V1")
        os.makedirs(os.path.join(d1, "nested"))
        self.makeFile(d2, "D_1000000002_model_P1.cif")
        self.makeFile(self.tmpdir, "D_1000000009_model_P1.cif.V1")  # top level files are not scanned

        with self.assertLogs(MODNAME, level="DEBUG") as cm:
            self.db.populateFromDirectory(self.tmpdir)

        got = sorted((p[0], p[1], p[2], p[3], p[4], p[5]) for p in self.insertParams())
        self.assertEqual(
            got,
            [
                ("D_1000000001", "model", "pdbx", 1, 3, "archive"),
                ("D_1000000001", "validation-report", "pdf", 1, 1, "archive"),
                ("D_1000000002", "model", "pdbx", 1, 1, "archive"),
            ],
        )
        self.assertTrue(any("Skipping unrecognized file" in m for m in cm.output))
        self.assertTrue(any("Successfully loaded" in m for m in cm.output))

    def testPopulateDepositDirectory(self) -> None:
        """A directory path containing /deposit/ is recorded as deposit storage."""
        top = os.path.join(self.tmpdir, "deposit") + os.sep
        self.makeFile(os.path.join(top, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
        self.db.populateFromDirectory(top)
        self.assertEqual([p[5] for p in self.insertParams()], ["deposit"])

    def testPopulateIgnoredDirectory(self) -> None:
        """Ignored storage types (default session and wf-instance) are skipped."""
        for name in ["session", "wf-instance"]:
            with self.subTest(name=name):
                top = os.path.join(self.tmpdir, name) + os.sep
                self.makeFile(os.path.join(top, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
                self.db.populateFromDirectory(top)
                self.core.executeUpdateQuery.assert_not_called()
                self.assertIn("Skipping %s directory" % name, self.log.getvalue())

    def testPopulateIgnoredExplicitEmpty(self) -> None:
        """An empty ignore list processes session directories."""
        top = os.path.join(self.tmpdir, "session") + os.sep
        self.makeFile(os.path.join(top, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
        self.db.populateFromDirectory(top, ignore_storage_types=[])
        self.assertEqual([p[5] for p in self.insertParams()], ["session"])

    def testPopulateIgnoredSubdirectory(self) -> None:
        """Subdirectories classified as ignored storage types are skipped."""
        top = os.path.join(self.tmpdir, "session")  # No trailing separator - only the subdirectory path matches
        self.makeFile(os.path.join(top, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
        self.db.populateFromDirectory(top)
        self.core.executeUpdateQuery.assert_not_called()
        self.assertIn("Skipping session subdirectory", self.log.getvalue())

    @unittest.expectedFailure
    def testPopulateDepositNoTrailingSeparator(self) -> None:
        """BUG: subdirectories are classified as deposit but records are stored with the parent's (archive) storage type."""
        top = os.path.join(self.tmpdir, "deposit")
        self.makeFile(os.path.join(top, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
        self.db.populateFromDirectory(top)
        self.assertEqual([p[5] for p in self.insertParams()], ["deposit"])

    def testPopulateDbError(self) -> None:
        """Database errors are logged and propagated."""
        self.makeFile(os.path.join(self.tmpdir, "D_1000000001"), "D_1000000001_model_P1.cif.V1")
        self.core.executeSelectQuery.side_effect = RuntimeError("select failed")
        with self.assertLogs(MODNAME, level="ERROR") as cm, self.assertRaises(RuntimeError):
            self.db.populateFromDirectory(self.tmpdir)
        self.assertIn("select failed", cm.output[-1])

    # ------------------------------------------------------- getFileActivity
    def testGetFileActivityDisabled(self) -> None:
        self.setTracking(False)
        with patch("sys.stdout", new=StringIO()) as out, self.assertLogs(MODNAME, level="WARNING"):
            self.assertEqual(self.db.getFileActivity(), [])
        self.assertIn("NOTE", out.getvalue())
        self.core.executeSelectQuery.assert_not_called()

    def testGetFileActivityTimeRange(self) -> None:
        """hours take precedence over days; default is 24 hours."""
        cases: List[Tuple[Dict[str, Any], int]] = [({}, 24), ({"days": 2}, 48), ({"hours": 5}, 5), ({"hours": 5, "days": 3}, 5)]
        for kwargs, hours in cases:
            with self.subTest(kwargs=kwargs):
                self.core.executeSelectQuery.reset_mock()
                self.db.getFileActivity(**kwargs)
                sql, params = self.lastSelect()
                self.assertIn("FROM %s WHERE created_date >= DATE_SUB(NOW(), INTERVAL %%s HOUR)" % TABLE, sql)
                self.assertEqual(params, (hours,))

    def testGetFileActivityFilters(self) -> None:
        """Deposition, type, format and storage filters build parameterised SQL."""
        fcases: List[Tuple[Dict[str, Any], str, Tuple[Any, ...]]] = [
            ({"deposition_ids": "all"}, "", (24,)),
            ({"deposition_ids": "D_1000000001"}, "AND deposition_id = %s", (24, "D_1000000001")),
            (
                {"deposition_ids": "D_1000000001-D_1000000010"},
                "AND CAST(SUBSTRING(deposition_id, 3) AS UNSIGNED) BETWEEN %s AND %s",
                (24, 1000000001, 1000000010),
            ),
            ({"deposition_ids": "D_1, D_2"}, "AND deposition_id IN (%s, %s)", (24, "D_1", "D_2")),
            ({"file_types": "model, validation-report"}, "AND (content_type = %s OR content_type = %s)", (24, "model", "validation-report")),
            (
                {"formats": "cif,XML,json,pdf"},
                "AND (format_type = %s OR format_type = %s OR format_type = %s OR format_type = %s)",
                (24, "pdbx", "xml", "json", "pdf"),
            ),
            ({"storage_types": "archive, deposit"}, "AND (storage_type = %s OR storage_type = %s)", (24, "archive", "deposit")),
        ]
        for kwargs, frag, params in fcases:
            with self.subTest(kwargs=kwargs):
                self.core.executeSelectQuery.reset_mock()
                self.assertEqual(self.db.getFileActivity(**kwargs), [])
                sql, got = self.lastSelect()
                self.assertIn(frag, sql)
                self.assertEqual(got, params)

    def testGetFileActivityBadRange(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(ValueError):
            self.db.getFileActivity(deposition_ids="D_abc-D_def")
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(ValueError):
            self.db.getFileActivity(deposition_ids="D_1-D_2-D_3")
        self.core.executeSelectQuery.assert_not_called()

    def testGetFileActivityResults(self) -> None:
        """Rows are converted to file paths via PathInfo; unresolved paths are dropped."""
        self.core.executeSelectQuery.return_value = [
            ("D_1000000001", "model", "pdbx", 1, 3, "archive"),
            ("D_1000000002", "bogus", "pdbx", 2, 1, "deposit"),
        ]
        self.pathInfo.getFilePath.side_effect = ["/archive/D_1000000001/D_1000000001_model_P1.cif.V3", None]
        self.assertEqual(self.db.getFileActivity(hours=1), ["/archive/D_1000000001/D_1000000001_model_P1.cif.V3"])
        self.assertEqual(
            self.pathInfo.getFilePath.call_args_list[0].kwargs,
            {"dataSetId": "D_1000000001", "contentType": "model", "formatType": "pdbx", "fileSource": "archive", "versionId": "3", "partNumber": "1"},
        )
        self.assertEqual(self.pathInfo.getFilePath.call_args_list[1].kwargs["fileSource"], "deposit")

    def testGetFileActivityEmptyTable(self) -> None:
        """With verbose and no results an empty table is reported."""
        self.core.executeSelectQuery.side_effect = [[], [(0,)]]
        with self.assertLogs(MODNAME, level="INFO") as cm:
            self.assertEqual(self.db.getFileActivity(), [])
        self.assertTrue(any("exists but is empty" in m for m in cm.output))
        self.assertEqual(self.core.executeSelectQuery.call_args_list[1].args, ("SELECT COUNT(*) FROM %s" % TABLE,))

    def testGetFileActivityError(self) -> None:
        self.core.executeSelectQuery.return_value = [("D_1", "model", "pdbx", 1, 1, "archive")]
        self.pathInfo.getFilePath.side_effect = RuntimeError("pi fail")
        with self.assertLogs(MODNAME, level="ERROR") as cm, self.assertRaises(RuntimeError):
            self.db.getFileActivity()
        self.assertIn("pi fail", cm.output[0])

    # ----------------------------------------------------------------- purge
    def testPurgeAllData(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(ValueError):
            self.db.purgeAllData()
        self.core.executeUpdateQuery.assert_not_called()
        self.db.purgeAllData(confirmed=True)
        self.core.executeUpdateQuery.assert_called_once_with("TRUNCATE TABLE %s" % TABLE)

    def testPurgeAllDataError(self) -> None:
        self.core.executeUpdateQuery.side_effect = RuntimeError("truncate fail")
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(RuntimeError):
            self.db.purgeAllData(confirmed=True)

    def testPurgeDataSetData(self) -> None:
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(ValueError):
            self.db.purgeDataSetData("D_1000000001")
        self.core.executeSelectQuery.return_value = [(4,)]
        with self.assertLogs(MODNAME, level="INFO") as cm:
            self.db.purgeDataSetData("D_1000000001", confirmed=True)
        self.assertIn("purged 4 records for deposition ID: D_1000000001", cm.output[-1])
        sql, params = self.core.executeUpdateQuery.call_args.args
        self.assertIn("DELETE FROM %s" % TABLE, sql)
        self.assertEqual(params, ("D_1000000001",))

    def testPurgeDataSetDataNoCount(self) -> None:
        with self.assertLogs(MODNAME, level="INFO") as cm:
            self.db.purgeDataSetData("D_1000000001", confirmed=True)
        self.assertIn("purged 0 records", cm.output[-1])

    def testPurgeDataSetDataError(self) -> None:
        self.core.executeUpdateQuery.side_effect = RuntimeError("delete fail")
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(RuntimeError):
            self.db.purgeDataSetData("D_1000000001", confirmed=True)

    # ------------------------------------------------------- displayActivity
    def testDisplayDisabled(self) -> None:
        self.setTracking(False)
        with patch("sys.stdout", new=StringIO()) as out, self.assertLogs(MODNAME, level="WARNING"):
            self.db.displayActivity(hours=1)
        self.assertIn("NOTE", out.getvalue())
        self.core.executeSelectQuery.assert_not_called()

    def testDisplayNoResults(self) -> None:
        with patch("sys.stdout", new=StringIO()) as out, self.assertLogs(MODNAME, level="INFO") as cm:
            self.db.displayActivity(days=2)
        self.assertEqual(out.getvalue(), "")
        self.assertIn("No records found", cm.output[0])
        self.assertEqual(self.lastSelect()[1], (48,))

    def testDisplayResults(self) -> None:
        self.core.executeSelectQuery.return_value = [
            ("D_1", "model", "archive", "2024-01-01 00:00:00"),
            ("D_2", "sf", "deposit", datetime(2024, 1, 2, 3, 4, 5)),
        ]
        with patch("sys.stdout", new=StringIO()) as out:
            self.db.displayActivity()
        self.assertEqual(
            out.getvalue().splitlines(),
            ["dep_id,file_type,storage_type,last_timestamp", "D_1,model,archive,2024-01-01 00:00:00", "D_2,sf,deposit,2024-01-02 03:04:05"],
        )
        self.assertEqual(self.lastSelect()[1], (24,))

    def testDisplayError(self) -> None:
        self.core.executeSelectQuery.side_effect = RuntimeError("display fail")
        with self.assertLogs(MODNAME, level="ERROR"), self.assertRaises(RuntimeError):
            self.db.displayActivity()

    # ----------------------------------------------------- getFileTimestamp
    def testGetFileTimestamp(self) -> None:
        self.core.executeSelectQuery.return_value = [(datetime(2024, 3, 4, 5, 6, 7),)]
        self.assertEqual(self.db.getFileTimestamp("/a/D_1000000001_model_P1.cif.V2", storage_type="deposit"), datetime(2024, 3, 4, 5, 6, 7))
        sql, params = self.lastSelect()
        self.assertIn("SELECT created_date FROM %s" % TABLE, sql)
        self.assertEqual(params, ("D_1000000001", "model", "pdbx", 1, "deposit"))

    def testGetFileTimestampNotFound(self) -> None:
        for rows in [[], [(None,)]]:
            with self.subTest(rows=rows):
                self.core.executeSelectQuery.return_value = rows
                self.assertIsNone(self.db.getFileTimestamp("/a/D_1000000001_model_P1.cif.V2"))

    def testGetFileTimestampInvalid(self) -> None:
        with self.assertLogs(MODNAME, level="WARNING"):
            self.assertIsNone(self.db.getFileTimestamp("/a/junk.txt"))
        self.core.executeSelectQuery.assert_not_called()

    def testGetFileTimestampError(self) -> None:
        self.core.executeSelectQuery.return_value = [("not a date",)]
        with self.assertLogs(MODNAME, level="ERROR"):
            self.assertIsNone(self.db.getFileTimestamp("/a/D_1000000001_model_P1.cif.V2"))

    # -------------------------------------------------- updateFileTimestamp
    def testUpdateFileTimestamp(self) -> None:
        self.core.executeSelectQuery.return_value = [(1,)]
        self.assertTrue(self.db.updateFileTimestamp("/a/D_1000000001_model_P1.cif.V2", datetime(2024, 1, 2, 3, 4, 5), storage_type="deposit"))
        sql, params = self.core.executeUpdateQuery.call_args.args
        self.assertIn("UPDATE %s" % TABLE, sql)
        self.assertEqual(params, ("2024-01-02 03:04:05", "D_1000000001", "model", "pdbx", 1, "deposit"))
        self.assertEqual(self.lastSelect()[1], ("D_1000000001", "model", "pdbx", 1, "deposit"))

    def testUpdateFileTimestampNoRecord(self) -> None:
        for rows in [[(0,)], []]:
            with self.subTest(rows=rows):
                self.core.executeSelectQuery.return_value = rows
                self.assertFalse(self.db.updateFileTimestamp("/a/D_1000000001_model_P1.cif.V2", datetime(2024, 1, 1)))

    def testUpdateFileTimestampDisabledOrInvalid(self) -> None:
        with self.assertLogs(MODNAME, level="WARNING"):
            self.assertFalse(self.db.updateFileTimestamp("/a/junk.txt", datetime(2024, 1, 1)))
        self.setTracking(False)
        self.assertTrue(self.db.updateFileTimestamp("/a/junk.txt", datetime(2024, 1, 1)))
        self.core.executeUpdateQuery.assert_not_called()

    def testUpdateFileTimestampError(self) -> None:
        self.core.executeUpdateQuery.side_effect = RuntimeError("upd fail")
        with self.assertLogs(MODNAME, level="ERROR") as cm:
            self.assertFalse(self.db.updateFileTimestamp("/a/D_1000000001_model_P1.cif.V2", datetime(2024, 1, 1)))
        self.assertIn("upd fail", cm.output[0])


if __name__ == "__main__":
    unittest.main()
