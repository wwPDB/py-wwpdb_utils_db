##
# File:    FileMetadataParserTests.py
# Date:    2026-10-06
#
# Updates:
#
##
"""
Test cases for FileMetadataParser - parsing of OneDep file names into metadata records.
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
from unittest.mock import MagicMock, patch

from wwpdb.utils.db.FileMetadataParser import FileMetadataParser


class FileMetadataParserTests(unittest.TestCase):
    """Tests for FileMetadataParser using the real PathInfo file name splitter."""

    def setUp(self) -> None:
        self.__tmpdir = tempfile.mkdtemp()
        self.__parser = FileMetadataParser(siteId="WWPDB_DEPLOY_TEST")

    def tearDown(self) -> None:
        shutil.rmtree(self.__tmpdir, ignore_errors=True)

    def __makeFile(self, name: str, mtime: float = 1700000000.0) -> str:
        path = os.path.join(self.__tmpdir, name)
        with open(path, "w") as ofh:
            ofh.write("data")
        os.utime(path, (mtime, mtime))
        return path

    def testParseWithExplicitTimestamp(self) -> None:
        """Explicit timestamp is used, the file need not exist."""
        ts = datetime(2024, 2, 18, 10, 11, 12)
        rec = self.__parser.parseFilePath("/nonexistent/D_1000000001_model_P1.cif.V3", storage_type="deposit", timestamp=ts)
        self.assertEqual(
            rec,
            {
                "deposition_id": "D_1000000001",
                "content_type": "model",
                "format_type": "pdbx",
                "part_number": 1,
                "version_number": 3,
                "storage_type": "deposit",
                "created_date": "2024-02-18 10:11:12",
            },
        )

    def testParseUsesFileModificationTime(self) -> None:
        """Without a timestamp the file mtime is used and storage type defaults to archive."""
        mtime = 1700000000.0
        path = self.__makeFile("D_1000000002_validation-report_P2.pdf.V2", mtime)
        rec = self.__parser.parseFilePath(path)
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec["deposition_id"], "D_1000000002")
        self.assertEqual(rec["content_type"], "validation-report")
        self.assertEqual(rec["format_type"], "pdf")
        self.assertEqual(rec["part_number"], 2)
        self.assertEqual(rec["version_number"], 2)
        self.assertEqual(rec["storage_type"], "archive")
        self.assertEqual(rec["created_date"], datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S"))  # noqa: DTZ006

    def testParseMissingVersionDefaultsToOne(self) -> None:
        """A file name without a version suffix yields version 1."""
        rec = self.__parser.parseFilePath("D_1000000001_model_P1.cif", timestamp=datetime(2024, 1, 1))
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec["version_number"], 1)
        self.assertEqual(rec["created_date"], "2024-01-01 00:00:00")

    def testParseNoTimestampMissingFile(self) -> None:
        """Missing file and no timestamp returns None with a warning."""
        with self.assertLogs("wwpdb.utils.db.FileMetadataParser", level="WARNING") as cm:
            rec = self.__parser.parseFilePath(os.path.join(self.__tmpdir, "D_1000000001_model_P1.cif.V1"))
        self.assertIsNone(rec)
        self.assertIn("No timestamp available", cm.output[0])

    def testParseInvalidNames(self) -> None:
        """Names not following the OneDep convention return None."""
        for name in ["invalid_filename.txt", "D_123_type_PX.cif.V1", "D_123_type.cif.V1", "model_P1.cif.V1"]:
            with self.subTest(name=name):
                path = self.__makeFile(name)
                with self.assertLogs("wwpdb.utils.db.FileMetadataParser", level="WARNING") as cm:
                    self.assertIsNone(self.__parser.parseFilePath(path))
                self.assertTrue(any("naming convention" in msg for msg in cm.output))

    def testParseSplitterException(self) -> None:
        """An exception from PathInfo.splitFileName is caught and None returned."""
        mockPi = MagicMock()
        mockPi.splitFileName.side_effect = RuntimeError("boom")
        with patch("wwpdb.utils.db.FileMetadataParser.PathInfo", return_value=mockPi):
            parser = FileMetadataParser()
        with self.assertLogs("wwpdb.utils.db.FileMetadataParser", level="WARNING") as cm:
            rec = parser.parseFilePath("/a/D_1000000001_model_P1.cif.V1", timestamp=datetime(2024, 1, 1))
        self.assertIsNone(rec)
        self.assertIn("boom", cm.output[0])
        mockPi.splitFileName.assert_called_once_with("D_1000000001_model_P1.cif.V1")

    def testParseSplitterBadTuple(self) -> None:
        """A malformed tuple from the splitter (wrong arity) is handled."""
        mockPi = MagicMock()
        mockPi.splitFileName.return_value = ("D_1", "model")
        with patch("wwpdb.utils.db.FileMetadataParser.PathInfo", return_value=mockPi):
            parser = FileMetadataParser()
        with self.assertLogs("wwpdb.utils.db.FileMetadataParser", level="WARNING"):
            self.assertIsNone(parser.parseFilePath("x", timestamp=datetime(2024, 1, 1)))

    def testPathInfoConstructorArgs(self) -> None:
        """PathInfo is created with the site id, verbose flag and log handle."""
        log = MagicMock()
        with patch("wwpdb.utils.db.FileMetadataParser.PathInfo") as mockCls:
            FileMetadataParser(siteId="SITE_X", verbose=True, log=log)
        mockCls.assert_called_once_with(siteId="SITE_X", verbose=True, log=log)

    def testExtractFileKey(self) -> None:
        """extractFileKey returns the PathInfo tuple using only the base name."""
        self.assertEqual(self.__parser.extractFileKey("/some/dir/D_1000000001_model_P1.cif.V4"), ("D_1000000001", "model", "pdbx", 1, 4))
        self.assertEqual(self.__parser.extractFileKey("D_1000000001_model_P1.cif"), ("D_1000000001", "model", "pdbx", 1, None))
        self.assertEqual(self.__parser.extractFileKey("junk.txt"), (None, None, None, None, None))

    def testExtractFileKeyException(self) -> None:
        """An exception in the splitter yields a tuple of Nones."""
        mockPi = MagicMock()
        mockPi.splitFileName.side_effect = ValueError("bad")
        with patch("wwpdb.utils.db.FileMetadataParser.PathInfo", return_value=mockPi):
            parser = FileMetadataParser()
        with self.assertLogs("wwpdb.utils.db.FileMetadataParser", level="WARNING") as cm:
            key = parser.extractFileKey("/x/y/z.cif")
        self.assertEqual(key, (None, None, None, None, None))
        self.assertIn("Failed to extract file key", cm.output[0])
        mockPi.splitFileName.assert_called_once_with("z.cif")


if __name__ == "__main__":
    unittest.main()
