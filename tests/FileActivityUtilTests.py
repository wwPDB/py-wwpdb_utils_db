##
# File:    FileActivityUtilTests.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#   - Adjusted tests to account for the new argument pre-checks and error handling in FileActivityUtil.
##
"""
Test cases for FileActivityUtil module - tests utility methods for database operations.
"""
__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import os
import sys
import unittest
from unittest.mock import patch
from io import StringIO
from typing import Optional, Union, List, Dict

from wwpdb.utils.db.FileActivityUtil import FileActivityUtil
from wwpdb.utils.testing.Features import Features

class DummyFileActivityDb:
    """Mock FileActivityDb for testing"""
    def __init__(self) -> None:
        self.purged: bool = False
        self.loadedDirectory: Optional[str] = None
        self.displayCalled: bool = False
        self.queryParams: Dict[str, Optional[Union[int, str]]] = {}

    def purgeFileActivityDb(self, confirmed: bool = False) -> None:
        self.purged = confirmed

    def populateFileActivityDb(self, directory: str) -> None:
        self.loadedDirectory = directory

    def displayFileActivityDb(self, hours: Optional[int] = None, days: Optional[int] = None, site_id: Optional[str] = None) -> None:
        self.displayCalled = True
        print("Dummy display output")

    def getFileActivity(self, hours: Optional[int] = None, days: Optional[int] = None, site_id: Optional[str] = None,
                       deposition_ids: str = "ALL", file_types: str = "ALL", formats: str = "ALL") -> List[str]:
        self.queryParams = {
            "hours": hours,
            "days": days,
            "site_id": site_id,
            "deposition_ids": deposition_ids,
            "file_types": file_types,
            "formats": formats
        }
        return ["dummy/file/path"]

class FileActivityUtilTests(unittest.TestCase):
    """Test cases for FileActivityUtil class"""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Create a dummy DB instance
        self.dummy_db = DummyFileActivityDb()
        # Initialize FileActivityUtil with the dummy DB
        self.util = FileActivityUtil(db=self.dummy_db)
        self.testDir = os.path.join(os.path.dirname(__file__), "test-output")
        if not os.path.exists(self.testDir):
            os.makedirs(self.testDir)

    def testPurgeWithoutConfirmation(self) -> None:
        """Test purge command without confirmation flag"""
        with patch('sys.stderr', new=StringIO()) as fake_err:
            ret = self.util.purgeFileActivityDb([])
            self.assertEqual(ret, 1)
            self.assertIn("Must provide --confirmed flag to purge database", fake_err.getvalue())
            self.assertFalse(self.dummy_db.purged)

    def testPurgeWithCancel(self) -> None:
        """Test purge command with cancellation simulation (production ignores cancellation)"""
        # Even if input returns 'n', production does not cancel purge since --confirmed is provided
        with patch('builtins.input', return_value='n'), patch('sys.stdout', new=StringIO()):
            ret = self.util.purgeFileActivityDb(['--confirmed'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.dummy_db.purged)

    def testPurgeWithConfirm(self) -> None:
        """Test purge command with confirmation"""
        with patch('builtins.input', return_value='y'):
            ret = self.util.purgeFileActivityDb(['--confirmed'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.dummy_db.purged)

    def testDisplayWithHours(self) -> None:
        """Test display command with hours parameter"""
        with patch('sys.stdout', new=StringIO()) as fake_out:
            ret = self.util.displayFileActivityDb(['--hours', '24'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.dummy_db.displayCalled)
            self.assertIn("Dummy display output", fake_out.getvalue())

    def testDisplayWithDays(self) -> None:
        """Test display command with days parameter"""
        with patch('sys.stdout', new=StringIO()) as fake_out:
            ret = self.util.displayFileActivityDb(['--days', '7'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.dummy_db.displayCalled)

    def testLoadFileActivityDb(self) -> None:
        """Test load command with directory parameter"""
        testDir = os.path.join(self.testDir, "test_load")
        if not os.path.exists(testDir):
            os.makedirs(testDir)
        ret = self.util.loadFileActivityDb(['--load-dir', testDir])
        self.assertEqual(ret, 0)
        self.assertEqual(self.dummy_db.loadedDirectory, testDir)

    def testQueryFileActivity(self) -> None:
        """Test query command with various parameters"""
        args = [
            '--hours', '24',
            '--deposition-ids', 'D_1000000000',
            '--file-types', 'model',
            '--formats', 'pdbx'
        ]
        with patch('sys.stdout', new=StringIO()) as fake_out:
            ret = self.util.queryFileActivity(args)
            self.assertEqual(ret, 0)
            self.assertEqual(self.dummy_db.queryParams['hours'], 24)
            self.assertEqual(self.dummy_db.queryParams['deposition_ids'], 'D_1000000000')
            self.assertIn("dummy/file/path", fake_out.getvalue())

def suiteUtilTests() -> unittest.TestSuite:
    suite = unittest.TestSuite()
    suite.addTest(FileActivityUtilTests("testPurgeWithoutConfirmation"))
    suite.addTest(FileActivityUtilTests("testPurgeWithCancel"))
    suite.addTest(FileActivityUtilTests("testPurgeWithConfirm"))
    suite.addTest(FileActivityUtilTests("testDisplayWithHours"))
    suite.addTest(FileActivityUtilTests("testDisplayWithDays"))
    suite.addTest(FileActivityUtilTests("testLoadFileActivityDb"))
    suite.addTest(FileActivityUtilTests("testQueryFileActivity"))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suiteUtilTests())
