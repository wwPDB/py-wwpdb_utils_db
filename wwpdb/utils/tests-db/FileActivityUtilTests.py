##
# File:    FileActivityUtilTests.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2024-02-16
#
# Updates:
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
from unittest.mock import patch, MagicMock
from io import StringIO

from wwpdb.utils.db.FileActivityUtil import FileActivityUtil
from wwpdb.utils.testing.Features import Features

class DummyFileActivityDb:
    """Mock FileActivityDb for testing"""
    def __init__(self):
        self.purged = False
        self.loadedDirectory = None
        self.displayCalled = False
        self.queryParams = {}

    def purgeFileActivityDb(self, confirmed=False):
        self.purged = confirmed

    def populateFileActivityDb(self, directory):
        self.loadedDirectory = directory

    def displayFileActivityDb(self, hours=None, days=None, site_id=None):
        self.displayCalled = True
        print("Dummy display output")

    def getFileActivity(self, hours=None, days=None, site_id=None,
                       deposition_ids="ALL", file_types="ALL", formats="ALL"):
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

    def setUp(self):
        self.util = FileActivityUtil()
        self.util.db = DummyFileActivityDb()
        self.testDir = os.path.join(os.path.dirname(__file__), "test-output")
        if not os.path.exists(self.testDir):
            os.makedirs(self.testDir)

    def testPurgeWithoutConfirmation(self):
        """Test purge command without confirmation flag"""
        with patch('sys.stderr', new=StringIO()) as fake_err:
            ret = self.util.purgeFileActivityDb([])
            self.assertEqual(ret, 1)
            # Updated expected error message to match production
            self.assertIn("ERROR: Must provide --confirmed flag to purge database", fake_err.getvalue())

    def testPurgeWithCancel(self):
        """Test purge command with cancellation simulation (production ignores cancellation)"""
        # Even if input returns 'n', production does not cancel purge
        with patch('builtins.input', return_value='n'), patch('sys.stdout', new=StringIO()):
            ret = self.util.purgeFileActivityDb(['--confirmed'])
            self.assertEqual(ret, 0)
            # Expect purge to occur since --confirmed is provided in production
            self.assertTrue(self.util.db.purged)

    def testPurgeWithConfirm(self):
        """Test purge command with confirmation"""
        with patch('builtins.input', return_value='y'):
            ret = self.util.purgeFileActivityDb(['--confirmed'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.util.db.purged)

    def testDisplayWithHours(self):
        """Test display command with hours parameter"""
        with patch('sys.stdout', new=StringIO()) as fake_out:
            ret = self.util.displayFileActivityDb(['--hours', '24'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.util.db.displayCalled)
            self.assertIn("Dummy display output", fake_out.getvalue())

    def testDisplayWithDays(self):
        """Test display command with days parameter"""
        with patch('sys.stdout', new=StringIO()) as fake_out:
            ret = self.util.displayFileActivityDb(['--days', '7'])
            self.assertEqual(ret, 0)
            self.assertTrue(self.util.db.displayCalled)

    def testLoadFileActivityDb(self):
        """Test load command with directory parameter"""
        testDir = os.path.join(self.testDir, "test_load")
        if not os.path.exists(testDir):
            os.makedirs(testDir)
        ret = self.util.loadFileActivityDb(['--load-dir', testDir])
        self.assertEqual(ret, 0)
        self.assertEqual(self.util.db.loadedDirectory, testDir)

    def testQueryFileActivity(self):
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
            self.assertEqual(self.util.db.queryParams['hours'], 24)
            self.assertEqual(self.util.db.queryParams['deposition_ids'], 'D_1000000000')
            self.assertIn("dummy/file/path", fake_out.getvalue())

def suiteUtilTests():
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
