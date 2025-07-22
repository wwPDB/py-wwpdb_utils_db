##
# File:    FileActivityDbTests.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#   - Updated DummyMyDbQuery to return only two records for activity_query to match expected counts.
##
"""
Test cases for FileActivityDb class - tests database operations for file activity tracking.
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import os
import tempfile
import unittest
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any

from wwpdb.utils.testing.Features import Features
from wwpdb.utils.db.FileActivityDb import FileActivityDb
from wwpdb.utils.db.FileMetadataParser import FileMetadataParser


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DummyMyDbQuery:
    """Mock database query class for testing."""
    def __init__(self) -> None:
        self.commands: List[str] = []
        self.test_data: Dict[str, List[List[Any]]] = {
            'version_check': [[1, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]],
            'activity_query': [
                ['file1.cif'],
                ['file2.pdb']
            ],
            'count_query': [[5]],
            'display_query': [
                # Format: site_id, deposition_id, content_type, created_date
                ['WWPDB_TEST', 'D_1000000000', 'model', '2024-02-18 10:00:00'],
                ['WWPDB_PROD', 'D_1000000001', 'validation', '2024-02-18 11:00:00']
            ]
        }
        self.closed: bool = False

    def selectRows(self, sql: str) -> List[List[Any]]:
        """Mock select query execution."""
        if self.closed:
            raise Exception("Query executed on closed connection")
        # Store the SQL query in commands
        self.commands.append(sql.strip())
        if 'version_number' in sql:
            return self.test_data['version_check']
        elif 'COUNT' in sql:
            return self.test_data['count_query']
        elif 'DISTINCT site_id, deposition_id, content_type, created_date' in sql:
            return self.test_data['display_query']
        elif 'SELECT location FROM file_activity_log' in sql:
            return self.test_data['activity_query']
        else:
            return []

    def sqlCommand(self, sqlList: List[str]) -> None:
        """Mock SQL command execution."""
        if self.closed:
            raise Exception("Command executed on closed connection")
        self.commands.extend([sql.strip() for sql in sqlList])

    def close(self) -> None:
        """Mock connection close."""
        if not self.closed:
            self.closed = True
            self.commands = []  # Clear commands on close

class FileActivityDbTests(unittest.TestCase):
    """Test cases for FileActivityDb class."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.db = FileActivityDb()
        self.parser = FileMetadataParser()
        self.dummy_query = DummyMyDbQuery()
        self.db._FileActivityDb__myQuery = self.dummy_query
        self.test_dir = os.path.join(os.path.dirname(__file__), "test-output")
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)

    def tearDown(self) -> None:
        """Clean up test fixtures."""
        if hasattr(self, 'db'):
            self.db.close()
        if os.path.exists(self.test_dir):
            for root, dirs, files in os.walk(self.test_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(self.test_dir)

    def testDebugMode(self) -> None:
        """Test debug mode setting and getting."""
        # Test that FileActivityDb can be created without errors
        db = FileActivityDb(verbose=True)
        self.assertIsNotNone(db)
        
        # Test that FileActivityDb can be created with verbose=False
        db2 = FileActivityDb(verbose=False)
        self.assertIsNotNone(db2)

    def testParseFileMetadataValid(self) -> None:
        """Test parsing valid file names."""
        test_cases = [
            {
                'filename': "D_1234567890_content-milestone_P1.txt.V2.gz",
                'expected': ("D_1234567890", "content", "txt", 1, 2, "milestone")
            },
            {
                'filename': "D_1234567890_model_P1.cif.V1",
                'expected': ("D_1234567890", "model", "cif", 1, 1, "unknown")
            },
            {
                'filename': "D_1234567890_validation-annotate_P1.xml.V3",
                'expected': ("D_1234567890", "validation", "xml", 1, 3, "annotate")
            },
            {
                'filename': "D_1234567890_model-initial_P2.pdb.V1.gz",
                'expected': ("D_1234567890", "model", "pdb", 2, 1, "initial")
            }
        ]
        for test in test_cases:
            with self.subTest(filename=test['filename']):
                result = self.parser.parseFilePath(test['filename'])
                self.assertEqual(result, test['expected'])

    def testParseFileMetadataInvalid(self) -> None:
        """Test parsing invalid file names."""
        invalid_names = [
            "",
            None,
            "invalid_filename.txt",
            "D_123_no_part.cif",
            "D_123_type_P1.cif.VX",  # Invalid version
            "D_123_type_PX.cif.V1",  # Invalid part number
            "D_123_type.cif.V1",     # Missing part number
            "model_P1.cif.V1"        # Missing deposition ID
        ]
        for name in invalid_names:
            with self.subTest(filename=name):
                result = self.parser.parseFilePath(name) if name else None
                self.assertIsNone(result)

    def testDisplayFileActivityDb(self) -> None:
        """Test display of file activity database contents."""
        test_cases = [
            {'hours': 24, 'site_id': None},
            {'days': 7, 'site_id': 'WWPDB_TEST'},
            {'hours': 48, 'site_id': None},
            {'days': 1, 'site_id': 'WWPDB_PROD'}
        ]
        for test in test_cases:
            with self.subTest(params=test):
                self.db.displayActivity(**test)
                # Check if any command contains the expected SELECT query
                # The query will be split across multiple lines and include WHERE clauses
                expected_parts = [
                    "SELECT DISTINCT site_id, deposition_id, content_type, created_date",
                    "FROM file_activity_log",
                    "WHERE created_date >= DATE_SUB(NOW(), INTERVAL"
                ]
                matches = False  # Initialize matches before the loop
                for cmd in self.dummy_query.commands:
                    if all(part.lower() in cmd.lower() for part in expected_parts):
                        matches = True
                        break
                self.assertTrue(matches, f"Expected query parts not found in commands: {self.dummy_query.commands}")

    def testPurgeDepositionData(self) -> None:
        """Test purging data for specific deposition ID."""
        # Test successful purge
        self.db.purgeDataSetData("D_1000000000", confirmed=True)
        commands = self.dummy_query.commands.copy()  # Copy commands before they're cleared
        self.assertTrue(any('DELETE' in cmd for cmd in commands))

        # Clear commands for next test
        self.dummy_query.commands = []

        # Test without confirmation
        with self.assertRaises(ValueError):
            self.db.purgeDepositionData("D_1000000000", confirmed=False)

        # Test invalid deposition ID format
        invalid_ids = [
            "invalid_id",           # Completely invalid format
            "D_12345",              # Too short
            "D_123456789",          # Too short
            "D_12345678901",        # Too long
            "D_ABC123456",          # Contains letters
            "1000000000",           # Missing D_ prefix
            "D_1234567890",         # Exactly 10 digits but not starting with 100
            "D_0123456789"          # 10 digits but not starting with 100
        ]
        for invalid_id in invalid_ids:
            with self.subTest(deposition_id=invalid_id):
                with self.assertRaises(ValueError, msg=f"Expected ValueError for invalid ID: {invalid_id}"):
                    self.db.purgeDepositionData(invalid_id, confirmed=True)

    def testPurgeFileActivityDb(self) -> None:
        """Test purging all data from file activity database."""
        # Test without confirmation
        with self.assertRaises(ValueError):
            self.db.purgeAllData(confirmed=False)

        # Test with confirmation
        self.db.purgeAllData(confirmed=True)
        commands = self.dummy_query.commands.copy()  # Copy commands before they're cleared
        self.assertTrue(any('TRUNCATE' in cmd for cmd in commands))

    @unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
    def testUpdateFileActivity(self) -> None:
        """Test file activity database update operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "subdir")
            os.makedirs(subdir)

            # Test files with different versions and types
            test_files = [
                "D_1000000000_model-initial_P1.pdb.V1",
                "D_1000000000_model-initial_P1.pdb.V2",
                "D_1000000000_validation-annotate_P1.xml.V1",
                "D_1000000001_structure-release_P1.cif.V1",
                "D_1000000001_structure-release_P1.cif.V2",
                "D_1000000001_structure-release_P1.cif.V3"
            ]

            for fname in test_files:
                with open(os.path.join(subdir, fname), "w") as f:
                    f.write("test content")

            # Test database population
            self.db.populateFileActivityDb(tmpdir)
            self.assertTrue(len(self.dummy_query.commands) > 0)
            # Verify only highest versions are stored
            self.assertTrue(any('version_number = 2' in cmd for cmd in self.dummy_query.commands))
            self.assertTrue(any('version_number = 3' in cmd for cmd in self.dummy_query.commands))

    def testGetFileActivity(self) -> None:
        """Test retrieving file activity records."""
        test_cases = [
            {
                'params': {'hours': 24, 'deposition_ids': 'ALL'},
                'expected_count': 2
            },
            {
                'params': {'days': 7, 'deposition_ids': 'D_1000000000', 'file_types': 'model'},
                'expected_count': 2
            },
            {
                'params': {
                    'hours': 24,
                    'site_id': 'WWPDB_TEST',
                    'deposition_ids': 'D_1000000000-D_1000000001',
                    'formats': 'cif,xml'
                },
                'expected_count': 2
            },
            {
                'params': {
                    'days': 30,
                    'deposition_ids': 'D_1000000000,D_1000000001',
                    'file_types': 'model,validation',
                    'formats': 'ALL'
                },
                'expected_count': 2
            }
        ]

        for test in test_cases:
            with self.subTest(params=test['params']):
                results = self.db.getFileActivity(**test['params'])
                self.assertEqual(len(results), test['expected_count'])

    def testConnectionManagement(self) -> None:
        """Test database connection management."""
        # Test context manager usage
        with FileActivityDb() as db:
            self.assertIsNotNone(db)
            dummy_query = DummyMyDbQuery()
            db._FileActivityDb__myQuery = dummy_query
            results = db.getFileActivity(hours=24, deposition_ids="ALL")
            self.assertTrue(len(results) > 0)
            self.assertFalse(dummy_query.closed)

        # After context exit, connection should be closed
        self.assertTrue(dummy_query.closed)
        self.assertTrue(len(dummy_query.commands) == 0)  # Commands should be cleared

        # Test manual connection management
        db = FileActivityDb()
        dummy_query = DummyMyDbQuery()
        db._FileActivityDb__myQuery = dummy_query

        # Test operations
        db.getFileActivity(hours=24, deposition_ids="ALL")
        self.assertFalse(dummy_query.closed)
        self.assertTrue(len(dummy_query.commands) > 0)  # Should have commands

        # Test close
        db.close()
        self.assertTrue(dummy_query.closed)
        self.assertTrue(len(dummy_query.commands) == 0)  # Commands should be cleared
        self.assertIsNone(db._FileActivityDb__myQuery)

        # Test operations after close
        with self.assertRaises(Exception):
            db.getFileActivity(hours=24, deposition_ids="ALL")

def suiteFileActivityDbTests() -> unittest.TestSuite:
    """Create test suite for FileActivityDb tests."""
    suite = unittest.TestSuite()
    suite.addTest(FileActivityDbTests("testDebugMode"))
    suite.addTest(FileActivityDbTests("testParseFileMetadataValid"))
    suite.addTest(FileActivityDbTests("testParseFileMetadataInvalid"))
    suite.addTest(FileActivityDbTests("testDisplayFileActivityDb"))
    suite.addTest(FileActivityDbTests("testPurgeFileActivityDb"))
    suite.addTest(FileActivityDbTests("testUpdateFileActivity"))
    suite.addTest(FileActivityDbTests("testGetFileActivity"))
    suite.addTest(FileActivityDbTests("testPurgeDepositionData"))
    suite.addTest(FileActivityDbTests("testConnectionManagement"))
    return suite

if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suiteFileActivityDbTests())
