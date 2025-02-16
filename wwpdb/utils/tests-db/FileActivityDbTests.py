##
# File:    FileActivityDbTests.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#
##
"""
Unit tests for file activity database management operations.

These tests verify the functionality of the FileActivityDb class which handles:
- Parsing file metadata from standardized filenames
- Tracking file activities in the OneDep system database
- Managing file version history
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

# Standard library imports
import os
import sys
import tempfile
import unittest
import logging
from datetime import datetime
from typing import List

# Application imports
from wwpdb.utils.testing.Features import Features
from wwpdb.utils.db.FileActivityDb import FileActivityDb

# Configure logging to capture debug messages
logging.basicConfig(level=logging.DEBUG)

# Dummy query class to capture SQL commands instead of executing them.
class DummyMyDbQuery:
    def __init__(self):
        self.commands = []
    def selectRows(self, sql: str) -> List[List]:
        # Return a dummy value for testing get_file_activity, etc.
        return [[1, datetime.now().strftime('%Y-%m-%d %H:%M:%S')]]
    def sqlCommand(self, sqlList: List[str]):
        self.commands.extend(sqlList)

class FileActivityDbTests(unittest.TestCase):

    def setUp(self):
        # Use a dummy query instance to override actual DB connectivity
        self.db = FileActivityDb()
        self.db.myQuery = DummyMyDbQuery()

    def testParseFileMetadataValid(self):
        # Valid file name example
        valid_name = "D_1234567890_content-milestone_P1.txt.V2.gz"
        metadata = self.db.parseFileMetadata(valid_name)
        self.assertIsNotNone(metadata)
        deposition_id, content_type, format_type, part_number, version_number, milestone = metadata
        self.assertEqual(deposition_id, "D_1234567890")
        self.assertEqual(content_type, "content")
        self.assertEqual(format_type, "txt")
        self.assertEqual(part_number, 1)
        self.assertEqual(version_number, 2)
        self.assertEqual(milestone, "milestone")

    def testParseFileMetadataInvalid(self):
        # Invalid file name
        invalid_name = "invalid_filename.txt"
        metadata = self.db.parseFileMetadata(invalid_name)
        self.assertIsNone(metadata)

    def testGetFileActivity(self):
        """Test getFileActivity method"""
        result = self.db.getFileActivity("D_1234567890")
        self.assertIsNotNone(result)
        self.assertTrue(isinstance(result, list))

    def testParseFileMetadataWithCompressedFile(self):
        """Test parseFileMetadata with compressed file extensions"""
        compressed_name = "D_1234567890_model-milestone_P1.cif.V2.gz"
        metadata = self.db.parseFileMetadata(compressed_name)
        self.assertIsNotNone(metadata)
        deposition_id, content_type, format_type, part_number, version_number, milestone = metadata
        self.assertEqual(deposition_id, "D_1234567890")
        self.assertEqual(content_type, "model")
        self.assertEqual(format_type, "cif")
        self.assertEqual(part_number, 1)
        self.assertEqual(version_number, 2)
        self.assertEqual(milestone, "milestone")

    def testParseFileMetadataWithSpecialMilestone(self):
        """Test parseFileMetadata with special milestone types"""
        special_name = "D_1234567890_model-annotate_P1.pdb.V1"
        metadata = self.db.parseFileMetadata(special_name)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata[5], "annotate")

    @unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
    def testUpdateFileActivity(self):
        """Test updating file activity"""
        filePath = "D_1234567890_model-initial_P1.pdb.V1"
        self.db.updateFileActivity(filePath)
        # Verify SQL commands were generated
        self.assertTrue(len(self.db.myQuery.commands) > 0)

    def testParseFileMetadataEdgeCases(self):
        """Test parseFileMetadata with edge cases"""
        # Test with empty string
        self.assertIsNone(self.db.parseFileMetadata(""))
        # Test with None
        self.assertIsNone(self.db.parseFileMetadata(None))
        # Test with invalid version number
        invalid_version = "D_1234567890_model-initial_P1.pdb.VX"
        self.assertIsNone(self.db.parseFileMetadata(invalid_version))

    def testMultipleFileVersions(self):
        """Test handling of multiple file versions"""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a subdirectory so that populateFileActivityDb processes the files.
            subdir = os.path.join(tmpdir, "subdir")
            os.mkdir(subdir)
            # Create multiple versions of the same file in the subdirectory.
            baseName = "D_1234567890_model-milestone_P1.pdb"
            for version in range(1, 4):
                filePath = os.path.join(subdir, f"{baseName}.V{version}")
                with open(filePath, "w") as f:
                    f.write(f"version {version}")

            self.db.populateFileActivityDb(tmpdir)
            logging.debug(f"SQL Commands: {self.db.myQuery.commands}")
            self.assertTrue(len(self.db.myQuery.commands) >= 1)

    @unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
    def testPopulateFileActivityDb(self):
        # Create a temporary directory with a dummy file to simulate populate_file_activity_db
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = os.path.join(tmpdir, "subdir")
            os.mkdir(subdir)
            # Create a dummy file that matches naming pattern.
            filePath = os.path.join(subdir, "D_1234567890_type-milestone_P1.txt.V1")
            with open(filePath, "w") as f:
                f.write("dummy content")
            # Call populate method (it will use DummyMyDbQuery so no real SQL executed)
            self.db.populateFileActivityDb(tmpdir)
            # Check that some SQL commands were generated.
            self.assertTrue(len(self.db.myQuery.commands) > 0)

if __name__ == "__main__":
    unittest.main()
