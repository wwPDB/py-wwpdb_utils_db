"""
FileActivityUtil Module
--------------------

This module provides utility methods for FileActivityDb operations.
It processes command line arguments and delegates to the appropriate FileActivityDb methods.
"""

import argparse
import sys
from typing import List, Optional

from wwpdb.utils.db.FileActivityDb import FileActivityDb

class FileActivityUtil:
    """
    Utility class for FileActivityDb operations.
    Provides high-level methods that handle argument processing and database operations.
    """

    def __init__(self):
        self.db = FileActivityDb()

    def purgeFileActivityDb(self, args: str) -> int:
        """
        Purge all data from the file activity database.

        Args:
            args: Command line arguments that must include --confirmed flag
                 to proceed with database purge

        Required Arguments:
            --confirmed: Flag to confirm purge operation

        Returns:
            int: 0 for success, 1 for failure

        Example:
            purgeFileActivityDb("--confirmed")
        """
        args_str = " ".join(args) if isinstance(args, list) else args
        args_list = args_str.split()
        if "--confirmed" not in args_list:
            sys.stderr.write("ERROR: Must provide --confirmed flag to purge database\n")
            return 1

        try:
            self.db.purgeFileActivityDb(confirmed=True)
            return 0
        except Exception as e:
            sys.stderr.write(f"ERROR: {str(e)}\n")
            return 1

    def displayFileActivityDb(self, args: str) -> int:
        """
        Display formatted database contents within a specified time range.

        Args:
            args: Command line arguments containing time range and optional site filter

        Required Arguments (mutually exclusive):
            --hours HOURS: Time range in hours to filter changes
            --days DAYS:   Time range in days to filter changes

        Optional Arguments:
            --site-id ID: Site ID to filter changes

        Returns:
            int: 0 for success, 1 for failure

        Example:
            displayFileActivityDb("--hours 24 --site-id WWPDB_DEPLOY")
            displayFileActivityDb("--days 7")
        """
        args_str = " ".join(args) if isinstance(args, list) else args
        parser = argparse.ArgumentParser()
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--hours", type=int, help="Time range in hours")
        group.add_argument("--days", type=int, help="Time range in days")
        parser.add_argument("--site-id", help="Site ID to filter results")

        try:
            parsed_args = parser.parse_args(args_str.split())
            self.db.displayFileActivityDb(
                hours=parsed_args.hours,
                days=parsed_args.days,
                site_id=parsed_args.site_id
            )
            return 0
        except SystemExit:
            return 1
        except Exception as e:
            sys.stderr.write(f"ERROR: {str(e)}\n")
            return 1

    def loadFileActivityDb(self, args: str) -> int:
        """
        Load file metadata into the database from a specified directory.

        Args:
            args: Command line arguments containing the directory path

        Required Arguments:
            --load-dir DIR: Directory containing files to process. The directory
                          should contain subdirectories with files following the
                          OneDep naming convention.

        Returns:
            int: 0 for success, 1 for failure

        Example:
            loadFileActivityDb("--load-dir /path/to/archive/files")
        """
        args_str = " ".join(args) if isinstance(args, list) else args
        parser = argparse.ArgumentParser()
        parser.add_argument("--load-dir", required=True,
                          help="Directory containing files to process")

        try:
            parsed_args = parser.parse_args(args_str.split())
            self.db.populateFileActivityDb(parsed_args.load_dir)
            return 0
        except SystemExit:
            return 1
        except Exception as e:
            sys.stderr.write(f"ERROR: {str(e)}\n")
            return 1

    def queryFileActivity(self, args: str) -> int:
        """
        Query changed files from the database using specified filters.

        Args:
            args: Command line arguments containing query filters

        Required Arguments:
            --hours HOURS or --days DAYS: Time range for filtering changes
            --deposition-ids IDS: List or range of deposition IDs:
                                - ALL: Query all deposition IDs
                                - Single ID: D_1000001
                                - Range: D_1000000-D_1000100
                                - List: D_1000001,D_1000002,D_1000003
            --file-types TYPES: Comma-separated list of file types or ALL
                              Example: model,structure,validation

        Optional Arguments:
            --site-id ID: Site ID to filter changes
            --formats FORMATS: Comma-separated list of file formats or ALL
                            Example: cif,xml,json

        Returns:
            int: 0 for success, 1 for failure

        Examples:
            queryFileActivity("--hours 24 --deposition-ids ALL --file-types model,structure")
            queryFileActivity("--days 7 --deposition-ids D_1000000-D_1000100 --file-types ALL --formats cif,xml")
        """
        args_str = " ".join(args) if isinstance(args, list) else args
        parser = argparse.ArgumentParser()
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--hours", type=int, help="Time range in hours")
        group.add_argument("--days", type=int, help="Time range in days")
        parser.add_argument("--site-id", help="Site ID to filter changes")
        parser.add_argument("--deposition-ids", required=True,
                          help="List or range of deposition IDs")
        parser.add_argument("--file-types", required=True,
                          help="Comma-separated list of file types or ALL")
        parser.add_argument("--formats", default="ALL",
                          help="Comma-separated list of file formats or ALL")

        try:
            parsed_args = parser.parse_args(args_str.split())
            results = self.db.getFileActivity(
                hours=parsed_args.hours,
                days=parsed_args.days,
                site_id=parsed_args.site_id,
                deposition_ids=parsed_args.deposition_ids,
                file_types=parsed_args.file_types,
                formats=parsed_args.formats
            )
            if results:
                print("\n".join(results))
            return 0
        except SystemExit:
            return 1
        except Exception as e:
            sys.stderr.write(f"ERROR: {str(e)}\n")
            return 1