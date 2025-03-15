##
# File:    FileActivityUtil.py
# Author:  Vivek Reddy Chithari
# Email:   vivek.chithari@rcsb.org
# Date:    2025-02-16
#
# Updates:
#   - Added pre-check for "--confirmed" in purge commands.
#   - Wrapped argument parsing in try/except to catch SystemExit and return a failure code.
#   - Improved code organization and dependency injection.
##
"""
Utility module for file activity database operations.

This module provides a high-level interface for:
- Managing file activity database operations
- Querying and displaying file changes
- Loading and purging file metadata
"""

__docformat__ = "restructuredtext en"
__author__ = "Vivek Reddy Chithari"
__email__ = "vivek.chithari@rcsb.org"
__license__ = "Apache 2.0"

import argparse
import logging
from typing import List, Union, Optional

from wwpdb.utils.db.FileActivityDb import FileActivityDb

logger = logging.getLogger(__name__)


class FileActivityUtil:
    """
    Utility class for FileActivityDb operations.

    This class provides high-level methods for managing file activity database operations.
    It is also used as the implementation layer for the command-line interface.

    The class supports:
    - Querying and displaying file activity
    - Loading file metadata from directories
    - Purging database records (all or by dataset)
    - Executing database operations with proper error handling
    """

    # Class attribute type annotation
    db: FileActivityDb

    def __init__(self, db: Optional[FileActivityDb] = None, verbose: bool = False) -> None:
        """Initialize FileActivityUtil with a database connection.

        Args:
            db (Optional[FileActivityDb]): Database instance to use. If None, creates a new instance.
            verbose (bool): Enable verbose output for the database instance
        """
        self.db = db if db is not None else FileActivityDb(verbose=verbose)
        self.__verbose = verbose

    def __parseArgs(self, args: Union[str, List[str]]) -> List[str]:
        """Convert string arguments to list if needed.

        Args:
            args (Union[str, List[str]]): Command line arguments as string or list

        Returns:
            List[str]: Arguments as list
        """
        return args.split() if isinstance(args, str) else args

    def __createParser(self, description: str) -> argparse.ArgumentParser:
        """Create a base argument parser with common settings.

        Args:
            description (str): Parser description

        Returns:
            argparse.ArgumentParser: Configured parser
        """
        parser = argparse.ArgumentParser(description=description)
        parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
        return parser

    def __createDbWithVerbose(self, parsed_args: argparse.Namespace) -> None:
        """Handle verbose mode by creating a verbose database instance if needed.

        If verbose mode is requested but our current instance isn't verbose,
        we'll create a new verbose instance.

        Args:
            parsed_args (argparse.Namespace): Parsed command line arguments
        """
        # Check if the verbose attribute exists and is True
        has_verbose = hasattr(parsed_args, "verbose") and parsed_args.verbose

        if has_verbose and not self.__verbose:
            # Configure logging to show DEBUG level messages
            logging.getLogger().setLevel(logging.DEBUG)
            # Update the verbose setting
            self.__verbose = True
            # Create a new db instance with verbose=True
            self.db = FileActivityDb(verbose=True)
            logging.debug("Verbose mode enabled - detailed logging activated")

    def purgeAllData(self, args: Union[str, List[str]]) -> int:
        """
        Purge all data from the file activity database.

        Args:
            args (Union[str, List[str]]): Command line arguments that must include --confirmed flag
                 to proceed with database purge

        Required Arguments:
            --confirmed: Flag to confirm purge operation

        Returns:
            int: 0 for success, 1 for failure

        Example:
            purgeAllData("--confirmed")
        """
        args_list = self.__parseArgs(args)
        if "--confirmed" not in args_list:
            logger.error("Must provide --confirmed flag to purge database")
            return 1

        parser = self.__createParser("Purge all data from file activity database")
        parser.add_argument("--confirmed", action="store_true", required=True, help="Confirmation flag required for purge operation")
        try:
            parsed_args = parser.parse_args(args_list)
            self.__createDbWithVerbose(parsed_args)
            self.db.purgeAllData(confirmed=parsed_args.confirmed)
            logger.info("Successfully purged all data from the database")
            return 0
        except SystemExit as e:
            logger.error("Argument parsing error: %s", e)
            return 1
        except Exception as e:
            logger.error("Failed to purge database: %s", str(e))
            return 1

    def purgeDataSetData(self, args: Union[str, List[str]]) -> int:
        """
        Purge data for a specific deposition ID from the database.

        Args:
            args (Union[str, List[str]]): Command line arguments

        Required Arguments:
            --deposition-id: The deposition ID to purge (e.g., D_1000000000)
            --confirmed: Flag to confirm purge operation

        Returns:
            int: 0 for success, 1 for failure
        """
        args_list = self.__parseArgs(args)
        if "--confirmed" not in args_list:
            logger.error("Must provide --confirmed flag to purge database")
            return 1

        parser = self.__createParser("Purge data for a specific deposition ID")
        parser.add_argument("--deposition-id", required=True, help="Deposition ID to purge (e.g., D_1000000000)")
        parser.add_argument("--confirmed", action="store_true", required=True, help="Confirmation flag required for purge operation")
        try:
            parsed_args = parser.parse_args(args_list)
            self.__createDbWithVerbose(parsed_args)
            self.db.purgeDataSetData(deposition_id=parsed_args.deposition_id, confirmed=parsed_args.confirmed)
            return 0
        except SystemExit as e:
            logger.error("Argument parsing error: %s", e)
            return 1
        except Exception as e:
            logger.error("Failed to purge deposition data: %s", str(e))
            return 1

    def displayActivity(self, args: Union[str, List[str]]) -> int:
        """
        Display formatted database contents within a specified time range.

        Args:
            args (Union[str, List[str]]): Command line arguments containing time range and optional site filter

        Required Arguments (mutually exclusive):
            --hours HOURS: Time range in hours to filter changes
            --days DAYS:   Time range in days to filter changes

        Optional Arguments:
            --site-id ID: Site ID to filter changes

        Returns:
            int: 0 for success, 1 for failure

        Example:
            displayActivity("--hours 24 --site-id WWPDB_DEPLOY")
            displayActivity("--days 7")
        """
        parser = self.__createParser("Display file activity database contents")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--hours", type=int, help="Time range in hours")
        group.add_argument("--days", type=int, help="Time range in days")
        parser.add_argument("--site-id", help="Site ID to filter results")
        try:
            parsed_args = parser.parse_args(self.__parseArgs(args))
            self.__createDbWithVerbose(parsed_args)
            self.db.displayActivity(hours=parsed_args.hours, days=parsed_args.days, site_id=parsed_args.site_id)
            return 0
        except SystemExit as e:
            logger.error("Argument parsing error: %s", e)
            return 1
        except Exception as e:
            logger.error("Failed to display database contents: %s", str(e))
            return 1

    def populateFromDirectory(self, args: Union[str, List[str]]) -> int:
        """
        Load file metadata into the database from a specified directory.

        Args:
            args (Union[str, List[str]]): Command line arguments containing the directory path

        Required Arguments:
            --load-dir DIR: Directory containing files to process. The directory
                          should contain subdirectories with files following the
                          OneDep naming convention.

        Optional Arguments:
            --site-id ID: Site ID to associate with the loaded files

        Returns:
            int: 0 for success, 1 for failure

        Example:
            populateFromDirectory("--load-dir /path/to/archive/files")
        """
        parser = self.__createParser("Load file metadata into database")
        parser.add_argument("--load-dir", required=True, help="Directory containing files to process")
        parser.add_argument("--site-id", help="Site ID to associate with loaded files")
        try:
            parsed_args = parser.parse_args(self.__parseArgs(args))
            self.__createDbWithVerbose(parsed_args)
            self.db.populateFromDirectory(parsed_args.load_dir, site_id=parsed_args.site_id)
            return 0
        except SystemExit as e:
            logger.error("Argument parsing error: %s", e)
            return 1
        except Exception as e:
            logger.error("Failed to load file activity data: %s", str(e))
            return 1

    def getActivity(self, args: Union[str, List[str]]) -> int:
        """
        Query changed files from the database using specified filters.

        Args:
            args (Union[str, List[str]]): Command line arguments containing query filters

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
            getActivity("--hours 24 --deposition-ids ALL --file-types model,structure")
            getActivity("--days 7 --deposition-ids D_1000000-D_1000100 --file-types ALL --formats cif,xml")
        """
        parser = self.__createParser("Query file activity database")
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("--hours", type=int, help="Time range in hours")
        group.add_argument("--days", type=int, help="Time range in days")
        parser.add_argument("--site-id", help="Site ID to filter changes")
        parser.add_argument("--deposition-ids", required=True, help="List or range of deposition IDs or ALL")
        parser.add_argument("--file-types", required=True, help="Comma-separated list of file types or ALL")
        parser.add_argument("--formats", default="ALL", help="Comma-separated list of file formats or ALL")
        try:
            parsed_args = parser.parse_args(self.__parseArgs(args))
            self.__createDbWithVerbose(parsed_args)
            results = self.db.getFileActivity(
                hours=parsed_args.hours,
                days=parsed_args.days,
                site_id=parsed_args.site_id,
                deposition_ids=parsed_args.deposition_ids,
                file_types=parsed_args.file_types,
                formats=parsed_args.formats,
            )
            if results:
                print("\n".join(results))
            return 0
        except SystemExit as e:
            logger.error("Argument parsing error: %s", e)
            return 1
        except Exception as e:
            logger.error("Failed to query file activity: %s", str(e))
            return 1


def main() -> int:
    """
    Main entry point for the file activity command-line interface.

    This function serves as the command-line interface for the FileActivityDb
    module, allowing users to interact with the file activity database through
    a simple command structure with subcommands.

    Usage:
        file-activity [command] [options]

    Commands:
        display         Display activity within a time range
        query           Query file activity with filters
        load            Load file metadata from a directory
        purge           Purge all data from the database
        purge-dataset   Purge data for a specific dataset

    Each command has its own set of options. Use --help with any command
    for more information:
        file-activity display --help

    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    import argparse
    import logging

    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Create the main parser
    parser = argparse.ArgumentParser(
        description="""File Activity Database Utility
        
This tool manages the OneDep system's file activity database.

Commands:
  display         Show recently changed files within a time range
  query           Find files matching specific criteria 
  load            Populate database from a directory of files
  purge           Remove all database records (CAUTION: destructive)
  purge-dataset   Remove records for a specific deposition ID

Use -v/--verbose to enable detailed logging
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Add global verbose flag to main parser
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Add subparsers for commands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")

    # Display command
    display_parser = subparsers.add_parser("display", help="Display activity within a time range")
    display_parser.description = """
    Display formatted database contents within a time range.
    
    Examples:
      display --hours 24
      display --days 7 --site-id WWPDB_DEPLOY
      display --days 7 -v  # With verbose output
    """
    display_parser.formatter_class = argparse.RawDescriptionHelpFormatter
    time_group = display_parser.add_mutually_exclusive_group(required=True)
    time_group.add_argument("--hours", type=int, help="Time range in hours (e.g., 24 for last day)")
    time_group.add_argument("--days", type=int, help="Time range in days (e.g., 7 for last week)")
    display_parser.add_argument("--site-id", help="Site ID to filter results (optional)")
    display_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Query command
    query_parser = subparsers.add_parser("query", help="Query file activity with filters")
    query_parser.description = """
    Query changed files with flexible filters.
    
    Examples:
      query --hours 24 --deposition-ids ALL --file-types model
      query --days 7 --deposition-ids D_8000210000-D_8000210100 --file-types pdbx --formats cif,xml
      query --days 7 --deposition-ids D_8000210001,D_8000210002 --file-types model --formats pdbx -v
    """
    query_parser.formatter_class = argparse.RawDescriptionHelpFormatter
    query_time_group = query_parser.add_mutually_exclusive_group(required=True)
    query_time_group.add_argument("--hours", type=int, help="Time range in hours")
    query_time_group.add_argument("--days", type=int, help="Time range in days")
    query_parser.add_argument("--site-id", help="Site ID to filter changes")
    query_parser.add_argument(
        "--deposition-ids",
        required=True,
        help="Deposition IDs to filter. Options: 'ALL' for all IDs, single ID (D_8000210001), "
             "range (D_8000210000-D_8000210100), or comma-separated list (D_8000210001,D_8000210002)",
    )
    query_parser.add_argument("--file-types", required=True, help="Comma-separated list of file types (e.g., model,structure,pdbx) or 'ALL'")
    query_parser.add_argument("--formats", default="ALL", help="Comma-separated list of file formats (e.g., cif,xml,json) or 'ALL' (default)")
    query_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Load command
    load_parser = subparsers.add_parser("load", help="Load file metadata from a directory")
    load_parser.description = """
    Load file metadata into the database from a specified directory.
    
    Example:
      load --load-dir /path/to/archive/files --site-id WWPDB_DEPLOY
    """
    load_parser.formatter_class = argparse.RawDescriptionHelpFormatter
    load_parser.add_argument("--load-dir", required=True, help="Directory containing files to process (should contain subdirectories with files)")
    load_parser.add_argument("--site-id", help="Site ID to associate with loaded files (optional)")
    load_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Purge command
    purge_parser = subparsers.add_parser("purge", help="Purge all data from the database")
    purge_parser.description = """
    Purge all data from the file activity database.
    
    CAUTION: This is a destructive operation that removes all records.
    Requires explicit confirmation with --confirmed flag.
    
    Example:
      purge --confirmed
    """
    purge_parser.formatter_class = argparse.RawDescriptionHelpFormatter
    purge_parser.add_argument("--confirmed", action="store_true", required=True, help="Confirmation flag required for purge operation (REQUIRED)")
    purge_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Purge dataset command
    purge_dataset_parser = subparsers.add_parser("purge-dataset", help="Purge data for a specific dataset")
    purge_dataset_parser.description = """
    Purge all data for a specific deposition ID from the database.
    
    CAUTION: This is a destructive operation that removes all records for the specified ID.
    Requires explicit confirmation with --confirmed flag.
    
    Example:
      purge-dataset --deposition-id D_8000210018 --confirmed
      purge-dataset --deposition-id D_8000210018 --confirmed -v  # With verbose output
    """
    purge_dataset_parser.formatter_class = argparse.RawDescriptionHelpFormatter
    purge_dataset_parser.add_argument("--deposition-id", required=True, help="Deposition ID to purge (e.g., D_8000210018)")
    purge_dataset_parser.add_argument("--confirmed", action="store_true", required=True, help="Confirmation flag required for purge operation (REQUIRED)")
    purge_dataset_parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")

    # Parse arguments
    args = parser.parse_args()

    # Check if command was specified
    if not args.command:
        parser.print_help()
        return 1

    # Configure logging based on verbosity - handle both global and subcommand verbose flags
    # Get verbose flag from the appropriate namespace
    verbose_enabled = getattr(args, 'verbose', False)
    
    if verbose_enabled:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Verbose mode enabled")

    # Create utility instance with verbose flag
    util = FileActivityUtil(verbose=verbose_enabled)

    # Dispatch to appropriate method based on command
    if args.command == "display":
        cmd = ""
        if args.hours:
            cmd = f"--hours {args.hours}"
        else:
            cmd = f"--days {args.days}"

        if args.site_id:
            cmd += f" --site-id {args.site_id}"
            
        # Add verbose flag if enabled
        if verbose_enabled:
            cmd += " -v"

        return util.displayActivity(cmd)
    elif args.command == "query":
        cmd = f"--{'hours' if args.hours else 'days'} {args.hours or args.days}"
        if args.site_id:
            cmd += f" --site-id {args.site_id}"
        cmd += f" --deposition-ids {args.deposition_ids} --file-types {args.file_types} --formats {args.formats}"
        
        # Add verbose flag if enabled
        if verbose_enabled:
            cmd += " -v"
            
        return util.getActivity(cmd)
    elif args.command == "load":
        cmd = f"--load-dir {args.load_dir}"
        if args.site_id:
            cmd += f" --site-id {args.site_id}"
            
        # Add verbose flag if enabled
        if verbose_enabled:
            cmd += " -v"
            
        return util.populateFromDirectory(cmd)
    elif args.command == "purge":
        cmd = "--confirmed" if args.confirmed else ""
        
        # Add verbose flag if enabled
        if verbose_enabled:
            cmd += " -v"
            
        return util.purgeAllData(cmd)
    elif args.command == "purge-dataset":
        cmd = f"--deposition-id {args.deposition_id}"
        if args.confirmed:
            cmd += " --confirmed"
            
        # Add verbose flag if enabled
        if verbose_enabled:
            cmd += " -v"
            
        return util.purgeDataSetData(cmd)

    # Should not reach here
    logger.error("Unknown command: %s", args.command)
    return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
