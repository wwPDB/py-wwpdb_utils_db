##
# File:    MysqlSchemaImporterMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Tests for MysqlSchemaImporter with a mocked mysql command line client.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import ast
import contextlib
import io
import os
import runpy
import shutil
import sys
import tempfile
import unittest
from typing import Any, Dict, List
from unittest import mock

from wwpdb.utils.db.MysqlSchemaImporter import MysqlSchemaImporter

_HEADER = "Field\tType\tNull\tKey\tDefault\tExtra\n"
_DESCRIBE: Dict[str, str] = {
    "audit_author": _HEADER
    + "structure_id\tvarchar(15)\tNO\tPRI\tNULL\t\n"
    + "ordinal\tint\tNO\tMUL\tNULL\t\n"
    + "name\tvarchar(80)\tYES\t\tNULL\t\n"
    + "this line is bad\n"
    + "created\tdatetime\tYES\t\tNULL\t\n",
    "empty_table": "",
    "nokey": _HEADER + "name\tvarchar(80)\tYES\t\tNULL\t\n",
}


class MysqlSchemaImporterMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__cwd = os.getcwd()
        self.__workPath = tempfile.mkdtemp()
        os.chdir(self.__workPath)
        self.__lfh = io.StringIO()
        self.__cmds: List[str] = []

    def tearDown(self) -> None:
        os.chdir(self.__cwd)
        shutil.rmtree(self.__workPath, ignore_errors=True)

    def __fakeMysql(self, cmd: str) -> int:
        """Emulate 'mysql ... -e "describe <table>;" > <file>'"""
        self.__cmds.append(cmd)
        tableName = cmd.split('"describe ')[1].split(";")[0]
        outFile = cmd.split(" > ")[-1].strip()
        with open(outFile, "w") as ofh:
            ofh.write(_DESCRIBE.get(tableName, ""))
        return 0

    def __create(self, tableNameList: List[str]) -> Dict[str, Any]:
        msi = MysqlSchemaImporter("user1", "secret", "dbhost", mysqlPath="/usr/bin/mysql", verbose=True, log=self.__lfh)
        out = io.StringIO()
        with mock.patch("wwpdb.utils.db.MysqlSchemaImporter.os.system", side_effect=self.__fakeMysql), contextlib.redirect_stdout(out):
            msi.create("da_internal", tableNameList)
        ret: Dict[str, Any] = ast.literal_eval(out.getvalue())
        return ret

    def testCreate(self) -> None:
        sD = self.__create(["audit_author", "empty_table"])
        self.assertEqual(
            self.__cmds,
            [
                '/usr/bin/mysql --user=user1 --password=secret --host=dbhost da_internal -e "describe audit_author;"  > mysql-schema-audit_author.txt',
                '/usr/bin/mysql --user=user1 --password=secret --host=dbhost da_internal -e "describe empty_table;"  > mysql-schema-empty_table.txt',
            ],
        )
        # Intermediate files are removed
        self.assertEqual(os.listdir(self.__workPath), [])
        # Tables without column data are skipped
        self.assertEqual(list(sD.keys()), ["AUDIT_AUTHOR"])
        tD = sD["AUDIT_AUTHOR"]
        self.assertEqual(tD["TABLE_ID"], "AUDIT_AUTHOR")
        self.assertEqual(tD["TABLE_NAME"], "audit_author")
        self.assertEqual(tD["TABLE_TYPE"], "transactional")
        self.assertEqual(tD["TABLE_DELETE_ATTRIBUTE"], "STRUCTURE_ID")
        self.assertEqual(tD["INDICES"], {"p1": {"ATTRIBUTES": ("STRUCTURE_ID", "ORDINAL"), "TYPE": "UNIQUE"}})
        self.assertEqual(tD["MAP_MERGE_INDICES"], {"audit_author": {"ATTRIBUTES": ("STRUCTURE_ID", "ORDINAL"), "TYPE": "EQUI-JOIN"}})
        self.assertEqual(tD["ATTRIBUTES"], {"STRUCTURE_ID": "structure_id", "ORDINAL": "ordinal", "NAME": "name", "CREATED": "created"})
        self.assertEqual(tD["ATTRIBUTE_MAP"]["NAME"], ("audit_author", "name", None, None))
        aI = tD["ATTRIBUTE_INFO"]
        self.assertEqual(aI["STRUCTURE_ID"], {"NULLABLE": False, "ORDER": 1, "PRECISION": 0, "PRIMARY_KEY": True, "SQL_TYPE": "VARCHAR", "WIDTH": "15"})
        self.assertEqual(aI["ORDINAL"], {"NULLABLE": False, "ORDER": 2, "PRECISION": 0, "PRIMARY_KEY": True, "SQL_TYPE": "INT", "WIDTH": 10})
        self.assertEqual(aI["NAME"]["NULLABLE"], True)
        self.assertEqual(aI["NAME"]["PRIMARY_KEY"], False)
        self.assertEqual(aI["CREATED"]["SQL_TYPE"], "DATETIME")
        self.assertEqual(aI["CREATED"]["ORDER"], 4)
        log = self.__lfh.getvalue()
        self.assertIn("bad line in mysql-schema-audit_author.txt  = this line is bad", log)
        self.assertIn("tableName audit_author length 4", log)

    def testCreateEmpty(self) -> None:
        sD = self.__create([])
        self.assertEqual(sD, {})
        self.assertEqual(self.__cmds, [])

    @unittest.expectedFailure
    def testCreateTableWithoutKey(self) -> None:
        """BUG: a table without any PRI/MUL key column raises IndexError (attIdKeyList[0]) in __buildDef."""
        sD = self.__create(["nokey"])
        self.assertIn("NOKEY", sD)

    def testMain(self) -> None:
        out = io.StringIO()
        env = {"MYSQL_DB_USER": "envuser", "MYSQL_SBKB_PW": "envpw"}
        modName = "wwpdb.utils.db.MysqlSchemaImporter"
        with mock.patch.dict(os.environ, env), mock.patch.dict(sys.modules), mock.patch("os.system", side_effect=self.__fakeMysql):
            sys.modules.pop(modName, None)
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(self.__lfh):
                runpy.run_module(modName, run_name="__main__")
        sD = ast.literal_eval(out.getvalue())
        self.assertEqual(list(sD.keys()), ["AUDIT_AUTHOR"])
        # 22 table names with one duplicate
        self.assertEqual(len(self.__cmds), 22)
        self.assertTrue(self.__cmds[0].startswith("/opt/local/bin/mysql --user=envuser --password=envpw --host=localhost da_internal "))


if __name__ == "__main__":
    unittest.main()
