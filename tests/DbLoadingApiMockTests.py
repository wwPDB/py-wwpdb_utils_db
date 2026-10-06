##
# File:    DbLoadingApiMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Tests for DbLoadingApi with mocked site configuration, RcsbDpUtility and shell commands.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
from typing import Any, Callable, Dict, List, Optional
from unittest import mock

if __package__ is None or __package__ == "":  # noqa: PLC1901
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mock_import import mocksetup  # type: ignore  # noqa: F401 pylint: disable=unused-import,import-error
else:
    from .mock_import import mocksetup  # noqa: F401,TID252 pylint: disable=unused-import,import-error

from wwpdb.utils.db.DbLoadingApi import DbLoadingApi

DEPID = "D_1000000001"


class DbLoadingApiMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__topPath = tempfile.mkdtemp()
        self.__archivePath = os.path.join(self.__topPath, "data")
        self.__sessionDir = os.path.join(self.__topPath, "session")
        os.makedirs(self.__sessionDir)
        self.__dataDir = self.__sessionDir + "/dbdata"
        self.__cfg: Dict[str, Optional[str]] = {
            "SITE_DB_HOST_NAME": "dbhost",
            "SITE_DB_USER_NAME": "dbuser",
            "SITE_DB_PASSWORD": "dbpw",
            "SITE_DB_PORT_NUMBER": "3306",
            "SITE_DB_SOCKET": None,
            "SITE_ARCHIVE_STORAGE_PATH": self.__archivePath,
        }
        self.__lfh = io.StringIO()
        self.__patchers = [
            mock.patch("wwpdb.utils.db.DbLoadingApi.getSiteId", return_value="TEST_SITE"),
            mock.patch("wwpdb.utils.db.DbLoadingApi.ConfigInfo"),
            mock.patch("wwpdb.utils.db.DbLoadingApi.ConfigInfoAppCommon"),
            mock.patch("wwpdb.utils.db.DbLoadingApi.RcsbDpUtility"),
        ]
        mocks = [p.start() for p in self.__patchers]
        self.__mockCi = mocks[1]
        self.__mockCiCommon = mocks[2]
        self.__mockDp = mocks[3]
        self.__mockCi.return_value.get.side_effect = lambda k: self.__cfg[k]
        self.__mockCiCommon.return_value.get_site_da_internal_schema_path.return_value = "/schema/da_internal.cif"
        self.__mockCiCommon.return_value.get_db_loader_path.return_value = "/bin/db-loader"

    def tearDown(self) -> None:
        for p in self.__patchers:
            p.stop()
        shutil.rmtree(self.__topPath, ignore_errors=True)

    def __makeModel(self) -> None:
        pth = os.path.join(self.__archivePath, "archive", DEPID)
        os.makedirs(pth)
        with open(os.path.join(pth, DEPID + "_model_P1.cif.V1"), "w") as ofh:
            ofh.write("data_x\n")

    def __writer(self, files: Dict[str, str]) -> Callable[[str], int]:
        """Return a fake os.system that creates the named files in the data directory on its first invocation"""

        def fake(cmd: str) -> int:  # noqa: ARG001 pylint: disable=unused-argument
            for fn, content in files.items():
                pth = os.path.join(self.__dataDir, fn)
                if not os.path.exists(pth):
                    with open(pth, "w") as ofh:
                        ofh.write(content)
            return 0

        return fake

    def __run(self, func: Callable[..., Any], *args: Any, files: Optional[Dict[str, str]] = None) -> Any:
        """Run func with mocked os.system - returns (return value, stdout, list of commands)"""
        out = io.StringIO()
        with mock.patch("wwpdb.utils.db.DbLoadingApi.os.system") as mockSys, contextlib.redirect_stdout(out):
            if files is not None:
                mockSys.side_effect = self.__writer(files)
            ret = func(*args)
        cmds: List[str] = [c.args[0] for c in mockSys.call_args_list]
        return ret, out.getvalue(), cmds

    def testConstructor(self) -> None:
        DbLoadingApi(log=self.__lfh, verbose=True)
        self.__mockCi.assert_called_once_with()
        self.__mockCiCommon.assert_called_once_with("TEST_SITE")
        requested = [c.args[0] for c in self.__mockCi.return_value.get.call_args_list]
        self.assertEqual(
            sorted(requested),
            sorted(["SITE_DB_HOST_NAME", "SITE_DB_USER_NAME", "SITE_DB_PASSWORD", "SITE_DB_PORT_NUMBER", "SITE_DB_SOCKET", "SITE_ARCHIVE_STORAGE_PATH"]),
        )

    # ------------------------------------------------------------- doDataLoading

    def testDataLoadingNoModel(self) -> None:
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoading, DEPID.lower(), self.__sessionDir)
        self.assertIn("No any cif file found", out)
        self.assertEqual(cmds, [])
        self.assertFalse(os.path.exists(self.__dataDir))

    def testDataLoadingNoFileList(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoading, DEPID.lower(), self.__sessionDir)
        self.assertIn("Creating " + self.__dataDir, out)
        self.assertTrue(os.path.isdir(self.__dataDir))
        self.assertEqual(len(cmds), 1)
        self.assertTrue(cmds[0].startswith("cd " + self.__dataDir + "; rm -f *; ls -tl "))
        self.assertIn(os.path.join(self.__archivePath, "archive", DEPID) + "/D_*model_P1.cif.V[1-9]*", cmds[0])
        self.assertIn("/bin/db-loader -server mysql -list FILELIST -map /schema/da_internal.cif -db da_internal -firstDataBlock >& db-loader.log", cmds[0])
        self.assertIn("No cif file found. Please check if the cif file exists.", out)

    def testDataLoadingNoSql(self) -> None:
        self.__makeModel()
        os.makedirs(self.__dataDir)
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoading, DEPID, self.__sessionDir, files={"FILELIST": "x"})
        self.assertNotIn("Creating", out)
        self.assertEqual(len(cmds), 1)
        self.assertIn('didn\'t generate the data file "DB_LOADER.sql"', out)
        self.assertIn(os.path.join(self.__dataDir, "db-loader.log"), out)

    def testDataLoadingSuccess(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER.sql": "select 1;", "data_loading.log": "all good\n"}
        _, out, cmds = self.__run(api.doDataLoading, DEPID, self.__sessionDir, files=files)
        self.assertEqual(len(cmds), 2)
        sqlFile = os.path.join(self.__dataDir, "DB_LOADER.sql")
        logFile = os.path.join(self.__dataDir, "data_loading.log")
        self.assertEqual(cmds[1], "cd %s; mysql -u dbuser -pdbpw -h dbhost -P 3306 <%s >&%s" % (self.__dataDir, sqlFile, logFile))
        self.assertIn("Finished the database commands", out)
        self.assertNotIn("ERROR found", out)

    def testDataLoadingSocketWithErrors(self) -> None:
        self.__cfg["SITE_DB_SOCKET"] = "/tmp/mysql.sock"  # noqa: S108
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER.sql": "select 1;", "data_loading.log": "line 1\nERROR 1064 at line 2\n"}
        _, out, cmds = self.__run(api.doDataLoading, DEPID, self.__sessionDir, files=files)
        self.assertIn(" -P 3306 -S /tmp/mysql.sock <", cmds[1])
        self.assertIn("ERROR found during the database loading", out)

    # ------------------------------------------------------------- doDataLoadingBcp

    def testBcpNoModel(self) -> None:
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoadingBcp, DEPID, self.__sessionDir)
        self.assertIn("No any cif file found", out)
        self.assertEqual(cmds, [])

    def testBcpNoFileList(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoadingBcp, DEPID, self.__sessionDir)
        self.assertIn("-map /schema/da_internal.cif -db da_internal -bcp >& db-loader.log", cmds[0])
        self.assertIn("Creating", out)
        self.assertIn("No cif file found", out)

    def testBcpNoLoadFile(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoadingBcp, DEPID, self.__sessionDir, files={"FILELIST": "x"})
        self.assertEqual(len(cmds), 1)
        self.assertIn('didn\'t generate the data file "DB_LOADER_LOAD.sql"', out)

    def testBcpSuccess(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER_LOAD.sql": "load;", "data_loading.log": "Error: something\n"}
        _, out, cmds = self.__run(api.doDataLoadingBcp, DEPID, self.__sessionDir, files=files)
        self.assertEqual(len(cmds), 2)
        d = self.__dataDir
        exp = "cd %s; mysql -u dbuser -pdbpw -h dbhost -P 3306 <%s>& %s; mysql -u dbuser -pdbpw -h dbhost -P 3306 <%s>& %s" % (
            d,
            os.path.join(d, "DB_LOADER_DELETE.sql"),
            os.path.join(d, "data_delete.log"),
            os.path.join(d, "DB_LOADER_LOAD.sql"),
            os.path.join(d, "data_loading.log"),
        )
        self.assertEqual(cmds[1], exp)
        self.assertIn("Finished the database commands", out)
        # "Error:" is not the word "ERROR"
        self.assertNotIn("ERROR found", out)

    def testBcpSocketWithErrors(self) -> None:
        self.__cfg["SITE_DB_SOCKET"] = "/tmp/mysql.sock"  # noqa: S108
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER_LOAD.sql": "load;", "data_loading.log": "error here\n"}
        _, out, cmds = self.__run(api.doDataLoadingBcp, DEPID, self.__sessionDir, files=files)
        self.assertEqual(cmds[1].count("-S /tmp/mysql.sock"), 2)
        self.assertIn("ERROR found during the database loading", out)

    # ------------------------------------------------------------- doDataLoadingByMapping

    def testByMappingNoModel(self) -> None:
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoadingByMapping, DEPID, self.__sessionDir, "/map/em.cif", "status")
        self.assertIn("No any cif file found", out)
        self.assertEqual(cmds, [])

    def testByMappingNoFileList(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        _, out, cmds = self.__run(api.doDataLoadingByMapping, DEPID, self.__sessionDir, "/map/em.cif", "status")
        self.assertIn("-list FILELIST -map /map/em.cif -db status -bcp >& db-loader.log", cmds[0])
        self.assertIn("No cif file found", out)

    def testByMappingNoLoadFile(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        _, out, _ = self.__run(api.doDataLoadingByMapping, DEPID, self.__sessionDir, "/map/em.cif", "status", files={"FILELIST": "x"})
        self.assertIn('didn\'t generate the data file "DB_LOADER_LOAD.sql"', out)

    def testByMappingSuccess(self) -> None:
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER_LOAD.sql": "load;", "data_loading.log": "ERROR\n"}
        _, out, cmds = self.__run(api.doDataLoadingByMapping, DEPID, self.__sessionDir, "/map/em.cif", "status", files=files)
        self.assertEqual(len(cmds), 2)
        self.assertNotIn("-S ", cmds[1])
        self.assertIn(os.path.join(self.__dataDir, "DB_LOADER_DELETE.sql"), cmds[1])
        self.assertIn("ERROR found during the database loading", out)

    def testByMappingSocket(self) -> None:
        self.__cfg["SITE_DB_SOCKET"] = "/tmp/mysql.sock"  # noqa: S108
        self.__makeModel()
        api = DbLoadingApi(log=self.__lfh)
        files = {"FILELIST": "x", "DB_LOADER_LOAD.sql": "load;", "data_loading.log": "fine\n"}
        _, out, cmds = self.__run(api.doDataLoadingByMapping, DEPID, self.__sessionDir, "/map/em.cif", "status", files=files)
        self.assertEqual(cmds[1].count("-S /tmp/mysql.sock"), 2)
        self.assertIn("Finished the database commands", out)

    # ------------------------------------------------------------- doLoadStatus

    def __setExp(self, sqlContent: Optional[str]) -> None:
        """Mock RcsbDpUtility.exp() to optionally create the SQL output file"""

        def exp(pth: str) -> None:
            if sqlContent is not None:
                with open(pth, "w") as ofh:
                    ofh.write(sqlContent)

        self.__mockDp.return_value.exp.side_effect = exp

    def __makePdbx(self) -> str:
        pth = os.path.join(self.__topPath, "status.cif")
        with open(pth, "w") as ofh:
            ofh.write("data_x\n")
        return pth

    def testLoadStatusNoFile(self) -> None:
        api = DbLoadingApi(log=self.__lfh)
        ret, _, cmds = self.__run(api.doLoadStatus, os.path.join(self.__topPath, "missing.cif"), self.__sessionDir)
        self.assertFalse(ret)
        self.assertEqual(cmds, [])
        self.assertIn("failing, no input cif file found", self.__lfh.getvalue())

    def testLoadStatusNoSql(self) -> None:
        pdbx = self.__makePdbx()
        self.__setExp(None)
        api = DbLoadingApi(log=self.__lfh, verbose=True)
        ret, out, cmds = self.__run(api.doLoadStatus, pdbx, self.__sessionDir)
        self.assertFalse(ret)
        self.assertEqual(cmds, [])
        self.assertIn("Creating", out)
        self.assertIn("failing, no load file created", self.__lfh.getvalue())
        self.__mockDp.assert_called_once_with(tmpPath=self.__dataDir, siteId="TEST_SITE", verbose=True, log=self.__lfh)
        dp = self.__mockDp.return_value
        dp.imp.assert_called_once_with(pdbx)
        dp.addInput.assert_any_call(name="mapping_file", value="/schema/da_internal.cif", type="file")
        dp.addInput.assert_any_call(name="first_block", value=True)
        dp.op.assert_called_once_with("db-loader")
        dp.expLog.assert_called_once_with("db-loader.log")
        dp.exp.assert_called_once_with(os.path.join(self.__dataDir, "DB_LOADER.sql"))
        dp.cleanup.assert_called_once_with()

    def testLoadStatusDpException(self) -> None:
        pdbx = self.__makePdbx()
        self.__mockDp.side_effect = RuntimeError("no dp")
        api = DbLoadingApi(log=self.__lfh)
        ret, _, _ = self.__run(api.doLoadStatus, pdbx, self.__sessionDir)
        self.assertFalse(ret)
        self.assertIn("__generateLoadDb(): failing, with exception", self.__lfh.getvalue())

    def testLoadStatusSuccess(self) -> None:
        pdbx = self.__makePdbx()
        self.__setExp("insert;")
        api = DbLoadingApi(log=self.__lfh)
        ret, _, cmds = self.__run(api.doLoadStatus, pdbx, self.__sessionDir, files={"status_load.log": "ok\n"})
        self.assertTrue(ret)
        sqlFile = os.path.join(self.__dataDir, "DB_LOADER.sql")
        logFile = os.path.join(self.__dataDir, "status_load.log")
        self.assertEqual(cmds, ["cd %s; mysql -u dbuser -pdbpw -h dbhost -P 3306 <%s >& %s" % (self.__dataDir, sqlFile, logFile)])
        log = self.__lfh.getvalue()
        self.assertIn("database server command", log)
        self.assertIn("doLoadStatus(): completed", log)

    def testLoadStatusSocketNoLog(self) -> None:
        self.__cfg["SITE_DB_SOCKET"] = "/tmp/mysql.sock"  # noqa: S108
        pdbx = self.__makePdbx()
        self.__setExp("insert;")
        api = DbLoadingApi(log=self.__lfh)
        ret, _, cmds = self.__run(api.doLoadStatus, pdbx, self.__sessionDir)
        # Missing server log is reported but still considered success
        self.assertTrue(ret)
        self.assertIn(" -S /tmp/mysql.sock <", cmds[0])
        self.assertIn('didn\'t generate the data file "DB_LOADER.sql"', self.__lfh.getvalue())

    def testLoadStatusErrors(self) -> None:
        pdbx = self.__makePdbx()
        self.__setExp("insert;")
        api = DbLoadingApi(log=self.__lfh)
        ret, _, _ = self.__run(api.doLoadStatus, pdbx, self.__sessionDir, files={"status_load.log": "ERROR 1146 (42S02)\n"})
        self.assertFalse(ret)
        self.assertIn("ERROR found during the database loading", self.__lfh.getvalue())

    def testLoadStatusException(self) -> None:
        pdbx = self.__makePdbx()
        self.__setExp("insert;")
        api = DbLoadingApi(log=self.__lfh)
        out = io.StringIO()
        with mock.patch("wwpdb.utils.db.DbLoadingApi.os.system", side_effect=OSError("boom")), contextlib.redirect_stdout(out):
            ret = api.doLoadStatus(pdbx, self.__sessionDir)
        self.assertFalse(ret)
        self.assertIn("doLoadStatus(): failing, with exception", self.__lfh.getvalue())


if __name__ == "__main__":
    unittest.main()
