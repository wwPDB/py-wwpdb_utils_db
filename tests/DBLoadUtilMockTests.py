##
# File:    DBLoadUtilMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Tests for DBLoadUtil with mocked request/session objects, site configuration, RcsbDpUtility and shell commands.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import io
import os
import shutil
import sys
import tempfile
import unittest
from typing import Dict, Optional
from unittest import mock

if __package__ is None or __package__ == "":  # noqa: PLC1901
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mock_import import mocksetup  # type: ignore  # noqa: F401 pylint: disable=unused-import,import-error
else:
    from .mock_import import mocksetup  # noqa: F401,TID252 pylint: disable=unused-import,import-error

from wwpdb.utils.db.DBLoadUtil import DBLoadUtil


class DBLoadUtilMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__sessionPath = tempfile.mkdtemp()
        self.__lfh = io.StringIO()
        self.__reqObj = mock.MagicMock()
        self.__reqObj.getValue.return_value = "TEST_SITE"
        self.__reqObj.newSessionObj.return_value.getId.return_value = "sess-1234"
        self.__reqObj.newSessionObj.return_value.getPath.return_value = self.__sessionPath
        self.__cfg: Dict[str, Optional[str]] = {
            "SITE_DB_HOST_NAME": "dbhost",
            "SITE_DB_USER_NAME": "dbuser",
            "SITE_DB_PASSWORD": "dbpw",
            "SITE_DB_PORT_NUMBER": "3306",
        }
        self.__patchers = [
            mock.patch("wwpdb.utils.db.DBLoadUtil.ConfigInfo"),
            mock.patch("wwpdb.utils.db.DBLoadUtil.ConfigInfoAppCommon"),
            mock.patch("wwpdb.utils.db.DBLoadUtil.RcsbDpUtility"),
            mock.patch("wwpdb.utils.db.DBLoadUtil.os.system", return_value=0),
        ]
        self.__mockCi, self.__mockCiCommon, self.__mockDp, self.__mockSystem = [p.start() for p in self.__patchers]
        self.__mockCi.return_value.get.side_effect = lambda k: self.__cfg[k]
        self.__mockCiCommon.return_value.get_site_da_internal_schema_path.return_value = "/schema/da_internal.cif"
        self.__sqlFile = os.path.join(self.__sessionPath, "dbload", "DB_LOADER.sql")
        self.__clogFile = os.path.join(self.__sessionPath, "dbload", "sqlload.log")

    def tearDown(self) -> None:
        for p in self.__patchers:
            p.stop()
        shutil.rmtree(self.__sessionPath, ignore_errors=True)

    def __produceSql(self) -> None:
        def exp(pth: str) -> None:
            os.makedirs(os.path.dirname(pth), exist_ok=True)
            with open(pth, "w") as ofh:
                ofh.write("insert;\n")

        self.__mockDp.return_value.exp.side_effect = exp

    def testConstructor(self) -> None:
        DBLoadUtil(reqObj=self.__reqObj, verbose=True, log=self.__lfh)
        self.__reqObj.getValue.assert_called_once_with("WWPDB_SITE_ID")
        self.__mockCi.assert_called_once_with("TEST_SITE")
        self.__mockCiCommon.assert_called_once_with("TEST_SITE")
        self.__reqObj.newSessionObj.assert_called_once_with()
        log = self.__lfh.getvalue()
        self.assertIn("creating/joining session sess-1234", log)
        self.assertIn("session path %s" % self.__sessionPath, log)

    def testConstructorQuiet(self) -> None:
        DBLoadUtil(reqObj=self.__reqObj, verbose=False, log=self.__lfh)
        self.assertEqual(self.__lfh.getvalue(), "")

    def testNoFiles(self) -> None:
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading([])
        self.__mockDp.assert_not_called()
        self.__mockSystem.assert_not_called()
        self.assertEqual(os.listdir(self.__sessionPath), [])

    def testLoadingNoSql(self) -> None:
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif", "/b/D_2_model.cif"])
        listFile = os.path.join(self.__sessionPath, "filelist_1.txt")
        with open(listFile) as ifh:
            self.assertEqual(ifh.read(), "/a/D_1_model.cif\n/b/D_2_model.cif\n")
        self.__mockDp.assert_called_once_with(tmpPath=self.__sessionPath, siteId="TEST_SITE", verbose=False, log=self.__lfh)
        dp = self.__mockDp.return_value
        dp.setDebugMode.assert_called_once_with()
        dp.imp.assert_called_once_with(listFile)
        dp.addInput.assert_any_call(name="mapping_file", value="/schema/da_internal.cif", type="file")
        dp.addInput.assert_any_call(name="file_list", value=True)
        dp.addInput.assert_any_call(name="first_block", value=True)
        dp.op.assert_called_once_with("db-loader")
        dp.expLog.assert_called_once_with(os.path.join(self.__sessionPath, "dbload", "db-loader.log"))
        dp.exp.assert_called_once_with(self.__sqlFile)
        dp.cleanup.assert_called_once_with()
        self.__mockSystem.assert_not_called()
        self.assertIn("failed to produce load file", self.__lfh.getvalue())

    def testLoadingSuccess(self) -> None:
        self.__produceSql()
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif"])
        self.__mockSystem.assert_called_once_with(
            "cd %s; mysql -u dbuser -pdbpw -h dbhost -P 3306 < %s >& %s" % (self.__sessionPath, self.__sqlFile, self.__clogFile)
        )
        self.assertIn("about to load %s with log %s" % (self.__sqlFile, self.__clogFile), self.__lfh.getvalue())

    def testLoadingIntegerPort(self) -> None:
        self.__produceSql()
        self.__mockCi.return_value.get.side_effect = lambda k: 3307 if k == "SITE_DB_PORT_NUMBER" else self.__cfg[k]
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif"])
        self.assertIn(" -P 3307 < ", self.__mockSystem.call_args.args[0])

    def testLoadingDbException(self) -> None:
        """Missing configuration (None) causes a failure that is caught and logged"""
        self.__produceSql()
        self.__cfg["SITE_DB_PASSWORD"] = None
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif"])
        self.__mockSystem.assert_not_called()
        self.assertIn("DbLoadiUtil::doLoading(): failing, with exception", self.__lfh.getvalue())

    def testLoadingDpException(self) -> None:
        self.__mockDp.side_effect = RuntimeError("dp failure")
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif"])
        log = self.__lfh.getvalue()
        self.assertIn("DbLoadUtil::__getLoadFile(): failing, with exception", log)
        self.assertIn("RuntimeError: dp failure", log)
        self.assertIn("failed to produce load file", log)

    @unittest.expectedFailure
    def testUniqueListFileName(self) -> None:
        """BUG: DBLoadUtil.__getFileName() returns inside the loop, so an existing filelist_1.txt is always
        reused (overwritten) rather than a unique filelist_N.txt name being chosen."""
        listFile1 = os.path.join(self.__sessionPath, "filelist_1.txt")
        with open(listFile1, "w") as ofh:
            ofh.write("previous\n")
        dbl = DBLoadUtil(reqObj=self.__reqObj, log=self.__lfh)
        dbl.doLoading(["/a/D_1_model.cif"])
        with open(listFile1) as ifh:
            self.assertEqual(ifh.read(), "previous\n")
        self.assertTrue(os.path.exists(os.path.join(self.__sessionPath, "filelist_2.txt")))


if __name__ == "__main__":
    unittest.main()
