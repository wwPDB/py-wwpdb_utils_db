##
# File:    StatusHistoryExecMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for the status history execution (CLI) module.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import io
import os
import unittest
from typing import Dict, List
from unittest import mock

from wwpdb.utils.db.StatusHistoryExec import StatusHistoryExec, main

MOD = "wwpdb.utils.db.StatusHistoryExec"


class StatusHistoryExecMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = io.StringIO()
        envD: Dict[str, str] = {"SITE_DA_INTERNAL_DB_USER": "dbuser", "SITE_DA_INTERNAL_DB_PASSWORD": "dbpw"}
        ePatch = mock.patch.dict(os.environ, envD)
        ePatch.start()
        self.addCleanup(ePatch.stop)
        #
        sPatch = mock.patch(MOD + ".getSiteId", return_value="TEST_SITE")
        self.__mockGetSiteId = sPatch.start()
        self.addCleanup(sPatch.stop)
        cPatch = mock.patch(MOD + ".ConfigInfo")
        self.__mockConfigInfo = cPatch.start()
        self.addCleanup(cPatch.stop)
        cfgD = {"SITE_WEB_APPS_TOP_PATH": "/top", "SITE_WEB_APPS_TOP_SESSIONS_PATH": "/top/sessions"}
        self.__mockConfigInfo.return_value.get.side_effect = cfgD.get
        rPatch = mock.patch(MOD + ".InputRequest")
        self.__mockInputRequest = rPatch.start()
        self.addCleanup(rPatch.stop)
        uPatch = mock.patch(MOD + ".StatusHistoryUtils")
        self.__mockShu = uPatch.start()
        self.addCleanup(uPatch.stop)

    def __reqValues(self) -> Dict[str, str]:
        return {c.args[0]: c.args[1] for c in self.__mockInputRequest.return_value.setValue.call_args_list}

    def __shu(self) -> mock.MagicMock:
        return self.__mockShu.return_value  # type: ignore[no-any-return]

    def testSetup(self) -> None:
        """The request object is populated from configuration and the environment"""
        StatusHistoryExec(defSiteId="MY_SITE", sessionId=None, verbose=True, log=self.__lfh)
        self.__mockGetSiteId.assert_called_once_with(defaultSiteId="MY_SITE")
        self.__mockConfigInfo.assert_called_once_with("TEST_SITE")
        self.__mockInputRequest.assert_called_once_with({}, verbose=True, log=self.__lfh)
        vD = self.__reqValues()
        self.assertEqual(vD["TopSessionPath"], "/top/sessions")
        self.assertEqual(vD["TopPath"], "/top")
        self.assertEqual(vD["WWPDB_SITE_ID"], "TEST_SITE")
        self.assertEqual(vD["SITE_DA_INTERNAL_DB_USER"], "dbuser")
        self.assertEqual(vD["SITE_DA_INTERNAL_DB_PASSWORD"], "dbpw")
        self.assertNotIn("sessionid", vD)
        self.assertEqual(os.environ["WWPDB_SITE_ID"], "TEST_SITE")
        self.__mockInputRequest.return_value.newSessionObj.assert_called_once_with()
        self.__mockInputRequest.return_value.printIt.assert_called_once_with(ofh=self.__lfh)

    def testSetupSessionId(self) -> None:
        StatusHistoryExec(sessionId="abc123", log=self.__lfh)
        self.assertEqual(self.__reqValues()["sessionid"], "abc123")

    def testSetupMissingEnvironment(self) -> None:
        """Database credentials must be present in the environment"""
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(KeyError):
            StatusHistoryExec(log=self.__lfh)

    def testCreateStatusHistory(self) -> None:
        self.__shu().getEntryIdList.return_value = ["D_1", "D_2", "D_3"]
        self.__shu().createHistory.return_value = ["D_1", "D_3"]
        crx = StatusHistoryExec(verbose=False, log=self.__lfh)
        crx.doCreateStatusHistory(overWrite=True)
        self.__mockShu.assert_called_with(reqObj=self.__mockInputRequest.return_value, verbose=False, log=self.__lfh)
        self.__shu().createHistory.assert_called_once_with(["D_1", "D_2", "D_3"], overWrite=True)
        self.__shu().createHistoryMulti.assert_not_called()
        self.assertIn("doCreateStatusHistory() 2 status files created", self.__lfh.getvalue())

    def testCreateStatusHistoryMulti(self) -> None:
        self.__shu().getEntryIdList.return_value = ["D_1", "D_2"]
        self.__shu().createHistoryMulti.return_value = ["D_1", "D_2"]
        crx = StatusHistoryExec(log=self.__lfh)
        crx.doCreateStatusHistory(numProc=4)
        self.__shu().createHistoryMulti.assert_called_once_with(["D_1", "D_2"], numProc=4, overWrite=False)
        self.__shu().createHistory.assert_not_called()
        self.assertIn("2 status files created", self.__lfh.getvalue())

    def testCreateStatusHistoryFailure(self) -> None:
        self.__shu().getEntryIdList.side_effect = RuntimeError("scan failed")
        crx = StatusHistoryExec(log=self.__lfh)
        crx.doCreateStatusHistory()
        out = self.__lfh.getvalue()
        self.assertIn("RuntimeError: scan failed", out)
        self.assertNotIn("status files created", out)

    def testLoadStatusHistory(self) -> None:
        self.__shu().loadStatusHistory.return_value = True
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertTrue(crx.doLoadStatusHistory(newTable=True))
        self.__shu().loadStatusHistory.assert_called_once_with(newTable=True)
        self.__shu().loadStatusHistoryMulti.assert_not_called()

    def testLoadStatusHistoryMulti(self) -> None:
        self.__shu().loadStatusHistoryMulti.return_value = True
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertTrue(crx.doLoadStatusHistory(numProc=3))
        self.__shu().loadStatusHistoryMulti.assert_called_once_with(3, newTable=False)
        self.__shu().loadStatusHistory.assert_not_called()

    def testLoadStatusHistoryFailure(self) -> None:
        self.__shu().loadStatusHistory.side_effect = RuntimeError("load failed")
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertFalse(crx.doLoadStatusHistory())
        self.assertIn("RuntimeError: load failed", self.__lfh.getvalue())

    def testLoadEntryStatusHistory(self) -> None:
        self.__shu().loadEntryStatusHistory.return_value = True
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertTrue(crx.doLoadEntryStatusHistory("D_1000000001"))
        self.__shu().loadEntryStatusHistory.assert_called_once_with(entryIdList=["D_1000000001"])

    def testLoadEntryStatusHistoryFailure(self) -> None:
        self.__mockShu.side_effect = RuntimeError("no utils")
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertFalse(crx.doLoadEntryStatusHistory("D_1000000001"))
        self.assertIn("RuntimeError: no utils", self.__lfh.getvalue())

    def testCreateEntryStatusHistory(self) -> None:
        self.__shu().createHistory.return_value = ["D_1000000001"]
        crx = StatusHistoryExec(log=self.__lfh)
        crx.doCreateEntryStatusHistory("D_1000000001", overWrite=True)
        self.__shu().createHistory.assert_called_once_with(["D_1000000001"], overWrite=True)
        self.assertIn("doCreateEntryStatusHistory() 1 status files created", self.__lfh.getvalue())

    def testCreateEntryStatusHistoryFailure(self) -> None:
        self.__shu().createHistory.side_effect = RuntimeError("create failed")
        crx = StatusHistoryExec(log=self.__lfh)
        crx.doCreateEntryStatusHistory("D_1000000001")
        out = self.__lfh.getvalue()
        self.assertIn("RuntimeError: create failed", out)
        self.assertNotIn("status files created", out)

    def testCreateStatusHistorySchema(self) -> None:
        self.__shu().createStatusHistorySchema.return_value = True
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertTrue(crx.doCreateStatusHistorySchema())
        self.__shu().createStatusHistorySchema.assert_called_once_with()

    def testCreateStatusHistorySchemaFailure(self) -> None:
        self.__shu().createStatusHistorySchema.side_effect = RuntimeError("schema failed")
        crx = StatusHistoryExec(log=self.__lfh)
        self.assertFalse(crx.doCreateStatusHistorySchema())
        self.assertIn("RuntimeError: schema failed", self.__lfh.getvalue())

    # --- Command line interface --

    def __runMain(self, argv: List[str]) -> mock.MagicMock:
        """Run main() with the given arguments with the execution class replaced by a mock"""
        with mock.patch(MOD + ".StatusHistoryExec") as mockExec, mock.patch("sys.argv", ["StatusHistoryExec.py"] + argv):  # noqa: RUF005
            main()
        mockExec.assert_called_once_with(defSiteId="WWPDB_DEPLOY_MACOSX", sessionId=None, verbose="-v" in argv, log=mock.ANY)
        return mockExec.return_value  # type: ignore[no-any-return]

    def testMainNoOptions(self) -> None:
        crx = self.__runMain([])
        self.assertEqual(crx.method_calls, [])

    def testMainCreateEntry(self) -> None:
        crx = self.__runMain(["--createentry", "D_1000000001", "--overwrite"])
        crx.doCreateEntryStatusHistory.assert_called_once_with("D_1000000001", overWrite=True)
        crx.doCreateStatusHistory.assert_not_called()
        crx.doLoadStatusHistory.assert_not_called()

    def testMainLoadEntry(self) -> None:
        """Loading a single entry disables batch operations"""
        crx = self.__runMain(["--loadentry", "D_1000000001", "--create", "--load", "-v"])
        self.assertEqual(crx.method_calls, [mock.call.doLoadEntryStatusHistory("D_1000000001")])

    def testMainBatchCreateAndLoad(self) -> None:
        crx = self.__runMain(["--create", "--load", "--newtable", "--numproc", "4"])
        self.assertEqual(
            crx.method_calls,
            [mock.call.doCreateStatusHistory(4, overWrite=False), mock.call.doLoadStatusHistory(4, newTable=True)],
        )

    def testMainNewTableOnly(self) -> None:
        """--newtable without --load recreates the schema"""
        crx = self.__runMain(["--newtable"])
        self.assertEqual(crx.method_calls, [mock.call.doCreateStatusHistorySchema()])

    def testMainEndToEnd(self) -> None:
        """main() drives the status history utilities"""
        self.__shu().loadStatusHistory.return_value = True
        with mock.patch("sys.argv", ["StatusHistoryExec.py", "--load"]), mock.patch("sys.stderr", self.__lfh):
            main()
        self.__mockGetSiteId.assert_called_once_with(defaultSiteId="WWPDB_DEPLOY_MACOSX")
        self.__shu().loadStatusHistory.assert_called_once_with(newTable=False)


if __name__ == "__main__":
    unittest.main()
