##
# File:    StatusHistoryUtilsMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for status history utilities (no database server or site configuration required).

Site configuration, path resolution, model file access, multiprocessing and database access are replaced by mocks.
Status history files are created in a temporary archive directory with the real StatusHistory class.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import io
import logging
import os
import shutil
import tempfile
import types
import unittest
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

from wwpdb.utils.db.StatusHistory import StatusHistory
from wwpdb.utils.db.StatusHistoryUtils import StatusHistoryUtils

MOD = "wwpdb.utils.db.StatusHistoryUtils"
ENTRY_ID = "D_1000000001"
PDB_ID = "1ABC"

DEP_DATE = "2015-01-01"
PROC_DATE = "2015-01-03"
APPROVAL_DATE = "2015-01-20"
RELEASE_DATE = "2015-02-11"
ANNOT_TS = "2015-01-10:12:00"
CURRENT_TS = "2015-03-01:08:30"
DEPOSIT_TS = "2015-02-15:10:00"

StatusTuple = Tuple[str, str, str, str, str, str, str, str, str]


def statusDetails(statusCode: str, initialDepositionDate: str = DEP_DATE, beginProcessingDate: str = PROC_DATE) -> StatusTuple:
    return (ENTRY_ID, PDB_ID, statusCode, "HPUB", "EP", initialDepositionDate, beginProcessingDate, APPROVAL_DATE, RELEASE_DATE)


class StatusHistoryUtilsMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__tmpDir = tempfile.mkdtemp()
        self.__archDir = os.path.join(self.__tmpDir, "archive")
        self.__sessionDir = os.path.join(self.__tmpDir, "session")
        os.makedirs(self.__sessionDir)
        os.makedirs(os.path.join(self.__archDir, ENTRY_ID))
        self.__lfh = io.StringIO()
        # model file path -> modification time stamp
        self.__modelTs: Dict[str, str] = {}
        #
        self.__reqObj = mock.MagicMock()
        self.__reqObj.getSessionObj.return_value.getPath.return_value = self.__sessionDir
        self.__reqObj.getValue.side_effect = {"WWPDB_SITE_ID": "TEST_SITE"}.get
        #
        cfgD: Dict[str, Any] = {
            "SITE_ARCHIVE_STORAGE_PATH": self.__tmpDir,
            "SITE_DA_INTERNAL_DB_NAME": "da_internal",
            "SITE_DA_INTERNAL_DB_HOST_NAME": "localhost",
            "SITE_DA_INTERNAL_DB_PORT_NUMBER": "3306",
            "SITE_DA_INTERNAL_DB_SOCKET": None,
            "SITE_DA_INTERNAL_DB_USER_NAME": "user",
            "SITE_DA_INTERNAL_DB_PASSWORD": "pw",
        }
        self.__mockConfigInfo = self.__patch(MOD + ".ConfigInfo")
        self.__mockConfigInfo.return_value.get.side_effect = cfgD.get
        mockBaseConfigInfo = self.__patch("wwpdb.utils.db.MyConnectionBase.ConfigInfo")
        mockBaseConfigInfo.return_value.get.side_effect = cfgD.get
        self.__dbCon = mock.MagicMock(name="dbCon")
        self.__mockGetConnection = self.__patch("wwpdb.utils.db.MyConnectionBase.getConnection", return_value=self.__dbCon)
        #
        for pathInfoName in [MOD + ".PathInfo", "wwpdb.utils.db.StatusHistory.PathInfo"]:
            mockPathInfo = self.__patch(pathInfoName)
            mockPathInfo.return_value.getStatusHistoryFilePath.side_effect = self.__historyPath
            mockPathInfo.return_value.getModelPdbxFilePath.side_effect = self.__modelPath
        self.__mockDataFile = self.__patch(MOD + ".DataFile", side_effect=self.__dataFile)
        self.__mockEntryInfo = self.__patch(MOD + ".PdbxEntryInfoIo")
        self.__mockIoAdapter = self.__patch(MOD + ".IoAdapterCore")
        self.__mockMpu = self.__patch(MOD + ".MultiProcUtil")
        self.__mockSdl = self.__patch(MOD + ".SchemaDefLoader")
        self.__mockMyDbQuery = self.__patch(MOD + ".MyDbQuery")
        self.__mockMyDbQuery.return_value.sqlCommand.return_value = True
        self.__mockMyDbConnect = self.__patch(MOD + ".MyDbConnect")
        # The pure Python fallback of scandir.walk() (used when the scandir C extension is unavailable, e.g. on newer
        # Python versions) recurses without bound for bottom-up walks -- use the equivalent os.walk() in its place.
        self.__patch(MOD + ".scandir", new=types.SimpleNamespace(walk=os.walk))

    def tearDown(self) -> None:
        shutil.rmtree(self.__tmpDir, ignore_errors=True)

    # ---- helpers --

    def __patch(self, target: str, **kwargs: Any) -> mock.MagicMock:
        patcher = mock.patch(target, **kwargs)
        mObj = patcher.start()
        self.addCleanup(patcher.stop)
        return mObj  # type: ignore[no-any-return]

    def __historyPath(self, dataSetId: str, fileSource: str = "archive", versionId: str = "latest") -> str:  # noqa: ARG002 pylint: disable=unused-argument
        return os.path.join(self.__archDir, dataSetId, dataSetId + "_status-history_P1.cif.V1")

    def __modelPath(  # pylint: disable=unused-argument
        self,
        dataSetId: str,
        wfInstanceId: Optional[str] = None,  # noqa: ARG002
        fileSource: str = "archive",  # noqa: ARG002
        versionId: str = "latest",
        mileStone: Optional[str] = None,
    ) -> str:
        msS = "-" + mileStone if mileStone else ""
        return os.path.join(self.__archDir, dataSetId, "%s_model%s_P1.cif.V%s" % (dataSetId, msS, versionId))

    def __dataFile(self, fPath: str) -> mock.MagicMock:
        df = mock.MagicMock()
        df.srcFileExists.return_value = fPath in self.__modelTs
        df.srcModTimeStamp.return_value = self.__modelTs.get(fPath)
        return df

    def __setModelTimeStamps(self, current: Optional[str] = CURRENT_TS, annotate: Optional[str] = ANNOT_TS, deposit: Optional[str] = DEPOSIT_TS) -> None:
        for ts, versionId, mileStone in [(current, "latest", None), (annotate, "1", "annotate"), (deposit, "latest", "deposit")]:
            if ts is not None:
                self.__modelTs[self.__modelPath(ENTRY_ID, versionId=versionId, mileStone=mileStone)] = ts

    def __setStatus(self, details: StatusTuple) -> None:
        self.__mockEntryInfo.return_value.getCurrentStatusDetails.return_value = details

    def __newUtils(self, verbose: bool = True) -> StatusHistoryUtils:
        return StatusHistoryUtils(reqObj=self.__reqObj, verbose=verbose, log=self.__lfh)

    def __readHistory(self, entryId: str = ENTRY_ID) -> List[Dict[str, Optional[str]]]:
        sH = StatusHistory(siteId="TEST_SITE", verbose=False, log=self.__lfh)
        sH.setEntryId(entryId, PDB_ID)
        dL: List[Dict[str, Optional[str]]] = sH.get()
        return dL

    def __transitions(self, entryId: str = ENTRY_ID) -> List[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]]:
        return [(d["status_code_begin"], d["date_begin"], d["status_code_end"], d["date_end"]) for d in self.__readHistory(entryId)]

    def __writeHistory(self, rowL: List[Tuple[str, str, str, str]]) -> None:
        sH = StatusHistory(siteId="TEST_SITE", verbose=False, log=self.__lfh)
        sH.setEntryId(ENTRY_ID, PDB_ID)
        for row in rowL:
            self.assertTrue(sH.add(*row, annotator="JW"))
        self.assertTrue(sH.store(ENTRY_ID))

    # ---- path utilities --

    def testConstruction(self) -> None:
        self.__newUtils()
        self.__mockConfigInfo.assert_called_once_with("TEST_SITE")
        self.__mockIoAdapter.assert_called_once_with(verbose=True, log=self.__lfh)
        self.__reqObj.getValue.assert_called_with("WWPDB_SITE_ID")

    def testEntryPathLists(self) -> None:
        """Entry directories in the archive are identified by name"""
        for dirName in ["D_1000000002", "D_100", "X_1000000003", os.path.join("D_1000000004", "nested"), os.path.join("sub", "D_1000000005")]:
            os.makedirs(os.path.join(self.__archDir, dirName))
        shu = self.__newUtils()
        self.assertEqual(sorted(shu.getEntryIdList()), [ENTRY_ID, "D_1000000002", "D_1000000004", "D_1000000005"])
        # Only existing history files are returned
        self.assertEqual(shu.getStatusHistoryPathList(), [])
        self.assertEqual(shu.getEntryStatusHistoryPathList([ENTRY_ID, "D_1000000002"]), [])
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        hPath = self.__historyPath(ENTRY_ID)
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.getStatusHistoryPathList(), [hPath])
        self.assertIn("search archive path", cm.output[0])
        self.assertEqual(shu.getEntryStatusHistoryPathList([ENTRY_ID, "D_1000000002"]), [hPath])
        self.assertEqual(shu.getEntryStatusHistoryPathList([]), [])

    def testEntryIdListMissingArchive(self) -> None:
        shutil.rmtree(self.__archDir)
        shu = self.__newUtils(verbose=False)
        self.assertEqual(shu.getEntryIdList(), [])
        self.assertEqual(shu.getStatusHistoryPathList(), [])

    # ---- history file creation --

    def testCreateHistoryReleased(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertTrue(any("write status history succeeded" in msg for msg in cm.output))
        self.assertEqual(
            self.__transitions(),
            [
                ("PROC", "2015-01-01:00:00", "PROC_ST_1", "2015-01-03:00:00"),
                ("PROC_ST_1", "2015-01-03:00:00", "AUTH", ANNOT_TS),
                ("PROC", "2015-01-01:00:00", "AUTH", ANNOT_TS),
                ("AUTH", ANNOT_TS, "REL", "2015-02-11:00:00"),
            ],
        )
        dL = self.__readHistory()
        self.assertEqual([d["annotator"] for d in dL], ["EP"] * 4)
        self.assertEqual(dL[0]["details"], "Automated initial entry")
        self.assertEqual(dL[0]["pdb_id"], PDB_ID)
        # Model file read from the archive
        self.__mockEntryInfo.return_value.setFilePath.assert_called_once_with(filePath=self.__modelPath(ENTRY_ID), idCode=ENTRY_ID)

    def testCreateHistoryHold(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("HPUB"))
        shu = self.__newUtils(verbose=False)
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        tL = self.__transitions()
        self.assertEqual(len(tL), 4)
        self.assertEqual(tL[3], ("AUTH", ANNOT_TS, "HPUB", "2015-01-20:00:00"))

    def testCreateHistoryReplaced(self) -> None:
        """AUCO/REPL use the deposit milestone model file time stamp"""
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REPL"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(self.__transitions()[3], ("AUTH", ANNOT_TS, "REPL", DEPOSIT_TS))

    def testCreateHistoryReplacedNoDepositFile(self) -> None:
        self.__setModelTimeStamps(deposit=None)
        self.__setStatus(statusDetails("AUCO"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(self.__transitions()[3], ("AUTH", ANNOT_TS, "AUCO", CURRENT_TS))

    def testCreateHistoryOtherStatus(self) -> None:
        """Other status codes create only the initial three records"""
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("AUTH"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(len(self.__transitions()), 3)

    def testCreateHistoryWait(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("WAIT"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(
            self.__transitions(),
            [
                ("PROC", "2015-01-01:00:00", "PROC_ST_1", "2015-01-03:00:00"),
                ("PROC_ST_1", "2015-01-03:00:00", "WAIT", ANNOT_TS),
                ("PROC", "2015-01-01:00:00", "WAIT", ANNOT_TS),
            ],
        )

    def testCreateHistoryProcessing(self) -> None:
        """Entries still in processing are skipped"""
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("PROC"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertTrue(any("skipping entry with current status code is 'PROC'" in msg for msg in cm.output))
        self.assertFalse(os.path.exists(self.__historyPath(ENTRY_ID)))

    def testCreateHistoryAuthWait(self) -> None:
        """statusUpdateAuthWait substitutes the processing status for a new history"""
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("PROC"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID], statusUpdateAuthWait="AUTH"), [ENTRY_ID])
        tL = self.__transitions()
        self.assertEqual(len(tL), 3)
        self.assertEqual(tL[1][2], "AUTH")
        # end of first step is the current time
        self.assertGreater(str(tL[1][3]), "2026-01-01")
        #
        os.remove(self.__historyPath(ENTRY_ID))
        self.assertEqual(shu.createHistory([ENTRY_ID], statusUpdateAuthWait="WAIT"), [ENTRY_ID])
        self.assertEqual([t[2] for t in self.__transitions()], ["PROC_ST_1", "WAIT", "WAIT"])

    def testCreateHistoryNoModel(self) -> None:
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID, "D_1000000002"]), [])
        self.assertTrue(any("no model file in file source archive" in msg for msg in cm.output))
        self.__mockEntryInfo.assert_not_called()

    def testCreateHistoryModelTimeStampFailure(self) -> None:
        """A failure reading the model file time stamp is logged and treated as a missing file"""
        self.__setStatus(statusDetails("REL"))
        self.__mockDataFile.side_effect = OSError("stat failed")
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="ERROR") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertIn("getModelFileTimeStamp failing", cm.output[0])

    def testCreateHistoryBadDepositionDate(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL", initialDepositionDate="?"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertTrue(any("missing initial deposition date" in msg for msg in cm.output))

    def testCreateHistoryNoProcessingDates(self) -> None:
        self.__setModelTimeStamps(current="not-a-date", annotate=None)
        self.__setStatus(statusDetails("REL", beginProcessingDate="?"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertTrue(any("missing beginning processing date" in msg for msg in cm.output))

    def testCreateHistoryMissingProcessingDate(self) -> None:
        """The annotate milestone time stamp substitutes for a missing begin processing date"""
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL", beginProcessingDate="?"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(self.__transitions()[:2], [("PROC", "2015-01-01:00:00", "PROC_ST_1", ANNOT_TS), ("PROC_ST_1", ANNOT_TS, "AUTH", ANNOT_TS)])

    def testCreateHistoryMissingAnnotateDate(self) -> None:
        """The begin processing date substitutes for an invalid annotate milestone time stamp"""
        self.__setModelTimeStamps(annotate="bad")
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils()
        self.assertEqual(shu.createHistory([ENTRY_ID]), [ENTRY_ID])
        self.assertEqual(self.__transitions()[1], ("PROC_ST_1", "2015-01-03:00:00", "AUTH", "2015-01-03:00:00"))

    def testCreateHistoryExisting(self) -> None:
        """Existing history files are kept unless overWrite is set"""
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertTrue(any("found existing status history category with row count 1" in msg for msg in cm.output))
        self.assertEqual(len(self.__transitions()), 1)
        #
        self.assertEqual(shu.createHistory([ENTRY_ID], overWrite=True), [ENTRY_ID])
        self.assertEqual(len(self.__transitions()), 4)

    def testCreateHistoryStoreFailure(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils()
        with mock.patch.object(StatusHistory, "store", return_value=False), self.assertLogs(MOD, level="INFO") as cm:
            self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertTrue(any("NO status history file written" in msg for msg in cm.output))

    def testCreateHistoryException(self) -> None:
        self.__setModelTimeStamps()
        self.__mockEntryInfo.return_value.getCurrentStatusDetails.side_effect = ValueError("bad model file")
        shu = self.__newUtils(verbose=False)
        self.assertEqual(shu.createHistory([ENTRY_ID]), [])
        self.assertIn("ValueError: bad model file", self.__lfh.getvalue())

    def testCreateHistoryWorker(self) -> None:
        self.__setModelTimeStamps()
        self.__setStatus(statusDetails("REL"))
        shu = self.__newUtils(verbose=False)
        self.assertEqual(shu.createHistoryWorker([ENTRY_ID, "D_1000000002"], "proc1", {}, self.__sessionDir), ([ENTRY_ID], [ENTRY_ID], []))
        self.assertEqual(len(self.__transitions()), 4)
        # existing file is retained without overWrite
        self.assertEqual(shu.createHistoryWorker([ENTRY_ID], "proc1", {"overWrite": False}, self.__sessionDir), ([], [], []))
        self.assertEqual(shu.createHistoryWorker([ENTRY_ID], "proc1", {"overWrite": True}, self.__sessionDir), ([ENTRY_ID], [ENTRY_ID], []))

    def testCreateHistoryMulti(self) -> None:
        mpu = self.__mockMpu.return_value
        mpu.runMulti.return_value = (True, [], [[ENTRY_ID]], [])
        shu = self.__newUtils()
        self.assertEqual(shu.createHistoryMulti([ENTRY_ID, "D_1000000002"], numProc=3, overWrite=True), [ENTRY_ID])
        mpu.set.assert_called_once_with(workerObj=shu, workerMethod="createHistoryWorker")
        mpu.setOptions.assert_called_once_with(optionsD={"overWrite": True})
        mpu.setWorkingDir.assert_called_once_with(self.__sessionDir)
        mpu.runMulti.assert_called_once_with(dataList=[ENTRY_ID, "D_1000000002"], numProc=3, numResults=1)

    # ---- database loading --

    def testLoadBatchFilesWorker(self) -> None:
        authD = {"DB_NAME": "da_internal"}
        tL = [("pdbx_database_status_history", "/tmp/x.tdd")]
        shu = self.__newUtils()
        self.assertEqual(shu.loadBatchFilesWorker(tL, "proc1", authD, self.__sessionDir), (tL, tL, []))
        myC = self.__mockMyDbConnect.return_value
        myC.setAuth.assert_called_once_with(authD)
        myC.connect.assert_called_once_with()
        myC.close.assert_called_once_with()
        _args, kwargs = self.__mockSdl.call_args
        self.assertIs(kwargs["dbCon"], myC.connect.return_value)
        self.assertEqual(kwargs["workPath"], self.__sessionDir)
        self.assertIs(kwargs["ioObj"], self.__mockIoAdapter.return_value)
        self.__mockSdl.return_value.loadBatchFiles.assert_called_once_with(loadList=tL, containerNameList=None, deleteOpt=None)

    def testLoadStatusHistoryMultiEmpty(self) -> None:
        shu = self.__newUtils()
        self.assertTrue(shu.loadStatusHistoryMulti(numProc=2))
        self.__mockMpu.assert_not_called()
        self.__mockGetConnection.assert_not_called()

    def __setupMultiLoad(self) -> Tuple[mock.MagicMock, mock.MagicMock, List[Tuple[str, str]]]:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        tL = [("PDBX_DATABASE_STATUS_HISTORY", os.path.join(self.__sessionDir, "status.tdd"))]
        mpu1 = mock.MagicMock(name="mpu1")
        mpu1.runMulti.return_value = (True, [], [[ENTRY_ID], tL], [])
        mpu2 = mock.MagicMock(name="mpu2")
        mpu2.runMulti.return_value = (True, [], [tL], [])
        self.__mockMpu.side_effect = [mpu1, mpu2]
        return mpu1, mpu2, tL

    def testLoadStatusHistoryMulti(self) -> None:
        mpu1, mpu2, tL = self.__setupMultiLoad()
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertTrue(shu.loadStatusHistoryMulti(numProc=2, newTable=False))
        self.assertTrue(any("Created table PDBX_DATABASE_STATUS_HISTORY load file" in msg for msg in cm.output))
        # Load file creation
        mpu1.set.assert_called_once_with(workerObj=self.__mockSdl.return_value, workerMethod="makeLoadFilesMulti")
        mpu1.runMulti.assert_called_once_with(dataList=[self.__historyPath(ENTRY_ID)], numProc=2, numResults=2)
        # Existing rows are deleted rather than the table recreated
        self.__mockSdl.return_value.delete.assert_called_once_with("PDBX_DATABASE_STATUS_HISTORY", containerNameList=[ENTRY_ID], deleteOpt="selected")
        self.__mockMyDbQuery.return_value.sqlCommand.assert_not_called()
        self.__dbCon.close.assert_called_once_with()
        # Loading uses the connection authentication details
        mpu2.set.assert_called_once_with(workerObj=shu, workerMethod="loadBatchFilesWorker")
        authD = mpu2.setOptions.call_args[0][0]
        self.assertEqual(authD["DB_NAME"], "da_internal")
        self.assertEqual(authD["DB_USER"], "user")
        self.assertEqual(authD["DB_PORT"], 3306)
        mpu2.runMulti.assert_called_once_with(dataList=tL, numProc=2, numResults=1)

    def testLoadStatusHistoryMultiNewTable(self) -> None:
        _mpu1, mpu2, _tL = self.__setupMultiLoad()
        mpu2.runMulti.return_value = (False, [], [[]], [])
        shu = self.__newUtils(verbose=False)
        self.assertFalse(shu.loadStatusHistoryMulti(numProc=2, newTable=True))
        self.__mockSdl.return_value.delete.assert_not_called()
        sqlL = self.__mockMyDbQuery.return_value.sqlCommand.call_args[1]["sqlCommandList"]
        self.assertTrue(any("CREATE TABLE" in sql.upper() for sql in sqlL))
        self.__mockMyDbQuery.return_value.setWarning.assert_called_once_with("default")
        self.__mockMyDbQuery.assert_called_once_with(dbcon=self.__dbCon, verbose=False, log=self.__lfh)

    def testLoadStatusHistoryMultiFailure(self) -> None:
        self.__setupMultiLoad()
        self.__mockMpu.side_effect = RuntimeError("no processes")
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="ERROR") as cm:
            self.assertFalse(shu.loadStatusHistoryMulti())
        self.assertIn("In loading status history", cm.output[0])

    def testLoadStatusHistoryEmpty(self) -> None:
        shu = self.__newUtils()
        self.assertTrue(shu.loadStatusHistory())
        self.__mockGetConnection.assert_not_called()

    def testLoadStatusHistory(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils()
        self.assertTrue(shu.loadStatusHistory(newTable=False))
        connectKw = self.__mockGetConnection.call_args[0][0]
        self.assertEqual(connectKw["db"], "da_internal")
        self.assertEqual(connectKw["host"], "localhost")
        _args, kwargs = self.__mockSdl.call_args
        self.assertIs(kwargs["dbCon"], self.__dbCon)
        self.assertEqual(kwargs["warnings"], "error")
        self.__mockSdl.return_value.load.assert_called_once_with(
            inputPathList=[self.__historyPath(ENTRY_ID)], containerList=None, loadType="batch-file", deleteOpt="all"
        )
        self.__mockMyDbQuery.assert_not_called()
        self.__dbCon.close.assert_called_once_with()

    def testLoadStatusHistoryNewTable(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils()
        self.assertTrue(shu.loadStatusHistory(newTable=True))
        self.__mockMyDbQuery.return_value.sqlCommand.assert_called_once()
        self.__mockSdl.return_value.load.assert_called_once()

    def testLoadStatusHistoryNoConnection(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__mockGetConnection.side_effect = RuntimeError("cannot connect")
        shu = self.__newUtils()
        with self.assertLogs(level="INFO") as cm:
            self.assertFalse(shu.loadStatusHistory())
        self.assertTrue(any("Connection error" in msg for msg in cm.output))
        self.assertTrue(any("loadStatusHistory) database connection failed" in msg for msg in cm.output))
        self.__mockSdl.assert_not_called()

    def testLoadStatusHistoryFailure(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__mockSdl.return_value.load.side_effect = RuntimeError("load failed")
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="ERROR") as cm:
            self.assertFalse(shu.loadStatusHistory())
        self.assertIn("Failure in loadStatusHistory", cm.output[0])
        self.__dbCon.close.assert_called_once_with()

    def testLoadEntryStatusHistory(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils()
        self.assertTrue(shu.loadEntryStatusHistory([ENTRY_ID, "D_1000000002"]))
        self.__mockSdl.return_value.load.assert_called_once_with(
            inputPathList=[self.__historyPath(ENTRY_ID)], containerList=None, loadType="batch-insert", deleteOpt="selected"
        )
        self.__dbCon.close.assert_called_once_with()

    def testLoadEntryStatusHistoryEmpty(self) -> None:
        shu = self.__newUtils()
        self.assertTrue(shu.loadEntryStatusHistory(["D_1000000002"]))
        self.__mockGetConnection.assert_not_called()

    def testLoadEntryStatusHistoryNoConnection(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__mockGetConnection.side_effect = RuntimeError("cannot connect")
        shu = self.__newUtils()
        with self.assertLogs(level="INFO") as cm:
            self.assertFalse(shu.loadEntryStatusHistory([ENTRY_ID]))
        self.assertTrue(any("Connection error" in msg for msg in cm.output))
        self.assertTrue(any("loadEntryStatusHistory) database connection failed" in msg for msg in cm.output))

    def testLoadEntryStatusHistoryFailure(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__mockSdl.side_effect = RuntimeError("bad schema")
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="ERROR") as cm:
            self.assertFalse(shu.loadEntryStatusHistory([ENTRY_ID]))
        self.assertIn("In load entry status history", cm.output[0])
        self.__dbCon.close.assert_called_once_with()

    def testCreateStatusHistorySchema(self) -> None:
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="INFO") as cm:
            self.assertTrue(shu.createStatusHistorySchema())
        self.assertTrue(any("mysql server returns True" in msg for msg in cm.output))
        sqlL = self.__mockMyDbQuery.return_value.sqlCommand.call_args[1]["sqlCommandList"]
        sqlS = "\n".join(sqlL).lower()
        self.assertIn("pdbx_database_status_history", sqlS)
        self.assertIn("da_internal", sqlS)
        self.__dbCon.close.assert_called_once_with()

    def testCreateStatusHistorySchemaSqlFailure(self) -> None:
        """A failure creating the schema is logged"""
        self.__mockMyDbQuery.return_value.sqlCommand.side_effect = RuntimeError("sql failed")
        shu = self.__newUtils()
        with self.assertLogs(MOD, level="ERROR") as cm:
            # The connection succeeded so the overall status is True
            self.assertTrue(shu.createStatusHistorySchema())
        self.assertIn("In _schemaCreate", cm.output[0])

    def testCreateStatusHistorySchemaNoConnection(self) -> None:
        self.__mockGetConnection.side_effect = RuntimeError("cannot connect")
        shu = self.__newUtils()
        with self.assertLogs(level="INFO") as cm:
            self.assertFalse(shu.createStatusHistorySchema())
        self.assertTrue(any("Connection error" in msg for msg in cm.output))
        self.assertTrue(any("createStatusHistorySchema) database connection failed" in msg for msg in cm.output))
        self.__mockMyDbQuery.assert_not_called()

    def testCreateStatusHistorySchemaFailure(self) -> None:
        shu = self.__newUtils()
        with mock.patch(MOD + ".StatusHistorySchemaDef", side_effect=RuntimeError("no schema")), self.assertLogs(MOD, level="ERROR") as cm:
            self.assertFalse(shu.createStatusHistorySchema())
        self.assertIn("In creation of status history schema", cm.output[0])
        self.__dbCon.close.assert_called_once_with()

    # ---- status history update --

    def testUpdateSkipped(self) -> None:
        shu = self.__newUtils()
        self.assertFalse(shu.updateEntryStatusHistory(None, "HPUB", "EP"))
        self.assertFalse(shu.updateEntryStatusHistory([], "HPUB", "EP"))
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], None, "EP"))
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", None))
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "AUCO", "EP"))
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "REPL", "EP"))
        self.assertFalse(os.path.exists(self.__historyPath(ENTRY_ID)))

    def testUpdate(self) -> None:
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils()
        self.assertTrue(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", "EP", statusCodePrior="AUTH"))
        dL = self.__readHistory()
        self.assertEqual(len(dL), 2)
        self.assertEqual((dL[1]["status_code_begin"], dL[1]["date_begin"], dL[1]["status_code_end"]), ("AUTH", "2015-01-03:00:00", "HPUB"))
        self.assertEqual(dL[1]["annotator"], "EP")
        self.assertEqual(dL[1]["details"], "Update by status module")
        # Repeating the same status is not recorded
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", "EP"))
        self.assertEqual(len(self.__readHistory()), 2)

    def testUpdateMissingPriorRecord(self) -> None:
        """A prior status which is not in the history is noted in the details"""
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils(verbose=False)
        self.assertTrue(shu.updateEntryStatusHistory([ENTRY_ID], "REL", "EP", details="Released", statusCodePrior="HPUB"))
        dL = self.__readHistory()
        self.assertEqual(len(dL), 2)
        self.assertEqual(dL[1]["details"], "Released (detected status history record) ")

    def testUpdateRevisionPrior(self) -> None:
        """A missing AUCO/REPL record is inserted using the deposit milestone time stamp"""
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        self.__setModelTimeStamps()
        shu = self.__newUtils()
        self.assertTrue(shu.updateEntryStatusHistory([ENTRY_ID], "AUTH", "EP", statusCodePrior="REPL"))
        self.assertEqual(
            [(t[0], t[2]) for t in self.__transitions()],
            [("PROC", "AUTH"), ("AUTH", "REPL"), ("REPL", "AUTH")],
        )
        dL = self.__readHistory()
        self.assertEqual(dL[1]["date_end"], DEPOSIT_TS)
        self.assertEqual(dL[1]["details"], "Automated revision or correction entry")
        self.assertEqual(dL[2]["date_begin"], DEPOSIT_TS)

    def testUpdateRevisionPriorNoDepositFile(self) -> None:
        """Without a deposit milestone file no revision record is inserted"""
        self.__writeHistory([("PROC", DEP_DATE, "AUTH", PROC_DATE)])
        shu = self.__newUtils()
        self.assertTrue(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", "EP", statusCodePrior="AUCO"))
        self.assertEqual([(t[0], t[2]) for t in self.__transitions()], [("PROC", "AUTH"), ("AUTH", "HPUB")])

    def testUpdateNoHistory(self) -> None:
        """An entry without a history file cannot be updated"""
        shu = self.__newUtils()
        self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", "EP"))
        self.assertFalse(os.path.exists(self.__historyPath(ENTRY_ID)))

    def testUpdateFailure(self) -> None:
        shu = self.__newUtils()
        with mock.patch(MOD + ".StatusHistory", side_effect=RuntimeError("no history")), self.assertLogs(MOD, level="ERROR") as cm:
            self.assertFalse(shu.updateEntryStatusHistory([ENTRY_ID], "HPUB", "EP"))
        self.assertIn("failed with exception", cm.output[0])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unittest.main()
