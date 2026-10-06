##
# File:    StatusHistoryMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for status history file methods and accessors (no database or site configuration required).

PathInfo is replaced by a mock which points at files in a temporary directory -- the status history
files are read and written with the real PDBx IO layer.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import io
import os
import re
import shutil
import tempfile
import unittest
from typing import List
from unittest import mock

from mmcif_utils.pdbx.PdbxIo import PdbxStatusHistoryIo

from wwpdb.utils.db.StatusHistory import StatusHistory

ENTRY_ID = "D_1000000001"
PDB_ID = "1ABC"


class StatusHistoryMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__tmpDir = tempfile.mkdtemp()
        self.__histPath = os.path.join(self.__tmpDir, ENTRY_ID + "_status-history_P1.cif.V1")
        self.__lfh = io.StringIO()
        patcher = mock.patch("wwpdb.utils.db.StatusHistory.PathInfo")
        self.__mockPathInfo = patcher.start()
        self.addCleanup(patcher.stop)
        self.__mockPathInfo.return_value.getStatusHistoryFilePath.return_value = self.__histPath

    def tearDown(self) -> None:
        shutil.rmtree(self.__tmpDir, ignore_errors=True)

    def __newHistory(self, verbose: bool = False) -> StatusHistory:
        return StatusHistory(siteId="TEST_SITE", fileSource="archive", verbose=verbose, log=self.__lfh)

    def __makeHistoryFile(self) -> None:
        """Create a status history file containing three records ending in AUTH."""
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 0)
        self.assertTrue(sH.add("PROC", "2015-01-01", "PROC_ST_1", "2015-01-03:12:00", "JW", "Automated"))
        self.assertTrue(sH.add("PROC_ST_1", "2015-01-03:12:00", "AUTH", "2015-01-05:00:00", "JW", "Automated"))
        self.assertTrue(sH.add("PROC", "2015-01-01", "AUTH", "2015-01-05:00:00", "JW", "Automated"))
        self.assertTrue(sH.store(ENTRY_ID))

    def __readFile(self, path: str) -> str:
        with open(path, encoding="utf-8") as ifh:
            return ifh.read()

    def testPathInfoConstruction(self) -> None:
        """PathInfo is constructed with or without a session path"""
        StatusHistory(siteId="S1", verbose=False, log=self.__lfh)
        _args, kwargs = self.__mockPathInfo.call_args
        self.assertEqual(kwargs["siteId"], "S1")
        self.assertNotIn("sessionPath", kwargs)
        #
        StatusHistory(siteId="S2", sessionPath="/some/session", log=self.__lfh)
        _args, kwargs = self.__mockPathInfo.call_args
        self.assertEqual(kwargs["siteId"], "S2")
        self.assertEqual(kwargs["sessionPath"], "/some/session")

    def testNewEntryEmpty(self) -> None:
        """A missing history file yields an empty container"""
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 0)
        self.__mockPathInfo.return_value.getStatusHistoryFilePath.assert_called_with(dataSetId=ENTRY_ID, fileSource="archive", versionId="latest")
        self.assertEqual(sH.get(), [])
        self.assertEqual(sH.getLastStatusAndDate(), (None, None))
        # Nothing to store
        self.assertFalse(sH.store(ENTRY_ID))
        self.assertFalse(os.path.exists(self.__histPath))

    def testAddAndStore(self) -> None:
        """Add records, store them and read them back"""
        sH = self.__newHistory(verbose=True)
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertTrue(sH.add("PROC", "2015-01-01", "PROC_ST_1", "2015-01-03:12:00", "JW", "first"))
        self.assertTrue(sH.add("PROC_ST_1", "2015-01-03:12:00", "AUTH", "2015-01-02", None, None))
        dL = sH.get()
        self.assertEqual(len(dL), 2)
        self.assertEqual(dL[0]["entry_id"], ENTRY_ID)
        self.assertEqual(dL[0]["pdb_id"], PDB_ID)
        self.assertEqual(dL[0]["ordinal"], "1")
        self.assertEqual(dL[0]["date_begin"], "2015-01-01:00:00")
        self.assertEqual(dL[0]["date_end"], "2015-01-03:12:00")
        self.assertEqual(dL[0]["annotator"], "JW")
        self.assertEqual(dL[0]["details"], "first")
        self.assertEqual(dL[0]["delta_days"], "2.5000")
        # Second record -- negative time delta is clamped and annotator defaulted
        self.assertEqual(dL[1]["ordinal"], "2")
        self.assertEqual(dL[1]["annotator"], "UNASSIGNED")
        self.assertIsNone(dL[1]["details"])
        self.assertEqual(dL[1]["delta_days"], "0.0000")
        self.assertEqual(sH.getLastStatusAndDate(), ("AUTH", "2015-01-02:00:00"))
        #
        self.assertTrue(sH.store(ENTRY_ID, versionId="next"))
        self.__mockPathInfo.return_value.getStatusHistoryFilePath.assert_called_with(dataSetId=ENTRY_ID, fileSource="archive", versionId="next")
        self.assertIn("storing 2 history records", self.__lfh.getvalue())
        self.assertIn("begins with nRows 1", self.__lfh.getvalue())
        content = self.__readFile(self.__histPath)
        self.assertIn("data_" + ENTRY_ID, content)
        self.assertIn("_pdbx_database_status_history.delta_days", content)
        #
        # Read back using an alternate input path
        sH2 = self.__newHistory()
        self.assertEqual(sH2.setEntryId(ENTRY_ID, PDB_ID, inpPath=self.__histPath), 2)
        dL2 = sH2.get()
        self.assertEqual(len(dL2), 2)
        self.assertEqual(dL2[0], dL[0])
        # An unset value is written as the CIF missing value marker
        self.assertEqual(dL2[1]["details"], "?")
        self.assertEqual(dL2[1]["status_code_end"], "AUTH")

    def testStoreAlternatePath(self) -> None:
        """Store to an explicit output path"""
        self.__makeHistoryFile()
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 3)
        outPath = os.path.join(self.__tmpDir, "alt.cif")
        self.assertTrue(sH.store(ENTRY_ID, outPath=outPath))
        self.assertEqual(self.__readFile(outPath), self.__readFile(self.__histPath))

    def testOverWrite(self) -> None:
        """overWrite discards existing history records"""
        self.__makeHistoryFile()
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 3)
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID, overWrite=True), 0)
        self.assertEqual(sH.get(), [])

    def testMissingInpPath(self) -> None:
        """A missing alternate input path yields an empty container"""
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID, inpPath=os.path.join(self.__tmpDir, "missing.cif")), 0)

    def testAddValidation(self) -> None:
        """add() rejects missing required values"""
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertFalse(sH.add(statusCodeBegin=None, dateBegin="2015-01-01", statusCodeEnd="AUTH", dateEnd="2015-01-02"))
        self.assertFalse(sH.add(statusCodeBegin="", dateBegin="2015-01-01", statusCodeEnd="AUTH", dateEnd="2015-01-02"))
        self.assertFalse(sH.add(statusCodeBegin="PROC", dateBegin="2015-01-01", statusCodeEnd=None, dateEnd="2015-01-02"))
        self.assertFalse(sH.add(statusCodeBegin="PROC", dateBegin="2015-01-01", statusCodeEnd="", dateEnd="2015-01-02"))
        self.assertFalse(sH.add(statusCodeBegin="PROC", dateBegin=None, statusCodeEnd="AUTH", dateEnd="2015-01-02"))
        self.assertFalse(sH.add(statusCodeBegin="PROC", dateBegin="short", statusCodeEnd="AUTH", dateEnd="2015-01-02"))
        self.assertEqual(sH.get(), [])
        # Missing pdbId
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, None)
        self.assertFalse(sH.add("PROC", "2015-01-01", "AUTH", "2015-01-02"))
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, "")
        self.assertFalse(sH.add("PROC", "2015-01-01", "AUTH", "2015-01-02"))
        # Missing entryId
        sH = self.__newHistory()
        sH.setEntryId("", PDB_ID)
        self.assertFalse(sH.add("PROC", "2015-01-01", "AUTH", "2015-01-02"))

    def testAddDefaultEndDate(self) -> None:
        """A missing end date defaults to the current time"""
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertTrue(sH.add("PROC", "2015-01-01", "AUTH", None))
        dL = sH.get()
        self.assertEqual(len(dL), 1)
        dateEnd = dL[0]["date_end"]
        self.assertIsNotNone(dateEnd)
        self.assertRegex(str(dateEnd), r"^\d{4}-\d{2}-\d{2}:\d{2}:\d{2}$")
        self.assertGreater(float(str(dL[0]["delta_days"])), 3000.0)

    def testGetNow(self) -> None:
        sH = self.__newHistory()
        now = sH.getNow()
        self.assertTrue(re.match(r"^\d{4}-\d{2}-\d{2}:\d{2}:\d{2}$", now))
        self.assertTrue(sH.dateTimeOk(now))

    def testDateTimeOk(self) -> None:
        sH = self.__newHistory()
        goodL: List[str] = ["2015-01-02", "2015-01-02:10:11", "2015-01-02:10:11:12", "2015-01-02:10:11 extra text"]
        for val in goodL:
            self.assertTrue(sH.dateTimeOk(val), val)
        badL: List[str] = ["", "2015", "2015-13-45", "2015-01-02:1", "garbage-garbage-garbage", "2015-02-30:00:00"]
        for val in badL:
            self.assertFalse(sH.dateTimeOk(val), val)
        self.assertFalse(sH.dateTimeOk(None))
        # Failures in conversion are reported on the log
        self.assertIn("fails for inpTimeStamp", self.__lfh.getvalue())

    def testNextRecord(self) -> None:
        """nextRecord() chains a new record from the last status"""
        self.__makeHistoryFile()
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, None), 3)
        # Same status is rejected
        self.assertFalse(sH.nextRecord(statusCodeNext="AUTH", dateNext="2015-02-01"))
        self.assertTrue(sH.nextRecord(statusCodeNext="HPUB", dateNext="2015-02-01", annotator="EP", details="next"))
        dL = sH.get()
        self.assertEqual(len(dL), 4)
        last = dL[-1]
        self.assertEqual(last["ordinal"], "4")
        self.assertEqual(last["status_code_begin"], "AUTH")
        self.assertEqual(last["status_code_end"], "HPUB")
        self.assertEqual(last["date_begin"], "2015-01-05:00:00")
        self.assertEqual(last["date_end"], "2015-02-01:00:00")
        # pdbId is recovered from the existing records
        self.assertEqual(last["pdb_id"], PDB_ID)
        self.assertEqual(last["annotator"], "EP")
        self.assertEqual(last["details"], "next")
        self.assertEqual(last["delta_days"], "27.0000")
        self.assertEqual(sH.getLastStatusAndDate(), ("HPUB", "2015-02-01:00:00"))
        # Default date is now
        self.assertTrue(sH.nextRecord(statusCodeNext="REL"))
        self.assertEqual(sH.get()[-1]["status_code_end"], "REL")
        self.assertTrue(sH.dateTimeOk(sH.get()[-1]["date_end"]))

    def testNextRecordEmpty(self) -> None:
        """nextRecord() fails without a prior status"""
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertFalse(sH.nextRecord(statusCodeNext="AUTH"))
        self.assertEqual(sH.get(), [])

    def testGetLastStatusRecoversPdbId(self) -> None:
        """getLastStatusAndDate() recovers pdbId so later additions succeed"""
        self.__makeHistoryFile()
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, None)
        self.assertEqual(sH.getLastStatusAndDate(), ("AUTH", "2015-01-05:00:00"))
        self.assertTrue(sH.add("AUTH", "2015-01-05", "HOLD", "2015-01-06"))
        self.assertEqual(sH.get()[-1]["pdb_id"], PDB_ID)

    def testLastOrdinalSelection(self) -> None:
        """The last status is taken from the record with the highest ordinal, not the last row"""
        self.__makeHistoryFile()
        content = self.__readFile(self.__histPath)
        # Swap the ordinals of the first and last rows
        content = content.replace(PDB_ID + " 1 ", PDB_ID + " X ").replace(PDB_ID + " 3 ", PDB_ID + " 1 ").replace(PDB_ID + " X ", PDB_ID + " 3 ")
        with open(self.__histPath, "w", encoding="utf-8") as ofh:
            ofh.write(content)
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 3)
        self.assertEqual(sH.getLastStatusAndDate(), ("PROC_ST_1", "2015-01-03:12:00"))

    def testCorruptOrdinal(self) -> None:
        """A non-numeric ordinal is reported and no last status is returned"""
        self.__makeHistoryFile()
        content = self.__readFile(self.__histPath).replace(PDB_ID + " 3 ", PDB_ID + " bad ")
        with open(self.__histPath, "w", encoding="utf-8") as ofh:
            ofh.write(content)
        sH = self.__newHistory()
        self.assertEqual(sH.setEntryId(ENTRY_ID, PDB_ID), 3)
        self.assertEqual(sH.getLastStatusAndDate(), (None, None))
        self.assertIn("Traceback", self.__lfh.getvalue())
        self.assertFalse(sH.nextRecord(statusCodeNext="REL"))

    @unittest.expectedFailure
    def testAddWithCorruptOrdinal(self) -> None:
        """add() documents a True/False return but raises TypeError when the prior ordinal cannot be parsed.

        __appendRow() uses the ordinal returned by __lastStatusAndDate() (None on failure) in ``iOrdinal + 1``.
        """
        self.__makeHistoryFile()
        content = self.__readFile(self.__histPath).replace(PDB_ID + " 3 ", PDB_ID + " bad ")
        with open(self.__histPath, "w", encoding="utf-8") as ofh:
            ofh.write(content)
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertFalse(sH.add("AUTH", "2015-01-05", "HOLD", "2015-01-06"))

    def testStoreWriteFailure(self) -> None:
        """A failure from the IO layer is reported by store()"""
        sH = self.__newHistory()
        sH.setEntryId(ENTRY_ID, PDB_ID)
        self.assertTrue(sH.add("PROC", "2015-01-01", "AUTH", "2015-01-02"))
        outPath = os.path.join(self.__tmpDir, "out.cif")
        with mock.patch.object(PdbxStatusHistoryIo, "write", return_value=False) as mockWrite:
            self.assertFalse(sH.store(ENTRY_ID, outPath=outPath))
        mockWrite.assert_called_once_with(outPath)
        self.assertFalse(os.path.exists(outPath))


if __name__ == "__main__":
    unittest.main()
