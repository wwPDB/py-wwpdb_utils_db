##
# File:    StatusHistoryTests.py
# Date:    30-Apr-2014
#
# Updates:
#
#   6-Jan-2015 jdw - Working version -
#   #  7-Aug-2015  jdw   fix number of returned argument for getCurrentStatusDetail()
#
#
##
"""
Test cases for status history file methods and accessors --

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"


import sys
import unittest
import traceback
import time
import os
import os.path
import platform

from wwpdb.utils.db.StatusHistory import StatusHistory
from wwpdb.utils.config.ConfigInfo import getSiteId
from wwpdb.io.file.DataFile import DataFile

from mmcif_utils.pdbx.PdbxIo import PdbxEntryInfoIo

# Not used but simple import test
from wwpdb.utils.db.StatusHistoryUtils import StatusHistoryUtils  # noqa: F401  pylint: disable=unused-import

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))
TESTOUTPUT = os.path.join(HERE, "test-output", platform.python_version())
if not os.path.exists(TESTOUTPUT):  # pragma: no cover
    os.makedirs(TESTOUTPUT)


@unittest.skip("Until can port tests")
class StatusHistoryTests(unittest.TestCase):
    def setUp(self):
        #
        self.__verbose = True
        self.__lfh = sys.stdout
        self.__testFile1 = "./tests/D_1000200183_model-annotate_P1.cif.V1"
        self.__testFile2 = "./tests/D_1000200183_model-release_P1.cif.V1"
        self.__siteId = getSiteId(defaultSiteId="WWPDB_DEPLOY_TEST")
        self.__sessionPath = "."

    def tearDown(self):
        pass

    def testReadWriteHistory(self):
        """Read and write history file --"""
        startTime = time.time()
        self.__lfh.write("\n\n========================================================================================================\n")
        self.__lfh.write("Starting StatusHistoryTests testReadWriteHistory at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            ei = PdbxEntryInfoIo(verbose=self.__verbose, log=self.__lfh)
            ei.setFilePath(filePath=self.__testFile1)
            sD = ei.getInfoD(contextType="history")
            self.__lfh.write("+StatusHistoryTests.testReadWriteHistory() status dictionary %r\n" % sD.items())
            #
            sH = StatusHistory(siteId=self.__siteId, fileSource="archive", sessionPath=self.__sessionPath, verbose=self.__verbose, log=self.__lfh)
            fp = sD["entry_id"] + "_status-history_P1.cif.V1-test"
            ok = sH.setEntryId(entryId=sD["entry_id"], pdbId=sD["pdb_id"], inpPath=fp)
            self.__lfh.write("History file check status is %r\n" % ok)
            #
            ok = sH.add(statusCodeBegin="DEP", dateBegin=sH.getNow(), statusCodeEnd="PROC", dateEnd=sH.getNow(), annotator="JW", details="Automated entry")
            self.__lfh.write("Add row returns status %r\n" % ok)
            #
            sH.add(statusCodeBegin="DEP", dateBegin=sH.getNow(), statusCodeEnd="HPUB", dateEnd=sH.getNow(), annotator="JW", details="Automated entry")
            sH.add(statusCodeBegin="DEP", dateBegin=sH.getNow(), statusCodeEnd="REL", dateEnd=sH.getNow(), annotator="JW", details="Automated entry")
            (lastStatus, lastDate) = sH.getLastStatusAndDate()
            self.__lfh.write("+StatusHistoryTests.testReadWriteHistory() last values %r %r\n" % (lastStatus, lastDate))
            #
            ok = sH.store(entryId=sD["entry_id"], outPath=fp)
            #
            #
            sH = StatusHistory(siteId=self.__siteId, fileSource="archive", sessionPath=self.__sessionPath, verbose=self.__verbose, log=self.__lfh)
            ok = sH.setEntryId(entryId=sD["entry_id"], pdbId=sD["pdb_id"], inpPath=fp)
            self.__lfh.write("Status is %r\n" % ok)
            dList = sH.get()
            for ii, d in enumerate(dList):
                self.__lfh.write("Row %r  : %r\n" % (ii, d.items()))

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted StatusHistoryTests testReadWriteHistory at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testCreateHistory(self):
        """Read existing entry and create initial status records as required -"""
        startTime = time.time()
        self.__lfh.write("\n\n========================================================================================================\n")
        self.__lfh.write("Starting StatusHistoryTests testCreateHistory at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            # Read model file and return status dictionary -
            ei = PdbxEntryInfoIo(verbose=self.__verbose, log=self.__lfh)
            ei.setFilePath(filePath=self.__testFile2)
            sD = ei.getInfoD(contextType="history")
            (
                _entryId,
                _pdbId,
                statusCode,
                _authReleaseCode,
                annotatorInitials,
                initialDepositionDate,
                beginProcessingDate,
                authorApprovalDate,
                releaseDate,
            ) = ei.getCurrentStatusDetails()
            self.__lfh.write("+StatusHistoryTests.testReadWriteHistory() status dictionary %r\n" % sD.items())
            #
            # Get the modification date for the model file -
            df = DataFile(fPath=self.__testFile2)
            currentModelTimeStamp = df.srcModTimeStamp()
            self.__lfh.write("+StatusHistoryTests.testCreateHistory() model file modification time %r\n" % currentModelTimeStamp)
            #
            #
            sH = StatusHistory(siteId=self.__siteId, fileSource="archive", sessionPath=self.__sessionPath, verbose=self.__verbose, log=self.__lfh)
            numHist = sH.setEntryId(entryId=sD["entry_id"], pdbId=sD["pdb_id"])
            self.__lfh.write("Status history count %r\n" % numHist)
            #
            # New status history?  The first record of a new file will mark the PROC->AUTH transition with at one
            #                      additional record depending on the current status state.
            #
            if (numHist < 1) and (statusCode not in ["PROC"]):

                sH.add(
                    statusCodeBegin="PROC",
                    dateBegin=initialDepositionDate,
                    statusCodeEnd="AUTH",
                    dateEnd=beginProcessingDate,
                    annotator=annotatorInitials,
                    details="Automated entry",
                )

                if statusCode in ["REL"]:
                    sH.add(
                        statusCodeBegin="AUTH", dateBegin=beginProcessingDate, statusCodeEnd=statusCode, dateEnd=releaseDate, annotator=annotatorInitials, details="Automated entry"
                    )

                elif statusCode in ["HOLD", "HPUB"]:
                    sH.add(
                        statusCodeBegin="AUTH",
                        dateBegin=beginProcessingDate,
                        statusCodeEnd=statusCode,
                        dateEnd=authorApprovalDate,
                        annotator=annotatorInitials,
                        details="Automated entry",
                    )

                elif statusCode in ["AUCO", "REPL"]:
                    sH.add(
                        statusCodeBegin="AUTH",
                        dateBegin=beginProcessingDate,
                        statusCodeEnd=statusCode,
                        dateEnd=currentModelTimeStamp,
                        annotator=annotatorInitials,
                        details="Automated entry",
                    )
                else:
                    pass
            #
            (lastStatus, lastDate) = sH.getLastStatusAndDate()
            self.__lfh.write("+StatusHistoryTests.testCreateHistory() last values %r %r\n" % (lastStatus, lastDate))
            #
            #  Save the current status history file --
            ok = sH.store(entryId=sD["entry_id"])
            #
            #
            # Recover the contents of the current history file -
            #
            sH = StatusHistory(siteId=self.__siteId, fileSource="archive", sessionPath=self.__sessionPath, verbose=self.__verbose, log=self.__lfh)
            ok = sH.setEntryId(entryId=sD["entry_id"], pdbId=sD["pdb_id"])
            self.__lfh.write("Status is %r\n" % ok)
            dList = sH.get()
            for ii, d in enumerate(dList):
                self.__lfh.write("Row %r  : %r\n" % (ii, d.items()))

        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted StatusHistoryTests testCreateHistory at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))


def suiteReadWriteTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusHistoryTests("testReadWriteHistory"))
    suiteSelect.addTest(StatusHistoryTests("testCreateHistory"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteReadWriteTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
