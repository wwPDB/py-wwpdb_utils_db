##
# File:  StatusHistory.py
# Date:  1-Jan-2015 J. Westbrook
#
# Update:
#  6-Jan-2015  jdw   working with expanded schema -
# 17-Jan-2015  jdw   add nextRecord()
# 29-Jan-2015  jdw   add special support for AUTH|WAIT
##
"""
Manage status history data file access and update --

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import datetime
import os
import sys
import traceback

from mmcif_utils.pdbx.PdbxIo import PdbxStatusHistoryIo

from wwpdb.io.locator.PathInfo import PathInfo


class StatusHistory:
    """
    Manage status history updates.

    """

    def __init__(self, siteId=None, fileSource="archive", sessionPath=None, verbose=False, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__debug = False
        self.__fileSource = fileSource
        self.__sessionPath = sessionPath
        self.__siteId = siteId
        #
        self.__inpFilePath = None
        self.__entryId = None
        self.__pdbId = None
        #
        self.__setup()

    def __setup(self):
        if self.__sessionPath is not None:
            self.__pI = PathInfo(siteId=self.__siteId, sessionPath=self.__sessionPath, verbose=self.__verbose, log=self.__lfh)
        else:
            self.__pI = PathInfo(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
        #
        self.__inpFilePath = None
        self.__entryId = None
        self.__pdbId = None
        self.__pio = PdbxStatusHistoryIo(verbose=self.__verbose, log=self.__lfh)
        self.__statusCategory = "pdbx_database_status_history"
        self.__timeFormat = "%Y-%m-%d:%H:%M"
        #

    def __setEntryId(self, entryId, pdbId):
        """Set the file path of the status history file and read any existing content --"""
        self.__entryId = entryId
        self.__pdbId = pdbId
        self.__inpFilePath = self.__pI.getStatusHistoryFilePath(dataSetId=entryId, fileSource=self.__fileSource, versionId="latest")
        if self.__exists():
            return self.__pio.setFilePath(filePath=self.__inpFilePath, idCode=entryId)
        return False

    def __setInpPath(self, inpPath, entryId, pdbId):
        """Set the file path of the status history file and read any existing content --"""
        self.__entryId = entryId
        self.__pdbId = pdbId
        self.__inpFilePath = inpPath
        if self.__exists():
            return self.__pio.setFilePath(filePath=self.__inpFilePath, idCode=entryId)
        return False

    def __new(self, entryId):
        """Create a new status history category using base category style content definition --"""
        return self.__pio.newContainer(containerName=entryId, overWrite=True)

    def setEntryId(self, entryId, pdbId, inpPath=None, overWrite=False):
        """Open an existing status history file from the archive directory and read the container corresponding to the input
        entry id.    An alternate file path can be provided to override reading input from the archive directory.

        overWrite = True to rewrite any existing history file -

        Return:  True for an existing file or for the creation of a new empty container initialized with an
                      empty status history data category, or False otherwise.

        """
        if inpPath is not None:
            ok = self.__setInpPath(inpPath, entryId, pdbId)
        else:
            ok = self.__setEntryId(entryId, pdbId)

        if not ok or overWrite:
            ok = self.__new(entryId)

        return self.__getRowCount()

    def store(self, entryId, outPath=None, versionId="latest"):
        if self.__getRowCount() < 1:
            return False
        if outPath is None:
            outFilePath = self.__pI.getStatusHistoryFilePath(dataSetId=entryId, fileSource=self.__fileSource, versionId=versionId)
        else:
            outFilePath = outPath
        #
        if self.__verbose:
            self.__lfh.write("+StatusHistory.store() %s storing %d history records in file path %s\n" % (entryId, self.__getRowCount(), outFilePath))
        #
        return self.__pio.write(outFilePath)

    def __exists(self):
        """Return True if a status history file exists or false otherwise."""
        if os.access(self.__inpFilePath, os.R_OK):
            return True
        return False

    def getNow(self):
        return self.__getNow()

    def __getNow(self):
        """Return a CIF style date-timestamp value for current local time -"""
        today = datetime.datetime.today()  # No timezone  # noqa: DTZ002
        return str(today.strftime(self.__timeFormat))  # noqa: DTZ007

    def dateTimeOk(self, dateTime):
        try:
            tS = self.__makeTimeStamp(dateTime)
            if (tS is not None) and (len(tS) < 16):
                return False
            datetime.datetime.strptime(tS, self.__timeFormat)  # noqa: DTZ007
            return True
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def __makeTimeStamp(self, inpTimeStamp):
        try:
            inpT = ""
            if len(inpTimeStamp) < 10:
                return inpT
            if len(inpTimeStamp) == 10:
                inpT = inpTimeStamp + ":00:00"
            elif len(inpTimeStamp) >= 16:
                inpT = inpTimeStamp[:16]
            #
            t = datetime.datetime.strptime(inpT, self.__timeFormat)  # noqa: DTZ007
            return str(t.strftime(self.__timeFormat))  # noqa: DTZ007
        except Exception as e:  # noqa: BLE001
            self.__lfh.write("+StatusHistory.__makeTimeStamp() fails for inpTimeStamp %r inpT %r err %r\n" % (inpTimeStamp, inpT, str(e)))
            if self.__debug:
                traceback.print_exc(file=self.__lfh)
            return inpTimeStamp

    def __getRowCount(self):
        return self.__pio.getRowCount(catName=self.__statusCategory)

    # def __updatePriorEndDate(self, dateEnd=None):
    #     """Update the 'end-date' value for the previous status history record."""
    #     nRows = self.__getRowCount()
    #     if nRows > 0 and dateEnd is not None:
    #         ok = self.__pio.updateAttribute(catName=self.__statusCategory, attribName="date_end", value=dateEnd, iRow=nRows - 1)
    #         return ok
    #     else:
    #         return False

    def get(self):
        return self.__pio.getAttribDictList(catName=self.__statusCategory)

    def getLastStatusAndDate(self):
        tup = self.__lastStatusAndDate()
        if (self.__pdbId is None) and (len(tup) > 3):
            self.__pdbId = tup[3]
        return (tup[0], tup[1])

    def __lastStatusAndDate(self):
        """Return the last status code, time stamp, and ordinal index in the current data context."""
        try:
            nRows = self.__getRowCount()
            if nRows > 0:
                dList = self.__pio.getAttribDictList(catName=self.__statusCategory)
                # -- Get the row with the last ordinal --
                tOrd = (-1, -1)
                for ii, d in enumerate(dList):
                    if int(str(d["ordinal"])) > tOrd[1]:
                        tOrd = (ii, int(d["ordinal"]))
                dA = dList[tOrd[0]]
                return (dA["status_code_end"], dA["date_end"], int(str(dA["ordinal"])), dA["pdb_id"])
            return (None, None, None, None)
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            return (None, None, None, None)

    # def __testValueExists(self, value, key="status_code_begin"):
    #     try:
    #         dList = self.__pio.getAttribDictList(catName=self.__statusCategory)
    #         for _ii, d in enumerate(dList):
    #             if d[key] == value:
    #                 return True
    #     except:  # noqa: E722  pylint: disable=bare-except
    #         return False

    def nextRecord(self, statusCodeNext="AUTH", dateNext=None, annotator=None, details=None):
        """ """
        try:
            statusLast, dateLast, _ordinalLast, pdbId = self.__lastStatusAndDate()
            if statusCodeNext == statusLast:
                return False
            if self.__pdbId is None or len(self.__pdbId) < 4:
                self.__pdbId = pdbId
            if dateNext is None:
                dateNext = self.getNow()
            ok = self.add(statusCodeBegin=statusLast, dateBegin=dateLast, statusCodeEnd=statusCodeNext, dateEnd=dateNext, annotator=annotator, details=details)
            return ok
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def add(self, statusCodeBegin="PROC", dateBegin=None, statusCodeEnd="PROC", dateEnd=None, annotator=None, details=None):
        return self.__appendRow(
            entryId=self.__entryId,
            pdbId=self.__pdbId,
            statusCodeBegin=statusCodeBegin,
            dateBegin=self.__makeTimeStamp(dateBegin),
            statusCodeEnd=statusCodeEnd,
            dateEnd=self.__makeTimeStamp(dateEnd),
            annotator=annotator,
            details=details,
        )

    def __appendRow(self, entryId, pdbId, statusCodeBegin="PROC", dateBegin=None, statusCodeEnd="PROC", dateEnd=None, annotator=None, details=None):
        """
        Append a row to the status history list --

        if -  dateEnd is not specified then the current date-time is used.

        return True for success or false otherwise
        """
        uD = {}

        nRows = self.__getRowCount()
        if self.__verbose:
            self.__lfh.write(
                "+StatusHistory.__appendRow() %s  begins with nRows %r pdbId %r statusBegin %r dateBegin %r statusEnd %r dateEnd %r\n"
                % (entryId, nRows, pdbId, statusCodeBegin, dateBegin, statusCodeEnd, dateEnd)
            )
        if nRows < 0:
            return False
        #
        if entryId is not None and len(entryId) > 0:
            uD["entry_id"] = str(entryId)
        else:
            return False

        if pdbId is not None and len(pdbId) > 0:
            uD["pdb_id"] = str(pdbId)
        else:
            return False

        if statusCodeBegin is not None and len(statusCodeBegin) > 0:
            uD["status_code_begin"] = str(statusCodeBegin)
        else:
            return False

        if statusCodeEnd is not None and len(statusCodeEnd) > 0:
            uD["status_code_end"] = str(statusCodeEnd)
        else:
            return False

        if dateBegin is not None and len(dateBegin) > 0:
            uD["date_begin"] = str(dateBegin)
        else:
            return False

        if dateEnd is not None and len(dateEnd) > 0:
            uD["date_end"] = str(dateEnd)
        else:
            uD["date_end"] = self.__getNow()

        if details is not None:
            uD["details"] = str(details)

        if annotator is not None and len(annotator) > 0:
            uD["annotator"] = str(annotator)
        else:
            uD["annotator"] = "UNASSIGNED"

        if nRows == 0:
            iOrdinal = 0
        else:
            _t, tt, iOrdinal, _ttt = self.__lastStatusAndDate()

        uD["ordinal"] = str(iOrdinal + 1)
        #
        # Compute the time delta -
        #
        tt = self.__deltaDate(uD["date_end"], uD["date_begin"])
        if tt > 0:
            uD["delta_days"] = "%.4f" % tt
        else:
            uD["delta_days"] = "0.0000"
        #

        ok = self.__pio.appendRowByAttribute(rowAttribDict=uD, catName=self.__statusCategory)
        return ok

    def __deltaDate(self, dateTimeEnd, dateTimeBegin, fail=-1):
        try:
            tEnd = datetime.datetime.strptime(dateTimeEnd, self.__timeFormat)  # noqa: DTZ007
            tBegin = datetime.datetime.strptime(dateTimeBegin, self.__timeFormat)  # noqa: DTZ007
            tDelta = tEnd - tBegin
            days = float(tDelta.total_seconds()) / 86400.0
            #  print " dateTimeBegin=", dateTimeBegin, " dateTimeEnd=", dateTimeEnd, " tDelta=", tDelta.total_seconds(), " days=", days
            return days
        except:  # noqa: E722  pylint: disable=bare-except
            return fail
