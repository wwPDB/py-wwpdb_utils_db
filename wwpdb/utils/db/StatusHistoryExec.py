#!/opt/wwpdb/bin/python
##
# File:    StatusHistoryExec.py
# Author:  jdw
# Date:    15-Jan-2015
# Version: 0.001
#
# Updates:
#   7-Aug-2015  jdw add option to create individual history files -
##
"""
Execuction module for status history database management --

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.001"

import os
import sys
import traceback
from optparse import OptionParser  # pylint: disable=deprecated-module

from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.db.StatusHistoryUtils import StatusHistoryUtils
from wwpdb.utils.session.WebRequest import InputRequest


class StatusHistoryExec:
    def __init__(self, defSiteId="WWWDPB_INTERNAL_RU", sessionId=None, verbose=True, log=sys.stderr):
        self.__lfh = log
        self.__verbose = verbose
        self.__setup(defSiteId=defSiteId, sessionId=sessionId)

    def __setup(self, defSiteId=None, sessionId=None):
        """Simulate the web application environment for managing session storage of  temporaty data files."""
        self.__siteId = getSiteId(defaultSiteId=defSiteId)
        #
        self.__cI = ConfigInfo(self.__siteId)
        self.__topPath = self.__cI.get("SITE_WEB_APPS_TOP_PATH")
        self.__topSessionPath = self.__cI.get("SITE_WEB_APPS_TOP_SESSIONS_PATH")
        #
        self.__reqObj = InputRequest({}, verbose=self.__verbose, log=self.__lfh)
        self.__reqObj.setValue("TopSessionPath", self.__topSessionPath)
        self.__reqObj.setValue("TopPath", self.__topPath)
        self.__reqObj.setValue("WWPDB_SITE_ID", self.__siteId)
        #
        self.__reqObj.setValue("SITE_DA_INTERNAL_DB_USER", os.environ["SITE_DA_INTERNAL_DB_USER"])
        self.__reqObj.setValue("SITE_DA_INTERNAL_DB_PASSWORD", os.environ["SITE_DA_INTERNAL_DB_PASSWORD"])

        os.environ["WWPDB_SITE_ID"] = self.__siteId
        if sessionId is not None:
            self.__reqObj.setValue("sessionid", sessionId)

        # retained due to side effects
        _sessionObj = self.__reqObj.newSessionObj()  # noqa: F841
        self.__reqObj.printIt(ofh=self.__lfh)
        #

    def doCreateStatusHistory(self, numProc=1, overWrite=False):
        """ """
        try:
            shu = StatusHistoryUtils(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            entryIdList = shu.getEntryIdList()
            if numProc > 1:
                rL = shu.createHistoryMulti(entryIdList, numProc=numProc, overWrite=overWrite)
            else:
                rL = shu.createHistory(entryIdList, overWrite=overWrite)
            self.__lfh.write("StatusHistoryExec.doCreateStatusHistory() %d status files created.\n\n" % len(rL))
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)

    def doLoadStatusHistory(self, numProc=1, newTable=False):
        """ """
        try:
            shu = StatusHistoryUtils(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            if numProc > 1:
                return shu.loadStatusHistoryMulti(numProc, newTable=newTable)
            return shu.loadStatusHistory(newTable=newTable)
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)

        return False

    def doLoadEntryStatusHistory(self, entryId):
        """Load/reload status history file for the input entryId"""
        try:
            shu = StatusHistoryUtils(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            return shu.loadEntryStatusHistory(entryIdList=[entryId])
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
        return False

    def doCreateEntryStatusHistory(self, entryId, overWrite=False):
        """ """
        try:
            shu = StatusHistoryUtils(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            rL = shu.createHistory([entryId], overWrite=overWrite)
            self.__lfh.write("StatusHistoryExec.doCreateEntryStatusHistory() %d status files created.\n\n" % len(rL))
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)

    def doCreateStatusHistorySchema(self):
        """Create/recreate status history schema -"""
        try:
            shu = StatusHistoryUtils(reqObj=self.__reqObj, verbose=self.__verbose, log=self.__lfh)
            return shu.createStatusHistorySchema()
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
        return False


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)

    parser.add_option("--loadentry", dest="loadEntryId", default=None, help="Load/reload datadase with entry status history file")
    parser.add_option("--createentry", dest="createEntryId", default=None, help="Create entry status history file")

    parser.add_option("--create", dest="create", action="store_true", default=False, help="Batch mode create/recreate missing history files")
    parser.add_option("--overwrite", dest="new", action="store_true", default=False, help="Overwrite any existing history files")

    parser.add_option("--load", dest="load", action="store_true", default=False, help="Batch mode data load for all history files")
    parser.add_option("--newtable", dest="newTable", action="store_true", default=False, help="Create a new status history database table before load")

    parser.add_option("--numproc", dest="numProc", default=1, help="Number of processors to engage in batch mode operations.")

    parser.add_option("-v", "--verbose", default=False, action="store_true", dest="verbose")

    (options, _args) = parser.parse_args()

    #    crx=StatusHistoryExec(defSiteId='WWDPB_INTERNAL_RU',sessionId=None,verbose=options.verbose,log=sys.stderr)
    crx = StatusHistoryExec(defSiteId="WWPDB_DEPLOY_MACOSX", sessionId=None, verbose=options.verbose, log=sys.stderr)

    if options.newTable and not options.load:
        crx.doCreateStatusHistorySchema()

    if options.createEntryId is not None:
        crx.doCreateEntryStatusHistory(options.createEntryId, overWrite=options.new)

    if options.loadEntryId is not None:
        crx.doLoadEntryStatusHistory(options.loadEntryId)
    else:
        # batch mode options -
        if options.create:
            crx.doCreateStatusHistory(int(options.numProc), overWrite=options.new)
        if options.load:
            crx.doLoadStatusHistory(int(options.numProc), newTable=options.newTable)


if __name__ == "__main__":
    main()
