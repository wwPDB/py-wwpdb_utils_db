##
# File:  StatusLoadWrapper.py
# Date:  18-May-2015
#
# Updates:
##
"""
Wrapper for simple database loader for da_internal database --

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import os
import traceback

from wwpdb.utils.config.ConfigInfo import ConfigInfo
from wwpdb.io.locator.PathInfo import PathInfo
from wwpdb.utils.db.DbLoadingApi import DbLoadingApi


class StatusLoadWrapper(object):

    """ Update release status items.

    """

    def __init__(self, siteId, verbose=False, log=sys.stderr):
        """
         :param `verbose`:  boolean flag to activate verbose logging.
         :param `log`:      stream for logging.

        """
        self.__verbose = verbose
        self.__lfh = log
        self.__debug = True
        self.__siteId = siteId
        #
        self.__cI = ConfigInfo(self.__siteId)
        self.__pI = PathInfo(siteId=self.__siteId, sessionPath='.', verbose=self.__verbose, log=self.__lfh)

    def dbLoad(self, depSetId, fileSource='deposit', versionId='latest', mileStone='deposit'):
        try:
            self.__lfh.write("+StatusLoadWrapper.dbload() site %s loading data set %s %s %s %s\n" % (self.__siteId, depSetId, fileSource, mileStone, versionId))
            pdbxFilePath = self.__pI.getModelPdbxFilePath(dataSetId=depSetId, fileSource=fileSource, versionId=versionId, mileStone=mileStone)
            fD, fN = os.path.split(pdbxFilePath)
            dbLd = DbLoadingApi(log=self.__lfh, verbose=self.__verbose)
            return dbLd.doLoadStatus(pdbxFilePath, fD)
        except Exception as e:
            if (self.__verbose):
                self.__lfh.write("+StatusLoadWrapper.dbload() dbload failed for %s %s\n" % (depSetId, str(e)))
                traceback.print_exc(file=self.__lfh)
            return False
