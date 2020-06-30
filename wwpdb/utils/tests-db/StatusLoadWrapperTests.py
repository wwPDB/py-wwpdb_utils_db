##
# File:    StatusLoadWrapperTests.py
# Date:    18-May-2015
#
# Updates:
#
#
##
"""
Test cases for status load wrapper
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

if __package__ is None or __package__ == '':
    import sys
    from os import path

    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    from mock_import import mocksetup
else:
    from .mock_import import mocksetup

import mock_import
from wwpdb.utils.db.StatusLoadWrapper import StatusLoadWrapper
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.testing.Features import Features

@unittest.skipUnless(Features().haveMySqlTestServer(), "Needs MySql test server for testing")
class StatusLoadWrapperTests(unittest.TestCase):

    def setUp(self):
        #
        self.__verbose = True
        self.__lfh = sys.stdout
        self.__depId = 'D_1000000001'
        self.__siteId = getSiteId(defaultSiteId="WWPDB_DEPLOY_TEST")

    def tearDown(self):
        pass

    def testLoad(self):
        """ Load da_internal database -
        """
        startTime = time.clock()
        self.__lfh.write("\n\n========================================================================================================\n")
        self.__lfh.write("Starting %s %s at %s\n" % (self.__class__.__name__,
                                                     sys._getframe().f_code.co_name,
                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        try:
            self.__lfh.write("+testLoad- Using site id %r\n" % self.__siteId)
            slw = StatusLoadWrapper(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
            slw.dbLoad(self.__depId)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.clock()
        self.__lfh.write("\nCompleted %s %s at %s (%.2f seconds)\n" % (self.__class__.__name__,
                                                                       sys._getframe().f_code.co_name,
                                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                       endTime - startTime))


def suiteLoadTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusLoadWrapperTests("testLoad"))
    return suiteSelect

if __name__ == '__main__':

    if (True):
        mySuite = suiteLoadTests()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
