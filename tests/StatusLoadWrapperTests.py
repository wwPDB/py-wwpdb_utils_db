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


import os
import sys
import time
import traceback
import unittest

if __package__ is None or __package__ == "":  # noqa: PLC1901
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mock_import import mocksetup  # type: ignore  # noqa: F401 pylint: disable=unused-import,import-error
else:
    from .mock_import import mocksetup  # noqa: F401,TID252 pylint: disable=unused-import,import-error

from wwpdb.utils.config.ConfigInfo import getSiteId
from wwpdb.utils.db.StatusLoadWrapper import StatusLoadWrapper
from wwpdb.utils.testing.Features import Features


@unittest.skipUnless(Features().haveMySqlTestServer(), "Needs MySql test server for testing")
class StatusLoadWrapperTests(unittest.TestCase):
    def setUp(self):
        #
        self.__verbose = True
        self.__lfh = sys.stdout
        self.__depId = "D_1000000001"
        self.__siteId = getSiteId(defaultSiteId="WWPDB_DEPLOY_TEST")

    def tearDown(self):
        pass

    def testLoad(self):
        """Load da_internal database -"""
        startTime = time.time()
        self.__lfh.write("\n\n========================================================================================================\n")
        self.__lfh.write("Starting StatusLoadWrapperTests testLoad at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.__lfh.write("+testLoad- Using site id %r\n" % self.__siteId)
            slw = StatusLoadWrapper(siteId=self.__siteId, verbose=self.__verbose, log=self.__lfh)
            slw.dbLoad(self.__depId)
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted StatusLoadWrapperTests testLoad at %s (%.2f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )


def suiteLoadTests():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(StatusLoadWrapperTests("testLoad"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteLoadTests()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
