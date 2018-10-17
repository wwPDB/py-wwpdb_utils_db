##
#
# File:    MyDbUtilTests.py
# Author:  J. Westbrook
# Date:    20-June-2015
# Version: 0.001
##
"""
Test cases opening database connections.

Environment setup --

        . set-test-env.sh

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import os
import sys
import unittest
import traceback
import time

from wwpdb.utils.db.MyDbUtil import MyDbConnect
from wwpdb.utils.db.MyDbUtil import MyDbQuery
from wwpdb.utils.db.MyDbAdapter import MyDbAdapter

from mmcif.io.IoAdapterCore import IoAdapterCore

from wwpdb.utils.testing.Features import Features

@unittest.skipUnless(Features().haveMySqlTestServer(), 'require MySql Test Environment')
class MyDbUtilTests(unittest.TestCase):

    def setUp(self):
        self.__dbName = 'stat'
        self.__lfh = sys.stderr
        self.__verbose = True
        self.__dbCon = None

    def tearDown(self):
        self.close()

    def open(self, dbUserId=None, dbUserPwd=None, dbHost=None, dbName=None, dbSocket=None):
        myC = MyDbConnect(dbServer='mysql', dbHost=dbHost, dbName=dbName, dbUser=dbUserId, dbPw=dbUserPwd, dbSocket=dbSocket, verbose=self.__verbose, log=self.__lfh)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            if self.__verbose:
                self.__lfh.write("\nDatabase connection opened %s %s at %s\n" % (self.__class__.__name__,
                                                                                 sys._getframe().f_code.co_name,
                                                                                 time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            if self.__verbose:
                self.__lfh.write("\nDatabase connection closed %s %s at %s\n" % (self.__class__.__name__,
                                                                                 sys._getframe().f_code.co_name,
                                                                                 time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
            self.__dbCon.close()
            self.__dbCon = None
            return True
        else:
            return False

    def testOpen1(self):
        """Test case -  all values specified

        Environment setup --

        . set-test-env.sh

        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            dbSocket = os.getenv("TEST_DB_SOCKET")
            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName, dbSocket=dbSocket)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%f seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))

    def testOpen2(self):
        """Test case -  w/o socket

            Environment setup --

            . set-test-env.sh

        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")

            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%f seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))

    def testOpen3(self):
        """Test case -  w/o socket w/ localhost

            Environment setup --

            . set-test-env.sh

        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = 'localhost'

            ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
            self.assertTrue(ok)
            ok = self.close()
            self.assertTrue(ok)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%f seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))

    def testPool1(self):
        """Test case -  connection pool management -

        Setup -
        . set-test-env.sh

        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        self.__verbose = False
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            for ii in xrange(5000):
                ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
                self.assertTrue(ok)
                ok = self.close()
                self.assertTrue(ok)

        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%f seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))

    def testPoolQuery(self):
        """Test case -  connection pool management -

        Setup -
        . set-test-env.sh

        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        self.__verbose = False
        try:
            dbUserId = os.getenv("TEST_DB_USER_NAME")
            dbUserPwd = os.getenv("TEST_DB_PASSWORD")
            dbName = os.getenv("TEST_DB_NAME")
            dbHost = os.getenv("TEST_DB_HOST")
            for ii in xrange(5000):
                ok = self.open(dbUserId=dbUserId, dbUserPwd=dbUserPwd, dbHost=dbHost, dbName=dbName)
                self.assertTrue(ok)
                for jj in xrange(100):
                    my = MyDbQuery(dbcon=self.__dbCon)
                    ok = my.testSelectQuery(count=ii + jj)
                    self.assertTrue(ok)

                ok = self.close()
                self.assertTrue(ok)
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%f seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))


def suiteOpen():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbUtilTests("testOpen1"))
    suiteSelect.addTest(MyDbUtilTests("testOpen2"))
    suiteSelect.addTest(MyDbUtilTests("testOpen3"))
    return suiteSelect


def suitePool():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyDbUtilTests("testPool1"))
    suiteSelect.addTest(MyDbUtilTests("testPoolQuery"))
    return suiteSelect


if __name__ == '__main__':
    if (True):
        mySuite = suiteOpen()
        unittest.TextTestRunner(verbosity=2).run(mySuite)

    if (True):
        mySuite = suitePool()
        unittest.TextTestRunner(verbosity=2).run(mySuite)
