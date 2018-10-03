##
#
# File:    WorkflowSchemaReportTests.py
# Author:  J. Westbrook
# Date:    12-Feb-2015
# Version: 0.001
##
"""
Test cases for SQL select and report generation  using workflow schema -

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import unittest
import traceback
import time

from wwpdb.utils.db.MyDbSqlGen import MyDbQuerySqlGen, MyDbConditionSqlGen
from wwpdb.utils.db.WorkflowSchemaDef import WorkflowSchemaDef


class WorkflowSchemaReportTests(unittest.TestCase):

    def setUp(self):
        self.__lfh = sys.stdout
        self.__verbose = False

    def tearDown(self):
        pass

    def testSelect1(self):
        """Test case -  selection everything for a simple condition -
        """
        startTime = time.time()
        self.__lfh.write("\nStarting %s %s at %s\n" % (self.__class__.__name__,
                                                       sys._getframe().f_code.co_name,
                                                       time.strftime("%Y %m %d %H:%M:%S", time.localtime())))
        try:
            sd = WorkflowSchemaDef(verbose=self.__verbose, log=self.__lfh)
            tableIdList = sd.getTableIdList()
            sqlGen = MyDbQuerySqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)

            for tableId in tableIdList:
                aIdList = sd.getAttributeIdList(tableId)
                for aId in aIdList:
                    sqlGen.addSelectAttributeId(attributeTuple=(tableId, aId))

                if 'DEP_SET_ID' in aIdList:
                    sqlCondition = MyDbConditionSqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
                    sqlCondition.addValueCondition((tableId, "DEP_SET_ID"), 'EQ', ('D_1000000000', 'CHAR'))
                    sqlGen.setCondition(sqlCondition)
                if 'ORDINAL_ID' in aIdList:
                    sqlGen.addOrderByAttributeId(attributeTuple=(tableId, 'ORDINAL_ID'))
                sqlS = sqlGen.getSql()
                if (self.__verbose):
                    self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)
                sqlGen.clear()
        except:
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted %s %s at %s (%d seconds)\n" % (self.__class__.__name__,
                                                                     sys._getframe().f_code.co_name,
                                                                     time.strftime("%Y %m %d %H:%M:%S", time.localtime()),
                                                                     endTime - startTime))


def suiteSelect():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(WorkflowSchemaReportTests("testSelect1"))
    return suiteSelect


if __name__ == '__main__':
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
