##
#
# File:    PdbDistroSchemaReportTests.py
# Author:  J. Westbrook
# Date:    21-May-2015
# Version: 0.001
##
"""
Test cases for SQL select and report generation  using PDB Distro -

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
from wwpdb.utils.db.PdbDistroSchemaDef import PdbDistroSchemaDef


class PdbDistroSchemaReportTests(unittest.TestCase):
    def setUp(self):
        self.__lfh = sys.stdout
        self.__verbose = False

    def tearDown(self):
        pass

    def testSelect1(self):
        """Test case -  selection everything for a simple condition -
        """
        startTime = time.time()
        self.__lfh.write("\nStarting PdbDistroSchemaReportTests testSelect1 at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            # selection list  -
            sList = [("PDB_ENTRY_TMP", "PDB_ID"), ("REFINE", "LS_D_RES_LOW"), ("REFINE", "LS_R_FACTOR_R_WORK")]
            # condition value list -
            cList = [
                (("PDB_ENTRY_TMP", "PDB_ID"), "LIKE", ("x-ray", "char")),
                (("PDB_ENTRY_TMP", "STATUS_CODE"), "EQ", ("REL", "char")),
                (("PDB_ENTRY_TMP", "METHOD"), "NE", ("THEORETICAL_MODEL", "char")),
                (("PDBX_WEBSELECT", "ENTRY_TYPE"), "EQ", ("PROTEIN", "char")),
                (("PDBX_WEBSELECT", "CRYSTAL_TWIN"), "GT", (0, "int")),
                (("PDBX_WEBSELECT", "REFINEMENT_SOFTWARE"), "LIKE", ("REFMAC", "char")),
                (("PDBX_WEBSELECT", "DATE_OF_RCSB_RELEASE"), "GE", (1900, "date")),
                (("PDBX_WEBSELECT", "DATE_OF_RCSB_RELEASE"), "LE", (2014, "date")),
            ]
            #
            #
            gList = [
                ("OR", ("PDBX_WEBSELECT", "METHOD_TO_DETERMINE_STRUCT"), "LIKE", ("MOLECULAR REPLACEMENT", "char")),
                ("OR", ("PDBX_WEBSELECT", "METHOD_TO_DETERMINE_STRUCT"), "LIKE", ("MR", "char")),
            ]
            # attribute ordering list
            oList = [("PDB_ENTRY_TMP", "PDB_ID"), ("REFINE", "LS_D_RES_LOW"), ("REFINE", "LS_R_FACTOR_R_WORK")]

            sd = PdbDistroSchemaDef(verbose=self.__verbose, log=self.__lfh)
            # tableIdList = sd.getTableIdList()
            # aIdList = sd.getAttributeIdList(tableId)
            sqlGen = MyDbQuerySqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)

            sTableIdList = []
            for sTup in sList:
                sqlGen.addSelectAttributeId(attributeTuple=(sTup[0], sTup[1]))
                sTableIdList.append(sTup[0])

            sqlCondition = MyDbConditionSqlGen(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
            for cTup in cList:
                sqlCondition.addValueCondition(cTup[0], cTup[1], cTup[2])
            sqlCondition.addGroupValueConditionList(gList, preOp="AND")
            sqlCondition.addTables(sTableIdList)
            #
            sqlGen.setCondition(sqlCondition)
            for oTup in oList:
                sqlGen.addOrderByAttributeId(attributeTuple=oTup)
            sqlS = sqlGen.getSql()
            if self.__verbose:
                self.__lfh.write("\n\n+MyDbSqlGenTests table creation SQL string\n %s\n\n" % sqlS)
            sqlGen.clear()
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted PdbDistroSchemaReportTests testSelect1 at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))


def suiteSelect():
    suiteSelectA = unittest.TestSuite()
    suiteSelectA.addTest(PdbDistroSchemaReportTests("testSelect1"))
    return suiteSelectA


if __name__ == "__main__":
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
