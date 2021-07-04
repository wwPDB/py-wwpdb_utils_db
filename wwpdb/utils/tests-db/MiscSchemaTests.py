##
#
# File:    MiscSchemaTests.py
# Author:  E.Peisach
# Date:    26-Jan-2020
# Version: 0.001
##
"""
Simple tests of various schema
"""

import unittest

from wwpdb.utils.db.ChemCompSchemaDef import ChemCompSchemaDef
from wwpdb.utils.db.DaInternalSchemaDef import DaInternalSchemaDef
from wwpdb.utils.db.PdbxSchemaDef import PdbxSchemaDef
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef


class MiscSchemaReportTests(unittest.TestCase):
    def tearDown(self):
        pass

    def __dotest(self, sd):
        tableIdList = sd.getTableIdList()

        for tableId in tableIdList:
            _aIdL = sd.getAttributeIdList(tableId)  # noqa: F841
            tObj = sd.getTable(tableId)
            attributeIdList = tObj.getAttributeIdList()
            attributeNameList = tObj.getAttributeNameList()
            print("Ordered attribute Id   list %s" % (str(attributeIdList)))
            print("Ordered attribute name list %s" % (str(attributeNameList)))
            #
            mAL = tObj.getMapAttributeNameList()
            print("Ordered mapped attribute name list %s" % (str(mAL)))

            mAL = tObj.getMapAttributeIdList()
            print("Ordered mapped attribute id   list %s" % (str(mAL)))

            cL = tObj.getMapInstanceCategoryList()
            print("Mapped category list %s" % (str(cL)))
            for c in cL:
                aL = tObj.getMapInstanceAttributeList(c)
                print("Mapped attribute list in %s :  %s" % (c, str(aL)))

    def testChemCompSchema(self):
        """Test case -  chemCompSchema test"""
        sd = ChemCompSchemaDef()
        self.__dotest(sd)

    def testPdbxSchema(self):
        """Test case -  chemCompSchema test"""
        sd = PdbxSchemaDef()
        self.__dotest(sd)

    def testStatusHistorySchema(self):
        """Test case -  StatusHistorySchema test"""
        sd = StatusHistorySchemaDef()
        self.__dotest(sd)

    def testDaInternalSchema(self):
        """Test case -  DaInternalSchema test"""
        sd = DaInternalSchemaDef()
        self.__dotest(sd)
        sd = DaInternalSchemaDef(databaseName="da_internal_combined")
        self.__dotest(sd)


def suiteSelect():  # pragma: no cover
    suiteSelectA = unittest.TestSuite()
    suiteSelectA.addTest(MiscSchemaReportTests("testChemCompSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testPdbxSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testDaInternalSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testStatusHistorySchema"))
    return suiteSelectA


if __name__ == "__main__":  # pragma: no cover
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
