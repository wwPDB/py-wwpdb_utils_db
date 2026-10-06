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

from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from wwpdb.utils.db.ChemCompSchemaDef import ChemCompSchemaDef
from wwpdb.utils.db.DaInternalSchemaDef import DaInternalSchemaDef
from wwpdb.utils.db.MessageSchemaDef import MessageSchemaDef
from wwpdb.utils.db.PdbDistroSchemaDef import PdbDistroSchemaDef
from wwpdb.utils.db.PdbxSchemaDef import PdbxSchemaDef
from wwpdb.utils.db.PrdChemCompSchemaDef import PrdChemCompSchemaDef
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef
from wwpdb.utils.db.WorkflowSchemaDef import WorkflowSchemaDef


class MiscSchemaReportTests(unittest.TestCase):
    def tearDown(self) -> None:
        pass

    def __dotest(self, sd: SchemaDefBase) -> None:
        tableIdList = sd.getTableIdList()
        self.assertGreater(len(tableIdList), 0)

        for tableId in tableIdList:
            _aIdL = sd.getAttributeIdList(tableId)  # noqa: F841
            tObj = sd.getTable(tableId)
            attributeIdList = tObj.getAttributeIdList()
            attributeNameList = tObj.getAttributeNameList()
            self.assertEqual(len(attributeIdList), len(attributeNameList))
            print("Ordered attribute Id   list %s" % (str(attributeIdList)))  # noqa: T201
            print("Ordered attribute name list %s" % (str(attributeNameList)))  # noqa: T201
            #
            mAL = tObj.getMapAttributeNameList()
            print("Ordered mapped attribute name list %s" % (str(mAL)))  # noqa: T201

            mAL = tObj.getMapAttributeIdList()
            print("Ordered mapped attribute id   list %s" % (str(mAL)))  # noqa: T201

            cL = tObj.getMapInstanceCategoryList()
            print("Mapped category list %s" % (str(cL)))  # noqa: T201
            for c in cL:
                aL = tObj.getMapInstanceAttributeList(c)
                print("Mapped attribute list in %s :  %s" % (c, str(aL)))  # noqa: T201

    def testChemCompSchema(self) -> None:
        """Test case -  chemCompSchema test"""
        sd = ChemCompSchemaDef()
        self.__dotest(sd)

    def testPdbxSchema(self) -> None:
        """Test case -  PdbxSchema test"""
        sd = PdbxSchemaDef()
        self.__dotest(sd)

    def testStatusHistorySchema(self) -> None:
        """Test case -  StatusHistorySchema test"""
        sd = StatusHistorySchemaDef()
        self.__dotest(sd)

    def testDaInternalSchema(self) -> None:
        """Test case -  DaInternalSchema test"""
        sd = DaInternalSchemaDef()
        self.__dotest(sd)
        sd = DaInternalSchemaDef(databaseName="da_internal_combined")
        self.assertEqual(sd.getDatabaseName(), "da_internal_combined")
        self.__dotest(sd)

    def testBirdSchema(self) -> None:
        """Test case -  BirdSchema test"""
        self.__dotest(BirdSchemaDef())

    def testMessageSchema(self) -> None:
        """Test case -  MessageSchema test"""
        self.__dotest(MessageSchemaDef())

    def testPdbDistroSchema(self) -> None:
        """Test case -  PdbDistroSchema test"""
        self.__dotest(PdbDistroSchemaDef())

    def testPrdChemCompSchema(self) -> None:
        """Test case -  PrdChemCompSchema test"""
        self.__dotest(PrdChemCompSchemaDef())

    def testWorkflowSchema(self) -> None:
        """Test case -  WorkflowSchema test"""
        self.__dotest(WorkflowSchemaDef())


def suiteSelect() -> unittest.TestSuite:  # pragma: no cover
    suiteSelectA = unittest.TestSuite()
    suiteSelectA.addTest(MiscSchemaReportTests("testChemCompSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testPdbxSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testDaInternalSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testStatusHistorySchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testBirdSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testMessageSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testPdbDistroSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testPrdChemCompSchema"))
    suiteSelectA.addTest(MiscSchemaReportTests("testWorkflowSchema"))
    return suiteSelectA


if __name__ == "__main__":  # pragma: no cover
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
