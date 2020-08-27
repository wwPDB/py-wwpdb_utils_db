##
#
# File:    MyQueryDirectivesTests.py
# Author:  J. Westbrook
# Date:    20-June-2015
# Version: 0.001
#
#  Updates:
#
#   09-Aug-2015  jdw add tests for multiple values dom references -
#   09-Aug-2015  jdw add status history tests
##
"""
Test cases for parsing query directives and producing SQL instructions.

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

from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery
from wwpdb.utils.db.PdbDistroSchemaDef import PdbDistroSchemaDef
from wwpdb.utils.db.DaInternalSchemaDef import DaInternalSchemaDef
from wwpdb.utils.db.MyQueryDirectives import MyQueryDirectives

from wwpdb.utils.testing.Features import Features


@unittest.skipUnless(Features().haveMySqlTestServer(), "require MySql Test Environment")
class MyQueryDirectivesTests(unittest.TestCase):
    def setUp(self):
        self.__databaseName = "stat"
        self.__lfh = sys.stdout
        self.__verbose = True
        self.__dbCon = None
        self.__domD = {
            "solution": "sad",
            "spaceg": "P 21 21 21",
            "software": "REFMAC",
            "date1": "2000",
            "date2": "2014",
            "reso1": "1.0",
            "reso2": "5.0",
            "rfree1": ".1",
            "rfree2": ".4",
            "solvent1": "2",
            "solvent2": "4",
            "weight1": "100",
            "weight2": "200",
            "twin": "1",
            "molecular_type": "protein",
            "molecular_type_list": ["protein", "RNA"],
            "xtype": "refine.ls_d_res_high",
            "ytype": "refine.ls_d_res_low",
            "source": "human",
            "multikey": "pdbx_webselect.space_group_name_H_M|refine.ls_d_res_high|refine.ls_d_res_low",
            "tax2": "9606|10090",
        }

        self.__qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.crystal_twin:COP:GT:VALUE:DOM_REF:twin",
            "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
            "VALUE_CONDITION:3:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
            "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
            "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",
            "VALUE_CONDITION:5:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:GE:VALUE:DOM_REF:date1",
            "VALUE_CONDITION:6:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:LE:VALUE:DOM_REF:date2",
            "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
            "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
            "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
            "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
            "VALUE_CONDITION:11:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:GE:VALUE:DOM_REF:solvent1",
            "VALUE_CONDITION:12:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:LE:VALUE:DOM_REF:solvent2",
            "VALUE_CONDITION:13:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:GE:VALUE:DOM_REF:weight1",
            "VALUE_CONDITION:14:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:LE:VALUE:DOM_REF:weight2",
            "JOIN_CONDITION:20:LOP:AND:L_ITEM:pdbx_webselect.Structure_ID:COP:EQ:R_ITEM:refine.ls_d_res_low",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        # ok = self.open()

    def tearDown(self):
        # self.close()
        pass

    def open(self, dbUserId=None, dbUserPwd=None):
        myC = MyDbConnect(dbName=self.__databaseName, dbUser=dbUserId, dbPw=dbUserPwd, verbose=self.__verbose, log=self.__lfh)
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            self.__lfh.write("\nDatabase connection opened MyQueryDirectivesTest open at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
            return True
        else:
            return False

    def close(self):
        if self.__dbCon is not None:
            self.__lfh.write("\nDatabase connection closed MyQueryDirectivesTest close at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
            self.__dbCon.close()

    def testDirective1(self):
        """Test case -  selection everything for a simple condition -"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyQueryDirectivesTest testDirective1 at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.open()
            sd = PdbDistroSchemaDef(verbose=self.__verbose, log=self.__lfh)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
            sqlS = mqd.build(queryDirL=self.__qdL, domD=self.__domD)
            if self.__verbose:
                self.__lfh.write("\n\n+testDirective1 SQL\n %s\n\n" % sqlS)
            self.close()
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write("\nCompleted MyQueryDirectivesTest testDirective1 at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime))

    def testDirectiveWithQuery0(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.crystal_twin:COP:GT:VALUE:DOM_REF:twin",
            "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
            "VALUE_CONDITION:3:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
            "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
            "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",
            "VALUE_CONDITION:5:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:GE:VALUE:DOM_REF:date1",
            "VALUE_CONDITION:6:LOP:AND:ITEM:pdbx_webselect.date_of_RCSB_release:COP:LE:VALUE:DOM_REF:date2",
            "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
            "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
            "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
            "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
            "VALUE_CONDITION:11:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:GE:VALUE:DOM_REF:solvent1",
            "VALUE_CONDITION:12:LOP:AND:ITEM:pdbx_webselect.solvent_content:COP:LE:VALUE:DOM_REF:solvent2",
            "VALUE_CONDITION:13:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:GE:VALUE:DOM_REF:weight1",
            "VALUE_CONDITION:14:LOP:AND:ITEM:pdbx_webselect.weight_in_ASU:COP:LE:VALUE:DOM_REF:weight2",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery1(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery2(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "JOIN_CONDITION:1:LOP:AND:L_ITEM:pdbx_webselect.Structure_ID:COP:EQ:R_ITEM:refine.Structure_ID",
            "VALUE_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE:DOM_REF:molecular_type",
            "VALUE_CONDITION:7:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:GE:VALUE:DOM_REF:reso1",
            "VALUE_CONDITION:8:LOP:AND:ITEM:pdbx_webselect.ls_d_res_high:COP:LE:VALUE:DOM_REF:reso2",
            "VALUE_CONDITION:9:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:GE:VALUE:DOM_REF:rfree1",
            "VALUE_CONDITION:10:LOP:AND:ITEM:pdbx_webselect.R_value_R_free:COP:LE:VALUE:DOM_REF:rfree2",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery3(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "VALUE_CONDITION:4:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
            "VALUE_KEYED_CONDITION:15:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:solution",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:mr:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:sad:LOP:OR:ITEM:pdbx_webselect.method_to_determine_struct:COP:LIKE:VALUE:%MAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MR%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MOLECULAR REPLACEMENT%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%SAD%",
            "CONDITION_LIST:1:KEY:other:LOP:AND:ITEM:pdbx_webselect.method_to_determine_struct:COP:NOT LIKE:VALUE:%MAD%",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery4(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.refinement_software:COP:LIKE:VALUE:DOM_REF:software",
            "VALUE_LIST_CONDITION:2:LOP:AND:ITEM:pdbx_webselect.entry_type:COP:EQ:VALUE_LOP:AND:VALUE_LIST:DOM_REF:molecular_type_list",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery5(self):
        # broken -- problem with automatic addition of equi-join conditions.
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF:xtype",
            "SELECT_ITEM:2:ITEM:DOM_REF:ytype",
            "VALUE_CONDITION:1:LOP:AND:ITEM:pdbx_webselect.space_group_name_H_M:COP:EQ:VALUE:DOM_REF:spaceg",
            "VALUE_KEYED_CONDITION:2:LOP:AND:CONDITION_LIST_ID:2:VALUE:DOM_REF:source",
            "CONDITION_LIST:2:KEY:human:LOP:OR:ITEM:entity_src_gen.pdbx_gene_src_ncbi_taxonomy_id:COP:EQ:VALUE:9606",
            "CONDITION_LIST:2:KEY:human:LOP:OR:ITEM:entity_src_nat.pdbx_ncbi_taxonomy_id:COP:EQ:VALUE:9606",
            "ORDER_ITEM:1:ITEM:DOM_REF:xtype:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF:ytype:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery6(self):
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF_0:multikey",
            "SELECT_ITEM:2:ITEM:DOM_REF_1:multikey",
            "SELECT_ITEM:3:ITEM:DOM_REF_2:multikey",
            "ORDER_ITEM:1:ITEM:DOM_REF_1:multikey:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF_2:multikey:SORT_ORDER:DECREASING",
        ]
        self.__testDirectiveWithDistroQuery(qdL=qdL, domD=self.__domD)

    def testDirectiveWithQuery7(self):
        """
        pdbx_database_status_history

         "ATTRIBUTES": {
                "ORDINAL": "ordinal",
                "ENTRY_ID": "entry_id",
                "PDB_ID": "pdb_id",
                "DATE_BEGIN": "date_begin",
                "DATE_END": "date_end",
                "STATUS_CODE_BEGIN": "status_code_begin",
                "STATUS_CODE_END": "status_code_end",
                "ANNOTATOR": "annotator",
                "DETAILS": "details",
                "DELTA_DAYS": "delta_days",
            },
        """
        myDomD = {
            "multiselect": "pdbx_database_status_history.entry_id|pdbx_database_status_history.pdb_id|pdbx_database_status_history.status_code_begin|pdbx_database_status_history.status_code_end",  # noqa: E501
            "endstat1": "hpuborhold",
            "beginstat1": "auth",
        }
        qdL = [
            "SELECT_ITEM:1:ITEM:DOM_REF_0:multiselect",
            "SELECT_ITEM:2:ITEM:DOM_REF_1:multiselect",
            "SELECT_ITEM:3:ITEM:DOM_REF_2:multiselect",
            "SELECT_ITEM:4:ITEM:DOM_REF_3:multiselect",
            "VALUE_KEYED_CONDITION:5:LOP:AND:CONDITION_LIST_ID:2:VALUE:DOM_REF:beginstat1",
            "VALUE_KEYED_CONDITION:6:LOP:AND:CONDITION_LIST_ID:1:VALUE:DOM_REF:endstat1",
            "CONDITION_LIST:1:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:HOLD",
            "CONDITION_LIST:1:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:HPUB",
            "CONDITION_LIST:1:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:PROC",
            "CONDITION_LIST:1:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:PROC_ST_1",
            "CONDITION_LIST:1:KEY:auth:LOP:OR:ITEM:pdbx_database_status_history.status_code_end:COP:EQ:VALUE:AUTH",
            "CONDITION_LIST:2:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:HOLD",
            "CONDITION_LIST:2:KEY:hpuborhold:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:HPUB",
            "CONDITION_LIST:2:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:PROC",
            "CONDITION_LIST:2:KEY:procorst1:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:PROC_ST_1",
            "CONDITION_LIST:2:KEY:auth:LOP:OR:ITEM:pdbx_database_status_history.status_code_begin:COP:EQ:VALUE:AUTH",
            "ORDER_ITEM:1:ITEM:DOM_REF_0:multiselect:SORT_ORDER:INCREASING",
            "ORDER_ITEM:2:ITEM:DOM_REF_1:multiselect:SORT_ORDER:INCREASING",
        ]
        self.__testDirectiveWithHistoryQuery(qdL=qdL, domD=myDomD)

    def __testDirectiveWithDistroQuery(self, qdL, domD):
        """Test case -  selection everything for a simple condition - (Distro Schema)"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyQueryDirectivesTest __testDirectiveWithDistroQuery at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.__databaseName = "stat"
            self.open()
            sd = PdbDistroSchemaDef(verbose=self.__verbose, log=self.__lfh)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
            sqlS = mqd.build(queryDirL=qdL, domD=domD, appendValueConditonsToSelect=True)
            if self.__verbose:
                self.__lfh.write("\n\n+testDirectiveWithDistroQuery SQL\n %s\n\n" % sqlS)
            self.__lfh.flush()
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            rowList = myQ.selectRows(queryString=sqlS)
            if self.__verbose:
                self.__lfh.write("\n+testDirectiveWithDistroQuery mysql server returns row length %d\n" % len(rowList))
                self.__lfh.flush()
                for ii, row in enumerate(rowList[:30]):
                    self.__lfh.write("   %6d  %r\n" % (ii, row))
            self.close()
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted MyQueryDirectivesTest __testDirectiveWithDistroQuery at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )

    def __testDirectiveWithHistoryQuery(self, qdL, domD):
        """Test case -  selection everything for a simple condition -"""
        startTime = time.time()
        self.__lfh.write("\nStarting MyQueryDirectivesTest __testDirectiveWithHistoryQuery at %s\n" % time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        try:
            self.__databaseName = "da_internal"
            self.open()
            sd = DaInternalSchemaDef(verbose=self.__verbose, log=self.__lfh)
            mqd = MyQueryDirectives(schemaDefObj=sd, verbose=self.__verbose, log=self.__lfh)
            sqlS = mqd.build(queryDirL=qdL, domD=domD, appendValueConditonsToSelect=True)
            if self.__verbose:
                self.__lfh.write("\n\n+testDirectiveWithHistoryQuery SQL\n %s\n\n" % sqlS)
            self.__lfh.flush()
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            rowList = myQ.selectRows(queryString=sqlS)
            if self.__verbose:
                self.__lfh.write("\n+testDirectiveWithHistoryQuery mysql server returns row length %d\n" % len(rowList))
                self.__lfh.flush()
                for ii, row in enumerate(rowList[:30]):
                    self.__lfh.write("   %6d  %r\n" % (ii, row))
            self.close()
        except:  # noqa: E722  pylint: disable=bare-except
            traceback.print_exc(file=self.__lfh)
            self.fail()

        endTime = time.time()
        self.__lfh.write(
            "\nCompleted MyQueryDirectivesTest __testDirectiveWithHistoryQuery at %s (%d seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        )


def suiteSelect1():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyQueryDirectivesTests("testDirective1"))
    return suiteSelect


def suiteSelectQuery():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery1"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery2"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery3"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery4"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery5"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery6"))
    suiteSelect.addTest(MyQueryDirectivesTests("testDirectiveWithQuery7"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = suiteSelect1()
    unittest.TextTestRunner(verbosity=2).run(mySuite)

    mySuite = suiteSelectQuery()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
