##
# File:    SchemaDefLoaderMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Tests for SchemaDefLoader using in-memory PDBx containers and a mocked database query layer.
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import copy
import io
import os
import shutil
import tempfile
import unittest
from typing import Any, Dict, List, Tuple, cast
from unittest import mock

from mmcif.api.DataCategory import DataCategory
from mmcif.api.PdbxContainers import DataContainer
from mmcif.io.IoAdapterPy import IoAdapterPy

from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase, SchemaDictType, TableDefDict
from wwpdb.utils.db.SchemaDefLoader import SchemaDefLoader

HERE = os.path.abspath(os.path.dirname(__file__))

_STR: Dict[str, Any] = {"SQL_TYPE": "VARCHAR", "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False}

_SCHEMA: SchemaDictType = {
    "ENTITY": {
        "TABLE_ID": "ENTITY",
        "TABLE_NAME": "entity",
        "TABLE_TYPE": "transactional",
        "ATTRIBUTES": {
            "STRUCTURE_ID": "Structure_ID",
            "ENTITY_ID": "id",
            "DESCRIPTION": "pdbx_description",
            "FORMULA_WEIGHT": "formula_weight",
            "POLY_TYPE": "poly_type",
        },
        "ATTRIBUTE_INFO": {
            "STRUCTURE_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
            "ENTITY_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 2},
            "DESCRIPTION": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
            "FORMULA_WEIGHT": {"SQL_TYPE": "FLOAT", "WIDTH": 10, "PRECISION": 2, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
            "POLY_TYPE": {"SQL_TYPE": "VARCHAR", "WIDTH": 20, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 5},
        },
        "ATTRIBUTE_MAP": {
            "STRUCTURE_ID": (None, None, "datablockid()", None),
            "ENTITY_ID": ("entity", "id", None, None),
            "DESCRIPTION": ("entity", "pdbx_description", None, None),
            "FORMULA_WEIGHT": ("entity", "formula_weight", None, None),
            "POLY_TYPE": ("entity_poly", "type", None, None),
        },
        "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ("STRUCTURE_ID", "ENTITY_ID")}},
        "MAP_MERGE_INDICES": {"entity": {"TYPE": "EQUI-JOIN", "ATTRIBUTES": ("id",)}, "entity_poly": {"TYPE": "EQUI-JOIN", "ATTRIBUTES": ("entity_id",)}},
        "TABLE_DELETE_ATTRIBUTE": "STRUCTURE_ID",
    },
    "STRUCT": {
        "TABLE_ID": "STRUCT",
        "TABLE_NAME": "struct",
        "TABLE_TYPE": "transactional",
        "ATTRIBUTES": {"STRUCTURE_ID": "Structure_ID", "TITLE": "title", "MISSING_ATT": "missing_att", "NUM": "num"},
        "ATTRIBUTE_INFO": {
            "STRUCTURE_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
            "TITLE": {"SQL_TYPE": "VARCHAR", "WIDTH": 8, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
            "MISSING_ATT": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 3},
            "NUM": {"SQL_TYPE": "INT", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 4},
        },
        "ATTRIBUTE_MAP": {
            "STRUCTURE_ID": (None, None, "datablockid()", None),
            "TITLE": ("struct", "title", None, None),
            "MISSING_ATT": ("struct", "not_there", None, None),
            "NUM": ("struct", "num", None, None),
        },
        "INDICES": {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": ("STRUCTURE_ID",)}},
        "TABLE_DELETE_ATTRIBUTE": "STRUCTURE_ID",
    },
    # No INDICES - not needed for loading
    "PDBX_CHEM_COMP_DESCRIPTOR": cast(
        TableDefDict,
        {
            "TABLE_ID": "PDBX_CHEM_COMP_DESCRIPTOR",
            "TABLE_NAME": "pdbx_chem_comp_descriptor",
            "TABLE_TYPE": "transactional",
            "ATTRIBUTES": {"STRUCTURE_ID": "Structure_ID", "DESCRIPTOR": "descriptor"},
            "ATTRIBUTE_INFO": {
                "STRUCTURE_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "DESCRIPTOR": {"SQL_TYPE": "TEXT", "WIDTH": 200, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
            },
            "ATTRIBUTE_MAP": {
                "STRUCTURE_ID": (None, None, "datablockid()", None),
                "DESCRIPTOR": ("pdbx_chem_comp_descriptor", "descriptor", None, None),
            },
            "TABLE_DELETE_ATTRIBUTE": "STRUCTURE_ID",
        },
    ),
    # No TABLE_TYPE or INDICES - not needed for loading
    "UNMAPPED": cast(
        TableDefDict,
        {
            "TABLE_ID": "UNMAPPED",
            "TABLE_NAME": "unmapped",
            "ATTRIBUTES": {"STRUCTURE_ID": "Structure_ID", "OTHER": "other"},
            "ATTRIBUTE_INFO": {
                "STRUCTURE_ID": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": False, "PRIMARY_KEY": True, "ORDER": 1},
                "OTHER": {"SQL_TYPE": "VARCHAR", "WIDTH": 10, "PRECISION": 0, "NULLABLE": True, "PRIMARY_KEY": False, "ORDER": 2},
            },
            # Function mapping that is not supported - ignored
            "ATTRIBUTE_MAP": {"STRUCTURE_ID": (None, None, "datablockid()", None), "OTHER": (None, None, "unknownfunc()", None)},
            "TABLE_DELETE_ATTRIBUTE": "STRUCTURE_ID",
        },
    ),
}

ROWSEP = "$##$\n"
COLSEP = "&##&\t"


def _makeContainers() -> List[DataContainer]:
    c1 = DataContainer("D1")
    c1.append(DataCategory("entity", ["id", "pdbx_description", "formula_weight"], [["1", "Protein A long name", "1000.5"], ["2", "?", "."]]))
    c1.append(DataCategory("entity_poly", ["entity_id", "type"], [["1", "polypeptide(L)"]]))
    c1.append(DataCategory("struct", ["title", "num"], [["My title is long", "5"]]))
    c1.append(DataCategory("pdbx_chem_comp_descriptor", ["descriptor"], [["C\\C=C"]]))
    c2 = DataContainer("D2")
    c2.append(DataCategory("struct", ["title", "num"], [["t2", "?"]]))
    return [c1, c2]


class SchemaDefLoaderMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__workPath = tempfile.mkdtemp()
        self.__sd = SchemaDefBase(databaseName="testdb", schemaDefDict=copy.deepcopy(_SCHEMA), verbose=False)
        self.__containers = _makeContainers()
        self.__ioObj = mock.MagicMock()
        self.__ioObj.readFile.side_effect = lambda path: [c for c in self.__containers if c.getName() == os.path.basename(path)]
        self.__lfh = io.StringIO()

    def tearDown(self) -> None:
        shutil.rmtree(self.__workPath, ignore_errors=True)

    def __loader(self, verbose: bool = True, cleanUp: bool = False, dbCon: Any = None) -> SchemaDefLoader:
        return SchemaDefLoader(
            schemaDefObj=self.__sd,
            ioObj=self.__ioObj,
            dbCon=dbCon,
            workPath=self.__workPath,
            cleanUp=cleanUp,
            warnings="default",
            verbose=verbose,
            log=self.__lfh,
        )

    def __readFile(self, path: str) -> str:
        with open(path) as ifh:
            return ifh.read()

    # ---------------------------------------------------------------- data mapping

    def testFetchMapping(self) -> None:
        sdl = self.__loader()
        tD, nameList = sdl.fetch(["/x/D1", "/x/D2"])
        self.assertEqual(nameList, ["D1", "D2"])
        self.assertEqual(self.__ioObj.readFile.call_count, 2)
        self.assertEqual(sorted(tD.keys()), ["ENTITY", "PDBX_CHEM_COMP_DESCRIPTOR", "STRUCT", "UNMAPPED"])
        # single category mapping with width truncation, null handling and missing attribute
        self.assertEqual(
            tD["STRUCT"],
            [
                {"STRUCTURE_ID": "D1", "TITLE": "My title", "MISSING_ATT": "", "NUM": "5"},
                {"STRUCTURE_ID": "D2", "TITLE": "t2", "MISSING_ATT": "", "NUM": r"\N"},
            ],
        )
        self.assertEqual(tD["PDBX_CHEM_COMP_DESCRIPTOR"], [{"STRUCTURE_ID": "D1", "DESCRIPTOR": "C\\C=C"}])
        # No instance category - no rows
        self.assertEqual(tD["UNMAPPED"], [])
        # merged categories -- one row per merge key value
        self.assertEqual(len(tD["ENTITY"]), 2)
        self.assertEqual([r["STRUCTURE_ID"] for r in tD["ENTITY"]], ["D1", "D1"])
        self.assertIn("+SchemaDefLoader(__fetch) completed", self.__lfh.getvalue())
        # width overflow in a merged category is reported
        self.assertIn("+ERROR - Table ENTITY attribute DESCRIPTION length 19 exceeds 10", self.__lfh.getvalue())

    @unittest.expectedFailure
    def testMergedCategoriesKeepAllValues(self) -> None:
        """BUG: SchemaDefLoader.__mapInstanceCategoryList initializes each contributing category row with
        null values for ALL attributes and then dict.update()s the merged row, so the category processed
        last overwrites values mapped from the earlier categories with nulls.
        """
        sdl = self.__loader(verbose=False)
        tD, _ = sdl.process(self.__containers)
        # Merge key for entity 1 is the first row (entity 2 has no entity_poly row)
        row = tD["ENTITY"][0]
        self.assertEqual(row["ENTITY_ID"], "1")
        self.assertEqual(row["POLY_TYPE"], "polypeptide(L)")
        self.assertEqual(row["DESCRIPTION"], "Protein A ")

    def testProcessContainerList(self) -> None:
        sdl = self.__loader()
        tD, nameList = sdl.process(self.__containers)
        self.assertEqual(nameList, ["D1", "D2"])
        self.assertEqual(len(tD["STRUCT"]), 2)
        self.assertIn("+SchemaDefLoader(__process) completed", self.__lfh.getvalue())
        self.__ioObj.readFile.assert_not_called()

    def testMappingException(self) -> None:
        """A None value cannot be processed and is reported - row is still returned with null values"""
        c = DataContainer("D3")
        c.append(DataCategory("struct", ["title", "num"], [[None, "7"]]))
        sdl = self.__loader(verbose=True)
        tD, _ = sdl.process([c])
        self.assertEqual(tD["STRUCT"], [{"STRUCTURE_ID": "D3", "TITLE": "", "MISSING_ATT": "", "NUM": "7"}])
        self.assertIn("+ERROR - processing table STRUCT attribute TITLE", self.__lfh.getvalue())

    def testMergedCategoryMissingAttributes(self) -> None:
        """Missing merge key and mapped attributes in a contributing category are tolerated"""
        c = DataContainer("D4")
        c.append(DataCategory("entity_poly", ["other"], [["x"]]))
        sdl = self.__loader(verbose=False)
        tD, _ = sdl.process([c])
        # Single row with an empty merge key and null values
        self.assertEqual(tD["ENTITY"], [{"STRUCTURE_ID": "D4", "ENTITY_ID": "", "DESCRIPTION": "", "FORMULA_WEIGHT": r"\N", "POLY_TYPE": ""}])

    def testMaximumWidthTracking(self) -> None:
        """The largest over-width value per attribute is reported by load()"""
        c = DataContainer("D5")
        c.append(DataCategory("struct", ["title", "num"], [["123456789", "1"], ["1234567890123", "2"], ["1234567890", "3"]]))
        sdl = self.__loader(verbose=True)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlBatchTemplateCommand.return_value = True
            self.assertTrue(sdl.load(containerList=[c], loadType="batch-insert"))
        self.assertIn("+SchemaDefLoader(load) ('STRUCT', 'TITLE') maximum width 13", self.__lfh.getvalue())

    def testDefaultIoAdapter(self) -> None:
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.IoAdapterCore") as mockIo:
            mockIo.return_value.readFile.return_value = []
            sdl = SchemaDefLoader(schemaDefObj=self.__sd, verbose=False, log=self.__lfh)
            tD, nameList = sdl.fetch(["somefile.cif"])
        mockIo.assert_called_once_with()
        mockIo.return_value.readFile.assert_called_once_with("somefile.cif")
        self.assertEqual(nameList, [])
        # No containers read - no tables populated
        self.assertEqual(tD, {})

    def testFetchRealBirdData(self) -> None:
        sdl = SchemaDefLoader(schemaDefObj=BirdSchemaDef(verbose=False), ioObj=IoAdapterPy(), workPath=self.__workPath, verbose=False, log=self.__lfh)
        pathList = [os.path.join(HERE, "data", "PRD", "PRD_000001.cif"), os.path.join(HERE, "data", "PRD", "PRD_000012.cif")]
        tD, nameList = sdl.fetch(pathList)
        self.assertEqual(nameList, ["PRD_000001", "PRD_000012"])
        self.assertEqual([r["PRD_ID"] for r in tD["PDBX_REFERENCE_MOLECULE"]], ["PRD_000001", "PRD_000012"])
        self.assertEqual(tD["PDBX_REFERENCE_MOLECULE"][0]["NAME"], "Actinomycin D")
        self.assertEqual(len(tD["PDBX_PRD_AUDIT"]), 4)

    # ---------------------------------------------------------------- load file creation

    def testMakeLoadFiles(self) -> None:
        sdl = self.__loader(verbose=False)
        nameList, exportList = sdl.makeLoadFiles(["D1", "D2"])
        self.assertEqual(nameList, ["D1", "D2"])
        exportD = dict(exportList)
        self.assertEqual(sorted(exportD.keys()), ["ENTITY", "PDBX_CHEM_COMP_DESCRIPTOR", "STRUCT"])
        self.assertEqual(exportD["STRUCT"], os.path.join(self.__workPath, "STRUCT-loadable-1.tdd"))
        self.assertEqual(
            self.__readFile(exportD["STRUCT"]),
            "D1" + COLSEP + "My title" + COLSEP + COLSEP + "5" + ROWSEP + "D2" + COLSEP + "t2" + COLSEP + COLSEP + r"\N" + ROWSEP,
        )
        # backslashes are escaped for chemical descriptors
        self.assertEqual(self.__readFile(exportD["PDBX_CHEM_COMP_DESCRIPTOR"]), "D1" + COLSEP + "C\\\\C=C" + ROWSEP)
        self.assertFalse(os.path.exists(os.path.join(self.__workPath, "UNMAPPED-loadable-1.tdd")))

    def testMakeLoadFilesAppendAndPart(self) -> None:
        sdl = self.__loader(verbose=False)
        _, exportList = sdl.makeLoadFiles(["D2"], partName="3")
        _, exportList = sdl.makeLoadFiles(["D2"], append=True, partName="3")
        self.assertEqual(exportList, [("STRUCT", os.path.join(self.__workPath, "STRUCT-loadable-3.tdd"))])
        content = self.__readFile(exportList[0][1])
        self.assertEqual(content.count(ROWSEP), 2)
        # Overwrite mode resets
        _, exportList = sdl.makeLoadFiles(["D2"], append=False, partName="3")
        self.assertEqual(self.__readFile(exportList[0][1]).count(ROWSEP), 1)

    def testSetDelimiters(self) -> None:
        sdl = self.__loader(verbose=False)
        self.assertTrue(sdl.setDelimiters(colSep="|", rowSep="\n"))
        _, exportList = sdl.makeLoadFiles(["D2"])
        self.assertEqual(self.__readFile(exportList[0][1]), "D2|t2||\\N\n")
        # Reset to defaults
        self.assertTrue(sdl.setDelimiters())
        _, exportList = sdl.makeLoadFiles(["D2"])
        self.assertEqual(self.__readFile(exportList[0][1]), "D2" + COLSEP + "t2" + COLSEP + COLSEP + r"\N" + ROWSEP)

    def testExport(self) -> None:
        sdl = self.__loader(verbose=False)
        sdl.setDelimiters(colSep="|", rowSep="\n")
        tD: Dict[str, List[Dict[str, str]]] = {
            "STRUCT": [{"STRUCTURE_ID": "X1", "TITLE": "a", "MISSING_ATT": "b", "NUM": "1"}],
            "ENTITY": [],
        }
        exportList = sdl.export(tD, partName="9")
        self.assertEqual(exportList, [("STRUCT", os.path.join(self.__workPath, "STRUCT-loadable-9.tdd"))])
        # export() always uses the default delimiters
        self.assertEqual(self.__readFile(exportList[0][1]), COLSEP.join(["X1", "a", "b", "1"]) + ROWSEP)

    def testMultiProcInterfaces(self) -> None:
        sdl = self.__loader(verbose=False)
        dataList, nameList, exportList, diagList = sdl.makeLoadFilesMulti(["D1", "D2"], "proc2", {}, self.__workPath)
        self.assertEqual(dataList, ["D1", "D2"])
        self.assertEqual(nameList, ["D1", "D2"])
        self.assertIn(("STRUCT", os.path.join(self.__workPath, "STRUCT-loadable-proc2.tdd")), exportList)
        self.assertEqual(diagList, [])
        #
        dataList, nameList2, tdList, diagList = sdl.fetchMulti(["D2"], "proc1", {}, self.__workPath)
        self.assertEqual(dataList, ["D2"])
        self.assertEqual(nameList2, ["D2"])
        self.assertEqual(len(tdList), 1)
        self.assertEqual(tdList[0]["STRUCT"][0]["TITLE"], "t2")
        self.assertEqual(diagList, [])

    # ---------------------------------------------------------------- database loading (mocked)

    def __commands(self, mockQ: mock.MagicMock) -> List[List[str]]:
        return [c.kwargs["sqlCommandList"] for c in mockQ.return_value.sqlCommand.call_args_list]

    def testSetWarning(self) -> None:
        sdl = self.__loader(verbose=False)
        self.assertTrue(sdl.setWarning("error"))
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            sdl.delete("STRUCT")
        mockQ.return_value.setWarning.assert_called_with("error")
        self.assertFalse(sdl.setWarning("bogus"))
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            sdl.delete("STRUCT")
        mockQ.return_value.setWarning.assert_called_with("default")

    def testLoadBatchFile(self) -> None:
        dbCon = mock.MagicMock()
        sdl = self.__loader(verbose=True, dbCon=dbCon)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            ok = sdl.load(inputPathList=["D1", "D2"], loadType="batch-file", deleteOpt="all")
        self.assertTrue(ok)
        self.assertEqual(mockQ.call_args.kwargs["dbcon"], dbCon)
        cmdD = {cmds[0].split()[2]: cmds for cmds in self.__commands(mockQ)}
        self.assertEqual(sorted(cmdD.keys()), ["testdb.entity;", "testdb.pdbx_chem_comp_descriptor;", "testdb.struct;"])
        structCmds = cmdD["testdb.struct;"]
        self.assertEqual(len(structCmds), 2)
        self.assertEqual(structCmds[0].strip(), "TRUNCATE TABLE testdb.struct;")
        self.assertIn("LOAD DATA LOCAL INFILE '%s'" % os.path.join(self.__workPath, "STRUCT-loadable-1.tdd"), structCmds[1])
        self.assertIn("(Structure_ID,title,missing_att,num)", structCmds[1])
        # Files are retained
        self.assertTrue(os.path.exists(os.path.join(self.__workPath, "STRUCT-loadable-1.tdd")))
        log = self.__lfh.getvalue()
        self.assertIn("+SchemaDefLoader(load) ('STRUCT', 'TITLE') maximum width 16", log)
        self.assertIn("+SchemaDefLoader(__batchFileImport) table STRUCT server returns True", log)

    def testLoadBatchFileAppendCleanup(self) -> None:
        sdl = self.__loader(verbose=False, cleanUp=True)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            ok = sdl.load(containerList=self.__containers[1:], loadType="batch-file-append")
        self.assertTrue(ok)
        cmds = self.__commands(mockQ)
        # No delete option - only the import command
        self.assertEqual(len(cmds), 1)
        self.assertEqual(len(cmds[0]), 1)
        self.assertTrue(cmds[0][0].startswith("LOAD DATA LOCAL INFILE"))
        # Load files were removed
        self.assertEqual(os.listdir(self.__workPath), [])

    @unittest.expectedFailure
    def testLoadBatchFileSelectedDelete(self) -> None:
        """BUG: __batchFileImport() ignores containerNameList (passes None to __getSqlDeleteList) so
        deleteOpt='selected' never generates the DELETE statements for the loaded containers."""
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            sdl.load(inputPathList=["D2"], loadType="batch-file", deleteOpt="selected")
        cmds = self.__commands(mockQ)
        self.assertTrue(cmds[0][0].startswith("DELETE FROM testdb.struct"))

    def testLoadBatchInsert(self) -> None:
        sdl = self.__loader(verbose=True)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlBatchTemplateCommand.return_value = True
            ok = sdl.load(containerList=self.__containers, loadType="batch-insert", deleteOpt="selected")
        self.assertTrue(ok)
        calls = mockQ.return_value.sqlBatchTemplateCommand.call_args_list
        # all tables are processed with a selected delete - even without data
        self.assertEqual(len(calls), 4)
        callD: Dict[str, Tuple[Any, ...]] = {}
        for c in calls:
            prepend = c.kwargs["prependSqlList"]
            self.assertEqual(len(prepend), 1)
            callD[prepend[0].split()[2]] = (c.args[0], prepend[0])
        insertList, delSql = callD["testdb.struct"]
        self.assertEqual(delSql.strip(), "DELETE FROM testdb.struct WHERE Structure_ID IN ('D1','D2');")
        self.assertEqual(
            insertList,
            [
                ("INSERT INTO testdb.struct (Structure_ID,title,num) VALUES (%s,%s,%s);", ["D1", "My title", "5"]),
                ("INSERT INTO testdb.struct (Structure_ID,title) VALUES (%s,%s);", ["D2", "t2"]),
            ],
        )
        self.assertEqual(callD["testdb.unmapped"][0], [])
        self.assertIn("batch insert completed for table struct rows 2", self.__lfh.getvalue())

    def testLoadBatchInsertNoDelete(self) -> None:
        sdl = self.__loader(verbose=True)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlBatchTemplateCommand.return_value = False
            ok = sdl.load(containerList=self.__containers[1:], loadType="batch-insert")
        # load does not propagate the insert status
        self.assertTrue(ok)
        calls = mockQ.return_value.sqlBatchTemplateCommand.call_args_list
        # Only the table with data is loaded
        self.assertEqual(len(calls), 1)
        self.assertIsNone(calls[0].kwargs["prependSqlList"])
        self.assertIn("batch insert fails for table struct length 1", self.__lfh.getvalue())

    def testLoadUnknownType(self) -> None:
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            self.assertFalse(sdl.load(inputPathList=["D1"], loadType="bogus"))
        mockQ.assert_not_called()

    @unittest.expectedFailure
    def testLoadNoInput(self) -> None:
        """BUG: load() with neither inputPathList nor containerList sets tableDataDict to a list
        and then calls .items() on it -> AttributeError instead of a no-op/False."""
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery"):
            self.assertTrue(sdl.load(loadType="batch-insert"))

    def testLoadBatchData(self) -> None:
        sdl = self.__loader(verbose=False)
        rowList = [{"STRUCTURE_ID": "Z1", "TITLE": "", "MISSING_ATT": r"\N", "NUM": "3"}]
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlBatchTemplateCommand.return_value = True
            self.assertTrue(sdl.loadBatchData("STRUCT", rowList=rowList, deleteOpt="all"))
            self.assertTrue(sdl.loadBatchData("STRUCT", rowList=rowList, containerNameList=None, deleteOpt="selected"))
        c1, c2 = mockQ.return_value.sqlBatchTemplateCommand.call_args_list
        self.assertEqual(c1.args[0], [("INSERT INTO testdb.struct (Structure_ID,num) VALUES (%s,%s);", ["Z1", "3"])])
        self.assertEqual([s.strip() for s in c1.kwargs["prependSqlList"]], ["TRUNCATE TABLE testdb.struct;"])
        # 'selected' without a container list does not delete
        self.assertIsNone(c2.kwargs["prependSqlList"])

    def testLoadBatchFiles(self) -> None:
        sdl = self.__loader(verbose=True, cleanUp=True)
        _, exportList = sdl.makeLoadFiles(["D1", "D2"])
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            ok = sdl.loadBatchFiles(loadList=exportList, containerNameList=["D1", "D2"], deleteOpt="truncate")
        self.assertTrue(ok)
        cmds = self.__commands(mockQ)
        self.assertEqual(len(cmds), len(exportList))
        for cmdL in cmds:
            self.assertTrue(cmdL[0].startswith("TRUNCATE TABLE testdb."))
        for _, pth in exportList:
            self.assertFalse(os.path.exists(pth))
        self.assertIn("+SchemaDefLoader(loadBatchFiles) completed with status True", self.__lfh.getvalue())

    def testLoadBatchFilesStopsOnFailure(self) -> None:
        sdl = self.__loader(verbose=False, cleanUp=True)
        _, exportList = sdl.makeLoadFiles(["D1"])
        self.assertGreater(len(exportList), 1)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = False
            ok = sdl.loadBatchFiles(loadList=exportList)
        self.assertFalse(ok)
        self.assertEqual(mockQ.return_value.sqlCommand.call_count, 1)
        # failed file is not removed
        self.assertTrue(os.path.exists(exportList[0][1]))

    def testLoadBatchFilesMissingFile(self) -> None:
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            ok = sdl.loadBatchFiles(loadList=[("STRUCT", os.path.join(self.__workPath, "missing.tdd"))])
        self.assertTrue(ok)
        # Unreadable load file - no import command generated
        self.assertEqual(self.__commands(mockQ), [[]])

    @unittest.expectedFailure
    def testLoadBatchFilesEmpty(self) -> None:
        """BUG: loadBatchFiles() with an empty load list raises UnboundLocalError ('ok' is never assigned)."""
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery"):
            self.assertTrue(sdl.loadBatchFiles(loadList=[]))

    def testDeleteVerbose(self) -> None:
        sdl = self.__loader(verbose=True)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            self.assertTrue(sdl.delete("ENTITY", deleteOpt="all"))
            mockQ.return_value.sqlCommand.return_value = False
            self.assertFalse(sdl.delete("ENTITY", containerNameList=["D1"], deleteOpt="selected"))
        cmds = self.__commands(mockQ)
        self.assertEqual([c.strip() for c in cmds[0]], ["TRUNCATE TABLE testdb.entity;"])
        # containerNameList is ignored by delete() - no selected delete is generated
        self.assertEqual(cmds[1], [])
        self.assertIn("+SchemaDefLoader(delete) table ENTITY server returns True", self.__lfh.getvalue())

    @unittest.expectedFailure
    def testDeleteQuiet(self) -> None:
        """BUG: delete() only returns the server status when verbose is set; otherwise always returns False."""
        sdl = self.__loader(verbose=False)
        with mock.patch("wwpdb.utils.db.SchemaDefLoader.MyDbQuery") as mockQ:
            mockQ.return_value.sqlCommand.return_value = True
            self.assertTrue(sdl.delete("ENTITY", deleteOpt="all"))


if __name__ == "__main__":
    unittest.main()
