##
#
# File:    SchemaDefConsistencyTests.py
# Author:  E. Peisach
# Date:    06-Oct-2026
# Version: 0.001
##
"""
Data-driven internal consistency checks for every schema definition class.

Only the public SchemaDefBase / TableDef API is used.
"""

import io
import unittest
from typing import List, Set, Tuple, Type

from wwpdb.utils.db.BirdSchemaDef import BirdSchemaDef
from wwpdb.utils.db.ChemCompSchemaDef import ChemCompSchemaDef
from wwpdb.utils.db.DaInternalSchemaDef import DaInternalSchemaDef
from wwpdb.utils.db.MessageSchemaDef import MessageSchemaDef
from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen
from wwpdb.utils.db.PdbDistroSchemaDef import PdbDistroSchemaDef
from wwpdb.utils.db.PdbxSchemaDef import PdbxSchemaDef
from wwpdb.utils.db.PrdChemCompSchemaDef import PrdChemCompSchemaDef
from wwpdb.utils.db.SchemaDefBase import SchemaDefBase, TableDef
from wwpdb.utils.db.StatusHistorySchemaDef import StatusHistorySchemaDef
from wwpdb.utils.db.WorkflowSchemaDef import WorkflowSchemaDef

# (schema class, expected default database name)
SCHEMA_CLASSES: List[Tuple[Type[SchemaDefBase], str]] = [
    (BirdSchemaDef, "prdv4"),
    (ChemCompSchemaDef, "compv4"),
    (DaInternalSchemaDef, "da_internal_combine"),
    (MessageSchemaDef, "wwpdb_message_v1"),
    (PdbDistroSchemaDef, "stat"),
    (PdbxSchemaDef, "pdbxv4"),
    (PrdChemCompSchemaDef, "prdccv4"),
    (StatusHistorySchemaDef, "da_internal"),
    (WorkflowSchemaDef, "status"),
]

STRING_TYPES = {"VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT"}
FLOAT_TYPES = {"FLOAT", "DECIMAL", "DOUBLE PRECISION", "NUMERIC"}
DATE_TYPES = {"DATE", "DATETIME"}
INTEGER_TYPES = {"INT", "INTEGER", "BIGINT", "SMALLINT", "INT UNSIGNED", "INT UNSIGNED AUTO_INCREMENT"}
KNOWN_SQL_TYPES = STRING_TYPES | FLOAT_TYPES | DATE_TYPES | INTEGER_TYPES

KNOWN_INDEX_TYPES = {"UNIQUE", "SEARCH", "FULLTEXT"}

# Schema classes whose MAP_MERGE_INDICES list schema attribute ids rather than
# instance-category attribute names (SchemaDefLoader looks them up as category
# attribute names).  See testPdbDistroMergeIndexUsesCategoryNames.
MERGE_INDEX_EXCLUDED: Set[str] = {"PdbDistroSchemaDef"}

# Attribute ids such as 'DECAY__' (from item decay_%) contain empty '_' separated
# fragments, which make getDefaultAttributeParameterMap() raise internally and fall back
# to appending the full id list after the partially built list (duplicated entries).
DEFAULT_PARAM_MAP_EXCLUDED: Set[Tuple[str, str]] = {("PdbxSchemaDef", "DIFFRN_STANDARDS")}


class SchemaDefConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = io.StringIO()

    def tearDown(self) -> None:
        pass

    def __schemas(self) -> List[Tuple[str, SchemaDefBase, str]]:
        return [(cls.__name__, cls(verbose=False, log=self.__lfh), dbName) for cls, dbName in SCHEMA_CLASSES]

    def __tables(self) -> List[Tuple[str, str, SchemaDefBase, TableDef]]:
        rL: List[Tuple[str, str, SchemaDefBase, TableDef]] = []
        for name, sd, _ in self.__schemas():
            for tableId in sd.getTableIdList():
                rL.append((name, tableId, sd, sd.getTable(tableId)))
        return rL

    def testDatabaseName(self) -> None:
        """Each schema instantiates and reports its expected, non-empty database name"""
        for name, sd, dbName in self.__schemas():
            with self.subTest(schema=name):
                self.assertIsInstance(sd, SchemaDefBase)
                self.assertTrue(sd.getDatabaseName())
                self.assertEqual(sd.getDatabaseName(), dbName)

    def testDaInternalDatabaseNameOverride(self) -> None:
        """DaInternalSchemaDef honours an explicit database name without changing tables"""
        sdDefault = DaInternalSchemaDef(verbose=False, log=self.__lfh)
        sd = DaInternalSchemaDef(verbose=False, log=self.__lfh, databaseName="da_internal_combined")
        self.assertEqual(sd.getDatabaseName(), "da_internal_combined")
        self.assertEqual(sd.getTableIdList(), sdDefault.getTableIdList())

    def testTableIds(self) -> None:
        """Table id lists are non-empty, unique, and each id resolves to a matching table definition"""
        for name, sd, _ in self.__schemas():
            with self.subTest(schema=name):
                tableIdList = sd.getTableIdList()
                self.assertGreater(len(tableIdList), 0)
                self.assertEqual(len(tableIdList), len(set(tableIdList)))
                tableNameList = [sd.getTableName(tId).upper() for tId in tableIdList]
                self.assertEqual(len(tableNameList), len(set(tableNameList)), "duplicate table names")
                self.assertIsNotNone(sd.getSchema())
            for tableId in sd.getTableIdList():
                with self.subTest(schema=name, table=tableId):
                    tObj = sd.getTable(tableId)
                    self.assertEqual(tObj.getId(), tableId)
                    self.assertTrue(tObj.getName())
                    self.assertEqual(tObj.getName(), sd.getTableName(tableId))
                    self.assertEqual(tObj.getType(), "transactional")

    def testAttributeLists(self) -> None:
        """Attribute id and name lists agree between SchemaDefBase and TableDef and line up"""
        for name, tableId, sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                aIdL = tObj.getAttributeIdList()
                aNameL = tObj.getAttributeNameList()
                self.assertGreater(len(aIdL), 0)
                self.assertEqual(aIdL, sd.getAttributeIdList(tableId))
                self.assertEqual(aNameL, sd.getAttributeNameList(tableId))
                self.assertEqual(len(aIdL), len(aNameL))
                self.assertEqual(len(aIdL), len(set(aIdL)), "duplicate attribute ids")
                self.assertEqual(len(aNameL), len({n.upper() for n in aNameL}), "duplicate attribute names")
                # ATTRIBUTES and ATTRIBUTE_INFO describe the same attribute ids
                self.assertEqual(set(tObj.getAttributeIdMap().keys()), set(aIdL))
                for aId, aName in zip(aIdL, aNameL):
                    self.assertEqual(tObj.getAttributeName(aId), aName)
                    self.assertEqual(sd.getQualifiedAttributeName((tableId, aId)), "%s.%s" % (sd.getTableName(tableId), aName))
                pL = sd.getDefaultAttributeParameterMap(tableId, skipAuto=False)
                if (name, tableId) in DEFAULT_PARAM_MAP_EXCLUDED:
                    self.assertEqual(set(p[0] for p in pL), set(aIdL))  # noqa: C401
                else:
                    self.assertEqual([p[0] for p in pL], aIdL)

    def testAttributeTypes(self) -> None:
        """Attribute SQL types are known, widths are sensible and type predicates agree"""
        for name, tableId, _sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                widthD = tObj.getStringWidthDict()
                nullD = tObj.getSqlNullValueDict()
                for aId in tObj.getAttributeIdList():
                    sqlType = tObj.getAttributeType(aId)
                    self.assertIsNotNone(sqlType, aId)
                    uType = str(sqlType).upper()
                    self.assertIn(uType, KNOWN_SQL_TYPES, aId)
                    self.assertEqual(tObj.isAttributeStringType(aId), uType in STRING_TYPES, aId)
                    self.assertEqual(tObj.isAttributeFloatType(aId), uType in FLOAT_TYPES, aId)
                    self.assertEqual(tObj.isAttributeIntegerType(aId), uType in INTEGER_TYPES, aId)
                    self.assertEqual(tObj.isAutoIncrementType(aId), "AUTO_INCREMENT" in uType, aId)
                    # Widths are ints except in PdbDistroSchemaDef where they are numeric strings
                    width = tObj.getAttributeWidth(aId)
                    self.assertTrue(str(width).isdigit(), "%s width %r" % (aId, width))
                    self.assertIsNotNone(tObj.getAttributePrecision(aId), aId)
                    if uType in STRING_TYPES:
                        self.assertGreater(int(str(width)), 0, aId)
                        self.assertEqual(widthD[aId], int(str(width)), aId)
                        self.assertEqual(nullD[aId], "", aId)
                        self.assertEqual(tObj.getSqlNullValue(aId), "", aId)
                    else:
                        self.assertEqual(widthD[aId], 0, aId)
                        self.assertEqual(nullD[aId], r"\N", aId)
                        self.assertEqual(tObj.getSqlNullValue(aId), r"\N", aId)
                    self.assertIsInstance(tObj.getAttributeNullable(aId), bool, aId)
                    self.assertIsInstance(tObj.getAttributeIsPrimaryKey(aId), bool, aId)

    def testPrimaryKeys(self) -> None:
        """Every table has a primary key made of existing, non-nullable attributes"""
        for name, tableId, _sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                aIdL = tObj.getAttributeIdList()
                pkL = tObj.getPrimaryKeyAttributeIdList()
                self.assertGreater(len(pkL), 0)
                for aId in pkL:
                    self.assertIn(aId, aIdL)
                    self.assertTrue(tObj.getAttributeIsPrimaryKey(aId))
                    self.assertFalse(tObj.getAttributeNullable(aId), aId)

    def testIndices(self) -> None:
        """Indices have a known type and reference existing attributes"""
        for name, tableId, _sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                aIdL = tObj.getAttributeIdList()
                for indexName in tObj.getIndexNames():
                    self.assertIn(tObj.getIndexType(indexName), KNOWN_INDEX_TYPES, indexName)
                    iAttrL = tObj.getIndexAttributeIdList(indexName)
                    self.assertGreater(len(iAttrL), 0, indexName)
                    for aId in iAttrL:
                        self.assertIn(aId, aIdL, indexName)

    def testDeleteAttribute(self) -> None:
        """An optional table delete attribute references an existing attribute"""
        for name, tableId, _sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                dId = tObj.getDeleteAttributeId()
                if dId is None:
                    self.assertIsNone(tObj.getDeleteAttributeName())
                    continue
                self.assertIn(dId, tObj.getAttributeIdList())
                self.assertEqual(tObj.getDeleteAttributeName(), tObj.getAttributeName(dId))

    def testAttributeMap(self) -> None:
        """Attribute maps reference valid attribute ids and well formed mapping tuples"""
        for name, tableId, _sd, tObj in self.__tables():
            mapIdL = tObj.getMapAttributeIdList()
            if not mapIdL:
                continue
            with self.subTest(schema=name, table=tableId):
                aIdL = tObj.getAttributeIdList()
                mapD = tObj.getMapAttributeDict()
                # every schema attribute is mapped
                self.assertEqual(set(mapD.keys()), set(aIdL))
                self.assertEqual(mapIdL, aIdL)
                self.assertEqual(tObj.getMapAttributeNameList(), tObj.getAttributeNameList())
                otherL = tObj.getMapOtherAttributeIdList()
                catL = tObj.getMapInstanceCategoryList()
                nMapped = 0
                for aId in mapIdL:
                    mInfo = tObj.getMapAttributeInfo(aId)
                    self.assertEqual(len(mInfo), 4, aId)
                    self.assertEqual(tObj.getMapAttributeFunction(aId), mInfo[2])
                    self.assertEqual(tObj.getMapAttributeFunctionArgs(aId), mInfo[3])
                    if mInfo[0] is None:
                        self.assertIn(aId, otherL)
                    else:
                        self.assertIn(mInfo[0], catL)
                        self.assertTrue(mInfo[1], aId)
                for cat in catL:
                    cIdL = tObj.getMapInstanceAttributeIdList(cat)
                    cAttrL = tObj.getMapInstanceAttributeList(cat)
                    self.assertEqual(len(cIdL), len(cAttrL))
                    for aId, aName in zip(cIdL, cAttrL):
                        self.assertEqual(mapD[aId], aName)
                    nMapped += len(cIdL)
                self.assertEqual(nMapped + len(otherL), len(mapIdL))

    def testMergeIndices(self) -> None:
        """Merge indices are EQUI-JOIN and list mapped instance-category attribute names"""
        for name, tableId, _sd, tObj in self.__tables():
            if name in MERGE_INDEX_EXCLUDED:
                continue
            for cat in tObj.getMapInstanceCategoryList():
                mL = tObj.getMapMergeIndexAttributes(cat)
                if not mL:
                    # Not every mapped category defines a merge index (e.g. secondary categories)
                    continue
                with self.subTest(schema=name, table=tableId, category=cat):
                    self.assertEqual(tObj.getMapMergeIndexType(cat), "EQUI-JOIN")
                    cAttrL = tObj.getMapInstanceAttributeList(cat)
                    for aName in mL:
                        self.assertIn(aName, cAttrL)

    @unittest.expectedFailure
    def testPdbDistroMergeIndexUsesCategoryNames(self) -> None:
        """Known data issue: PdbDistroSchemaDef merge indices use schema attribute ids
        (e.g. 'STRUCTURE_ID') instead of instance category attribute names (e.g. 'structure_id')
        """
        sd = PdbDistroSchemaDef(verbose=False, log=self.__lfh)
        badL: List[Tuple[str, str, str]] = []
        for tableId in sd.getTableIdList():
            tObj = sd.getTable(tableId)
            for cat in tObj.getMapInstanceCategoryList():
                cAttrL = tObj.getMapInstanceAttributeList(cat)
                for aName in tObj.getMapMergeIndexAttributes(cat):
                    if aName not in cAttrL:
                        badL.append((tableId, cat, aName))
        self.assertEqual(badL, [])

    def testCreateTableSql(self) -> None:
        """SQL table creation statements can be generated for every table"""
        sqlGen = MyDbAdminSqlGen(verbose=False, log=self.__lfh)
        for name, tableId, sd, tObj in self.__tables():
            with self.subTest(schema=name, table=tableId):
                dbName = sd.getDatabaseName()
                self.assertIsNotNone(dbName)
                sqlL = sqlGen.createTableSQL(str(dbName), tObj)
                self.assertGreater(len(sqlL), 0)
                sqlS = "\n".join(sqlL)
                self.assertIn(str(tObj.getName()), sqlS)
                for aName in tObj.getAttributeNameList():
                    self.assertIn(aName, sqlS)


def suiteSelect() -> unittest.TestSuite:  # pragma: no cover
    return unittest.TestLoader().loadTestsFromTestCase(SchemaDefConsistencyTests)


if __name__ == "__main__":  # pragma: no cover
    mySuite = suiteSelect()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
