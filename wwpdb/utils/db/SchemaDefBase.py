##
# File:    SchemaDefBase.py
# Author:  J. Westbrook
# Date:    25-Nov-2011
# Version: 0.001 Initial version
#
# Updates:
# 23-Jan-2012  jdw Added indices and attribute_info
# 27-Jan-2010  jdw general table and wrap in TableDef class.
#                  simplify index description with type (todo)
# 23-Jan-2012  jdw refactored from MessageSchemaDef
#  7-Jan-2013  jdw add instance mapping data accessors
#  9-Jan-2013  jdw add merging index attribute accessors
#
##
"""
Base classes for schema defintions.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.001"

import sys
from operator import itemgetter
from typing import Dict, List, Optional, Sequence, TextIO, Tuple, Union, cast

from typing_extensions import NotRequired, TypedDict

# Python 3.8 does not have TypeAlias


class AttributeInfoDict(TypedDict):
    """Column definition: ATTRIBUTE_INFO[attributeId]"""

    SQL_TYPE: str
    WIDTH: Union[int, str]  # PdbDistroSchemaDef and MysqlSchemaImporter store some widths as strings
    PRECISION: int
    NULLABLE: bool
    PRIMARY_KEY: bool
    ORDER: int


class IndexDict(TypedDict):
    """Index definition: INDICES[indexName] and MAP_MERGE_INDICES[categoryName]"""

    TYPE: str  # UNIQUE, SEARCH, FULLTEXT or EQUI-JOIN
    ATTRIBUTES: Sequence[str]  # ('family_prd_id', 'prd_id', 'ordinal')


# (instance category, instance attribute, function, function args) - e.g. ('pdbx_reference_entity_poly_link', 'atom_id_1', None, None)
AttributeMapTuple = Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]


class TableDefDict(TypedDict):
    """Table definition: schemaDefDict[tableId]"""

    TABLE_ID: str
    TABLE_NAME: str
    TABLE_TYPE: str
    ATTRIBUTES: Dict[str, str]
    ATTRIBUTE_INFO: Dict[str, AttributeInfoDict]
    INDICES: Dict[str, IndexDict]
    TABLE_DELETE_ATTRIBUTE: NotRequired[str]
    ATTRIBUTE_MAP: NotRequired[Dict[str, AttributeMapTuple]]
    MAP_MERGE_INDICES: NotRequired[Dict[str, IndexDict]]


SchemaDictType = Dict[str, TableDefDict]


class SchemaDefBase:
    """A base class for schema definitions."""

    def __init__(
        self, databaseName: Optional[str] = None, schemaDefDict: Optional[SchemaDictType] = None, verbose: bool = True, log: TextIO = sys.stderr
    ) -> None:
        self.__verbose = verbose
        self.__lfh = log
        self.__databaseName = databaseName
        # None only when constructed without a schema (getSchema() then returns None and table lookups fail as before)
        self.__schemaDefDict = cast("SchemaDictType", schemaDefDict)

    def getSchema(self) -> Optional[SchemaDictType]:
        return self.__schemaDefDict

    def getDatabaseName(self) -> Optional[str]:
        return self.__databaseName

    def getTable(self, tableId: str) -> "TableDef":
        return TableDef(tableDefDict=self.__schemaDefDict[tableId], verbose=self.__verbose, log=self.__lfh)

    def getTableName(self, tableId: str) -> str:
        return self.__schemaDefDict[tableId]["TABLE_NAME"]

    def getTableIdList(self) -> List[str]:
        return list(self.__schemaDefDict.keys())

    def getAttributeIdList(self, tableId: str) -> List[str]:
        tD = self.__schemaDefDict[tableId]
        tupL = []
        for attributeId, v in tD["ATTRIBUTE_INFO"].items():
            tupL.append((attributeId, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getDefaultAttributeParameterMap(self, tableId: str, skipAuto: bool = True) -> List[Tuple[str, str]]:
        """For the input table, return a dictionary of attribute identifiers and parameter names.
        Default parameter names are compressed and camel-case conversions of the attribute ids.
        """
        dL = []
        aIdList = self.getAttributeIdList(tableId)

        try:
            tDef = self.getTable(tableId)
            for aId in aIdList:
                if skipAuto and tDef.isAutoIncrementType(aId):
                    continue
                pL = aId.lower().split("_")
                tL = []
                tL.append(pL[0])
                for p in pL[1:]:
                    tt = p[0].upper() + p[1:]
                    tL.append(tt)
                dL.append((aId, "".join(tL)))
        except:  # noqa: E722  pylint: disable=bare-except
            for aId in aIdList:
                dL.append((aId, aId))
        return dL

    def getAttributeNameList(self, tableId: str) -> List[str]:
        tD = self.__schemaDefDict[tableId]
        tupL = []
        for k, v in tD["ATTRIBUTE_INFO"].items():
            attributeName = tD["ATTRIBUTES"][k]
            tupL.append((attributeName, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getQualifiedAttributeName(self, tableAttributeTuple: Tuple[Optional[str], Optional[str]] = (None, None)) -> str:
        # None ids (the default) raise KeyError on lookup
        tableId = cast("str", tableAttributeTuple[0])
        attributeId = cast("str", tableAttributeTuple[1])
        tD = self.__schemaDefDict[tableId]
        tableName = tD["TABLE_NAME"]
        attributeName = tD["ATTRIBUTES"][attributeId]
        qAN = tableName + "." + attributeName
        return qAN


class TableDef:
    """Wrapper class for table schema definition."""

    def __init__(self, tableDefDict: Optional[TableDefDict] = None, verbose: bool = True, log: TextIO = sys.stderr) -> None:  # noqa: ARG002   pylint: disable=unused-argument
        if tableDefDict is None:
            # An empty definition: getters fall back to their failure values
            tableDefDict = cast("TableDefDict", {})
        self.__tD = tableDefDict

    def getName(self) -> Optional[str]:
        try:
            return self.__tD["TABLE_NAME"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getType(self) -> Optional[str]:
        try:
            return self.__tD["TABLE_TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getId(self) -> Optional[str]:
        try:
            return self.__tD["TABLE_ID"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeIdMap(self) -> Dict[str, str]:
        try:
            return self.__tD["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return {}

    def getAttributeName(self, attributeId: str) -> Optional[str]:
        try:
            return self.__tD["ATTRIBUTES"][attributeId]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeInfo(self, attributeId: str) -> Tuple[Optional[str], ...]:
        """Return the tuple of mapping details for the input attribute id."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId]
        except:  # noqa: E722  pylint: disable=bare-except
            return ()

    def getAttributeType(self, attributeId: str) -> Optional[str]:
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def isAutoIncrementType(self, attributeId: str) -> bool:
        try:
            tL = [tt.upper() for tt in self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].split()]
            if "AUTO_INCREMENT" in tL:
                return True
        except:  # noqa: E722  pylint: disable=bare-except
            pass
        return False

    def isAttributeStringType(self, attributeId: str) -> bool:
        try:
            return self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def isAttributeFloatType(self, attributeId: str) -> bool:
        try:
            return self.__isFloatType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def isAttributeIntegerType(self, attributeId: str) -> bool:
        try:
            return self.__isIntegerType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def getAttributeWidth(self, attributeId: str) -> Optional[Union[int, str]]:
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["WIDTH"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributePrecision(self, attributeId: str) -> Optional[int]:
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRECISION"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeNullable(self, attributeId: str) -> Optional[bool]:
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["NULLABLE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeIsPrimaryKey(self, attributeId: str) -> Optional[bool]:
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRIMARY_KEY"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getPrimaryKeyAttributeIdList(self) -> List[str]:
        try:
            return [atId for atId in self.__tD["ATTRIBUTE_INFO"] if self.__tD["ATTRIBUTE_INFO"][atId]["PRIMARY_KEY"]]
        except:  # noqa: E722  pylint: disable=bare-except
            pass

        return []

    def getAttributeIdList(self) -> List[str]:
        """Get the ordered attribute Id list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getAttributeNameList(self) -> List[str]:
        """Get the ordered attribute name list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]

    def getIndexNames(self) -> List[str]:
        try:
            return list(self.__tD["INDICES"].keys())
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getIndexType(self, indexName: str) -> Optional[str]:
        try:
            return self.__tD["INDICES"][indexName]["TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getIndexAttributeIdList(self, indexName: str) -> Sequence[str]:
        try:
            return self.__tD["INDICES"][indexName]["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeNameList(self) -> List[str]:
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"]:
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))
            return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeIdList(self) -> List[str]:
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"]:
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))

            return [tup[0] for tup in sTupL]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceCategoryList(self) -> List[str]:
        """Get the unique list of instance categories within the attribute map."""
        try:
            cL = [vTup[0] for k, vTup in self.__tD["ATTRIBUTE_MAP"].items() if vTup[0] is not None]
            uL = list(set(cL))
            return uL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapOtherAttributeIdList(self) -> List[str]:
        """Get the list of attributes that have no assigned instance mapping."""
        try:
            aL = []
            for k, vTup in self.__tD["ATTRIBUTE_MAP"].items():
                if vTup[0] is None:
                    aL.append(k)
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceAttributeList(self, categoryName: str) -> List[Optional[str]]:
        """Get the list of instance category attribute names for mapped attributes in the input instance category."""
        try:
            aL = []
            for vTup in self.__tD["ATTRIBUTE_MAP"].values():
                if vTup[0] == categoryName:
                    aL.append(vTup[1])
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceAttributeIdList(self, categoryName: str) -> List[str]:
        """Get the list of schema attribute Ids for mapped attributes from the input instance category."""
        try:
            aL = []
            for k, vTup in self.__tD["ATTRIBUTE_MAP"].items():
                if vTup[0] == categoryName:
                    aL.append(k)
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeFunction(self, attributeId: str) -> Optional[str]:
        """Return the tuple element of mapping details for the input attribute id for the optional function."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId][2]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeFunctionArgs(self, attributeId: str) -> Optional[str]:
        """Return the tuple element of mapping details for the input attribute id for the optional function arguments."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId][3]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeDict(self) -> Dict[str, Optional[str]]:
        """Return the dictionary of d[schema attribute id] = mapped instance category attribute"""
        d = {}
        for k, v in self.__tD["ATTRIBUTE_MAP"].items():
            d[k] = v[1]
        return d

    def getMapMergeIndexAttributes(self, categoryName: str) -> Sequence[str]:
        """Return the list of merging index attribures for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][categoryName]["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapMergeIndexType(self, indexName: str) -> Union[str, List[str]]:
        """Return the merging index type for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][indexName]["TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getSqlNullValue(self, attributeId: str) -> str:
        """Return the appropriate NULL value for this attribute:."""
        try:
            if self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper()):
                return ""
            if self.__isDateType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper()):
                return r"\N"
            return r"\N"
        except:  # noqa: E722  pylint: disable=bare-except
            return r"\N"

    def getSqlNullValueDict(self) -> Dict[str, str]:
        """Return a dictionary containing appropriate NULL value for each attribute."""
        d = {}
        for atId, atInfo in self.__tD["ATTRIBUTE_INFO"].items():
            if self.__isStringType(atInfo["SQL_TYPE"].upper()):
                d[atId] = ""
            elif self.__isDateType(atInfo["SQL_TYPE"].upper()):
                d[atId] = r"\N"
            else:
                d[atId] = r"\N"
        #
        return d

    def getStringWidthDict(self) -> Dict[str, int]:
        """Return a dictionary containing maximum string widths assigned to char data types.
        Non-character type data items are assigned zero width.
        """
        d = {}
        for atId, atInfo in self.__tD["ATTRIBUTE_INFO"].items():
            if self.__isStringType(atInfo["SQL_TYPE"].upper()):
                d[atId] = int(atInfo["WIDTH"])
            else:
                d[atId] = 0
        return d

    def __isStringType(self, sqlType: str) -> bool:
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType in ["VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT"]

    def __isDateType(self, sqlType: str) -> bool:
        """Return if input type corresponds to a common SQL date/time data type."""
        return sqlType in ["DATE", "DATETIME"]

    def __isFloatType(self, sqlType: str) -> bool:
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType in ["FLOAT", "DECIMAL", "DOUBLE PRECISION", "NUMERIC"]

    def __isIntegerType(self, sqlType: str) -> bool:
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType.startswith("INT") or sqlType in ["INTEGER", "BIGINT", "SMALLINT"]

    def getDeleteAttributeId(self) -> Optional[str]:
        """Return the attribute identifier that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["TABLE_DELETE_ATTRIBUTE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getDeleteAttributeName(self) -> Optional[str]:
        """Return the attribute name that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["ATTRIBUTES"][self.__tD["TABLE_DELETE_ATTRIBUTE"]]
        except:  # noqa: E722  pylint: disable=bare-except
            return None
