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


class SchemaDefBase(object):

    """A base class for schema definitions."""

    def __init__(self, databaseName=None, schemaDefDict=None, verbose=True, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__databaseName = databaseName
        self.__schemaDefDict = schemaDefDict

    def getSchema(self):
        return self.__schemaDefDict

    def getDatabaseName(self):
        return self.__databaseName

    def getTable(self, tableId):
        return TableDef(tableDefDict=self.__schemaDefDict[tableId], verbose=self.__verbose, log=self.__lfh)

    def getTableName(self, tableId):
        return self.__schemaDefDict[tableId]["TABLE_NAME"]

    def getTableIdList(self):
        return list(self.__schemaDefDict.keys())

    def getAttributeIdList(self, tableId):
        tD = self.__schemaDefDict[tableId]
        tupL = []
        for attributeId, v in tD["ATTRIBUTE_INFO"].items():
            tupL.append((attributeId, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getDefaultAttributeParameterMap(self, tableId, skipAuto=True):
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

    def getAttributeNameList(self, tableId):
        tD = self.__schemaDefDict[tableId]
        tupL = []
        for k, v in tD["ATTRIBUTE_INFO"].items():
            attributeName = tD["ATTRIBUTES"][k]
            tupL.append((attributeName, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getQualifiedAttributeName(self, tableAttributeTuple=(None, None)):
        tableId = tableAttributeTuple[0]
        attributeId = tableAttributeTuple[1]
        tD = self.__schemaDefDict[tableId]
        tableName = tD["TABLE_NAME"]
        attributeName = tD["ATTRIBUTES"][attributeId]
        qAN = tableName + "." + attributeName
        return qAN


class TableDef(object):

    """Wrapper class for table schema definition."""

    def __init__(self, tableDefDict=None, verbose=True, log=sys.stderr):  # pylint: disable=unused-argument
        if tableDefDict is None:
            tableDefDict = {}
        self.__tD = tableDefDict

    def getName(self):
        try:
            return self.__tD["TABLE_NAME"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getType(self):
        try:
            return self.__tD["TABLE_TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getId(self):
        try:
            return self.__tD["TABLE_ID"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeIdMap(self):
        try:
            return self.__tD["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return {}

    def getAttributeName(self, attributeId):
        try:
            return self.__tD["ATTRIBUTES"][attributeId]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeInfo(self, attributeId):
        """Return the tuple of mapping details for the input attribute id."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId]
        except:  # noqa: E722  pylint: disable=bare-except
            return ()

    def getAttributeType(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def isAutoIncrementType(self, attributeId):
        try:
            tL = [tt.upper() for tt in self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].split()]
            if "AUTO_INCREMENT" in tL:
                return True
        except:  # noqa: E722  pylint: disable=bare-except
            pass
        return False

    def isAttributeStringType(self, attributeId):
        try:
            return self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def isAttributeFloatType(self, attributeId):
        try:
            return self.__isFloatType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def isAttributeIntegerType(self, attributeId):
        try:
            return self.__isIntegerType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper())
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def getAttributeWidth(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["WIDTH"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributePrecision(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRECISION"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeNullable(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["NULLABLE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getAttributeIsPrimaryKey(self, attributeId):
        try:
            return self.__tD["ATTRIBUTE_INFO"][attributeId]["PRIMARY_KEY"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getPrimaryKeyAttributeIdList(self):
        try:
            return [atId for atId in self.__tD["ATTRIBUTE_INFO"].keys() if self.__tD["ATTRIBUTE_INFO"][atId]["PRIMARY_KEY"]]
        except:  # noqa: E722  pylint: disable=bare-except
            pass

        return []

    def getAttributeIdList(self):
        """Get the ordered attribute Id list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [tup[0] for tup in sTupL]

    def getAttributeNameList(self):
        """Get the ordered attribute name list"""
        tupL = []
        for k, v in self.__tD["ATTRIBUTE_INFO"].items():
            tupL.append((k, v["ORDER"]))
        sTupL = sorted(tupL, key=itemgetter(1))
        return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]

    def getIndexNames(self):
        try:
            return list(self.__tD["INDICES"].keys())
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getIndexType(self, indexName):
        try:
            return self.__tD["INDICES"][indexName]["TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getIndexAttributeIdList(self, indexName):
        try:
            return self.__tD["INDICES"][indexName]["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeNameList(self):
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"].keys():
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))
            return [self.__tD["ATTRIBUTES"][tup[0]] for tup in sTupL]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeIdList(self):
        """Get the ordered mapped attribute name list"""
        try:
            tupL = []
            for k in self.__tD["ATTRIBUTE_MAP"].keys():
                iOrd = self.__tD["ATTRIBUTE_INFO"][k]["ORDER"]
                tupL.append((k, iOrd))

            sTupL = sorted(tupL, key=itemgetter(1))

            return [tup[0] for tup in sTupL]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceCategoryList(self):
        """Get the unique list of instance categories within the attribute map."""
        try:
            cL = [vTup[0] for k, vTup in self.__tD["ATTRIBUTE_MAP"].items() if vTup[0] is not None]
            uL = list(set(cL))
            return uL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapOtherAttributeIdList(self):
        """Get the list of attributes that have no assigned instance mapping."""
        try:
            aL = []
            for k, vTup in self.__tD["ATTRIBUTE_MAP"].items():
                if vTup[0] is None:
                    aL.append(k)
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceAttributeList(self, categoryName):
        """Get the list of instance category attribute names for mapped attributes in the input instance category."""
        try:
            aL = []
            for _k, vTup in self.__tD["ATTRIBUTE_MAP"].items():
                if vTup[0] == categoryName:
                    aL.append(vTup[1])
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapInstanceAttributeIdList(self, categoryName):
        """Get the list of schema attribute Ids for mapped attributes from the input instance category."""
        try:
            aL = []
            for k, vTup in self.__tD["ATTRIBUTE_MAP"].items():
                if vTup[0] == categoryName:
                    aL.append(k)
            return aL
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapAttributeFunction(self, attributeId):
        """Return the tuple element of mapping details for the input attribute id for the optional function."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId][2]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeFunctionArgs(self, attributeId):
        """Return the tuple element of mapping details for the input attribute id for the optional function arguments."""
        try:
            return self.__tD["ATTRIBUTE_MAP"][attributeId][3]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getMapAttributeDict(self):
        """Return the dictionary of d[schema attribute id] = mapped instance category attribute"""
        d = {}
        for k, v in self.__tD["ATTRIBUTE_MAP"].items():
            d[k] = v[1]
        return d

    def getMapMergeIndexAttributes(self, categoryName):
        """Return the list of merging index attribures for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][categoryName]["ATTRIBUTES"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getMapMergeIndexType(self, indexName):
        """Return the merging index type for this mapped instance category."""
        try:
            return self.__tD["MAP_MERGE_INDICES"][indexName]["TYPE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return []

    def getSqlNullValue(self, attributeId):
        """Return the appropriate NULL value for this attribute:."""
        try:
            if self.__isStringType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper()):
                return ""
            elif self.__isDateType(self.__tD["ATTRIBUTE_INFO"][attributeId]["SQL_TYPE"].upper()):
                return r"\N"
            else:
                return r"\N"
        except:  # noqa: E722  pylint: disable=bare-except
            return r"\N"

    def getSqlNullValueDict(self):
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

    def getStringWidthDict(self):
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

    def __isStringType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType in ["VARCHAR", "CHAR", "TEXT", "MEDIUMTEXT", "LONGTEXT"]

    def __isDateType(self, sqlType):
        """Return if input type corresponds to a common SQL date/time data type."""
        return sqlType in ["DATE", "DATETIME"]

    def __isFloatType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType in ["FLOAT", "DECIMAL", "DOUBLE PRECISION", "NUMERIC"]

    def __isIntegerType(self, sqlType):
        """Return if input type corresponds to a common SQL string data type."""
        return sqlType.startswith("INT") or sqlType in ["INTEGER", "BIGINT", "SMALLINT"]

    def getDeleteAttributeId(self):
        """Return the attribute identifier that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["TABLE_DELETE_ATTRIBUTE"]
        except:  # noqa: E722  pylint: disable=bare-except
            return None

    def getDeleteAttributeName(self):
        """Return the attribute name that is used to delete all of the
        records associated with the highest level of organizaiton provided by
        this schema definition (e.g. entry, definition, ...).
        """
        try:
            return self.__tD["ATTRIBUTES"][self.__tD["TABLE_DELETE_ATTRIBUTE"]]
        except:  # noqa: E722  pylint: disable=bare-except
            return None
