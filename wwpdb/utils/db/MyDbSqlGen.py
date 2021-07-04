##
# File:    MyDdSqlGen.py
# Author:  J. Westbrook
# Date:    31-Jan-2012
# Version: 0.001 Initial version
#
# Updates:
# 27-Jan-2012 Jdw Refactored from MyDbUtil to isolate portable SQL generators.
#  1-Feb-2012 Jdw Add export/import methods
# 11-Apr-2014 jdw add template methods with attribute Id inputs.
# 25-May-2015 jdw complete the coding of the contraint generator class
# 28-May-2015 jdw adjust terminology in api method names and internal vars -
# 16-Jun-2015 jdw generalized the addition of a condition group addGroupValueConditionList
#
#
##
"""
A collection of classes to generate SQL commands to perform queries and schema construction.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.001"

import sys
import itertools
import copy

try:
    # Python 3
    from itertools import zip_longest
except ImportError:  # pragma: no cover
    # Python 2
    from itertools import izip_longest as zip_longest


class MyDbAdminSqlGen(object):

    """Builds SQL commands to create table schema from a schema definition derived from class SchemaDefBase.

    Note:
    """

    def __init__(self, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        pass

    def truncateTableSQL(self, databaseName, tableName):
        """Return the SQL string require to truncate (remove all rows) from the input table."""
        return "TRUNCATE TABLE %s.%s; " % (databaseName, tableName)

    def idUpdateTemplateSQL(self, databaseName, tableDefObj, updateAttributeIdList=None, conditionAttributeIdList=None):
        """Return the SQL string template for updating the input attributes into the named table subject
        to the constraining attributes.

        The string provides formatting placeholders for updated values  as well as for constraining values.

        The input table object is used to adjust the value quoting in the returned template string.

        """
        if updateAttributeIdList is None:
            updateAttributeIdList = []
        if conditionAttributeIdList is None:
            conditionAttributeIdList = []
        tableName = tableDefObj.getName()
        fL = []
        for atId in updateAttributeIdList:
            if tableDefObj.isAttributeStringType(atId):
                fL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
            elif tableDefObj.isAttributeFloatType(atId):
                fL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
            elif tableDefObj.isAttributeIntegerType(atId):
                fL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
            else:
                fL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
        #
        if len(conditionAttributeIdList) > 0:
            cL = []
            for atId in conditionAttributeIdList:
                if tableDefObj.isAttributeStringType(atId):
                    cL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
                else:
                    cL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
            tS = "UPDATE %s.%s SET %s WHERE (%s);" % (databaseName, tableName, ",".join(fL), ",".join(cL))
        else:
            tS = "UPDATE %s.%s SET %s;" % (databaseName, tableName, ",".join(fL))
        #
        return tS

    def idInsertTemplateSQL(self, databaseName, tableDefObj, insertAttributeIdList=None):
        """Return the SQL string template for inserting the input attributes into the named table.

        The string provides formatting placeholders for updated values  as well as for constraining values.

        The input table object is used to adjust the value quoting in the returned template string.

        """
        if insertAttributeIdList is None:
            insertAttributeIdList = []
        tableName = tableDefObj.getName()
        attributeNameList = []
        #
        fL = []
        for atId in insertAttributeIdList:
            attributeNameList.append(tableDefObj.getAttributeName(atId))
            if tableDefObj.isAttributeStringType(atId):
                fL.append("%s")
            elif tableDefObj.isAttributeFloatType(atId):
                fL.append("%s")
            elif tableDefObj.isAttributeIntegerType(atId):
                fL.append("%s")
            else:
                fL.append("%s")
        #
        tS = "INSERT INTO %s.%s (%s) VALUES (%s);" % (databaseName, tableName, ",".join(attributeNameList), ",".join(fL))
        #
        return tS

    def idDeleteTemplateSQL(self, databaseName, tableDefObj, conditionAttributeIdList=None):
        """Return the SQL string template for deleting records in the named table subject
        to the constraining attributes.

        The string provides formatting placeholders for constraining values.

        The input table object is used to adjust the value quoting in the returned template string.

        """
        if conditionAttributeIdList is None:
            conditionAttributeIdList = []
        tableName = tableDefObj.getName()
        #
        if len(conditionAttributeIdList) > 0:
            cL = []
            for atId in conditionAttributeIdList:
                if tableDefObj.isAttributeStringType(atId):
                    cL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")
                else:
                    cL.append(" %s=" % tableDefObj.getAttributeName(atId) + "%s")

            tS = "DELETE FROM  %s.%s WHERE (%s);" % (databaseName, tableName, " AND ".join(cL))
        else:
            tS = "DELETE FROM  %s.%s;" % (databaseName, tableName)
        #
        return tS

    def insertTemplateSQL(self, databaseName, tableName, attributeNameList=None):
        """Return the SQL string template for inserting the input attributes into the named table.

        The string provides formatting placeholders for inserted values that are added when
        the SQL command is executed.

        """
        if attributeNameList is None:
            attributeNameList = []
        fL = []
        for _v in attributeNameList:
            fL.append("%s")
        #
        tS = "INSERT INTO %s.%s (%s) VALUES (%s);" % (databaseName, tableName, ",".join(attributeNameList), ",".join(fL))
        return tS

    def deleteTemplateSQL(self, databaseName, tableName, attributeNameList=None):
        """Return the SQL string template for deleting table records constrained by the input attributes.

        The string provides formatting placeholders for the constraining values.
         delete from <tableName>  where at1=%s and at2=%s

        """
        if attributeNameList is None:
            attributeNameList = []
        fL = []
        for v in attributeNameList:
            fL.append(" %s=" % v + "%s")
        #
        tS = "DELETE FROM %s.%s WHERE %s;" % (databaseName, tableName, " AND ".join(fL))
        return tS

    def deleteFromListSQL(self, databaseName, tableName, attributeName, valueList, chunkSize=10):
        """Return the SQL string for deleting table records for a list of string values of
        the input attribute.

         delete from <databaseName>.<tableName>  where attributeName  IN (v1,v2,v3);

        """
        sqlList = []
        chunkLists = self.__makeSubLists(chunkSize, valueList)
        for chunk in chunkLists:
            fL = ["'%s'" % v for v in chunk]
            sqlList.append("DELETE FROM %s.%s WHERE %s IN (%s); " % (databaseName, tableName, attributeName, ",".join(fL)))

        return sqlList

    def __makeSubLists(self, n, iterable):
        args = [iter(iterable)] * n
        return ([e for e in t if e is not None] for t in zip_longest(*args))

    def createDatabaseSQL(self, databaseName):
        """Return a list of strings containing the SQL to drop and recreate the input database.

        DROP DATABASE IF EXISTS <databaseName>;
        CREATE DATABASE <databaseName>;
        """
        oL = []
        oL.append("DROP DATABASE IF EXISTS %s;" % databaseName)
        oL.append("CREATE DATABASE %s;" % databaseName)
        return oL

    def createTableSQL(self, databaseName, tableDefObj):
        """Return a list of strings containing the SQL commands to create the table and indices
        described by the input table definition.

        """
        oL = []
        oL.extend(self.__setDatabase(databaseName))
        oL.extend(self.__dropTable(tableDefObj.getName()))
        oL.extend(self.__createTable(tableDefObj))
        oL.extend(self.__createTableIndices(tableDefObj))
        return oL

    def __setDatabase(self, databaseName):
        """Return a list of strings containing database connection SQL command for the input database

        USE <databaseName>;
        """
        return ["USE %s;" % databaseName]

    def __dropTable(self, tableName):
        """Return a list of strings containing the SQL DROP TABLE command for the input table:

        DROP TABLE IF EXISTS <tableName>;
        """
        return ["DROP TABLE IF EXISTS %s;" % tableName]

    def __createTable(self, tableDefObj):
        """Return a list of strings containing the SQL command to create the table described in
        input table schema definition object.

        """
        oL = []
        pkL = []
        #
        attributeIdList = tableDefObj.getAttributeIdList()
        #
        oL.append("CREATE TABLE %s (" % tableDefObj.getName())
        for _ii, attributeId in enumerate(attributeIdList):
            #
            name = tableDefObj.getAttributeName(attributeId)

            sqlType = tableDefObj.getAttributeType(attributeId)
            width = int(tableDefObj.getAttributeWidth(attributeId))
            precision = int(tableDefObj.getAttributePrecision(attributeId))
            notNull = "not null" if not tableDefObj.getAttributeNullable(attributeId) else "    null default null"
            if tableDefObj.getAttributeIsPrimaryKey(attributeId):
                pkL.append(name)
            #
            if (sqlType == "CHAR") or (sqlType == "VARCHAR"):
                sW = "%-s(%d)" % (sqlType, width)
                tS = "%-40s %-16s  %s" % (name, sW, notNull)
            elif sqlType.startswith("INT") or sqlType in ["INTEGER", "BIGINT", "SMALLINT"]:
                tS = "%-40s %-16s  %s" % (name, sqlType, notNull)
            elif sqlType in ["FLOAT", "REAL", "DOUBLE PRECISION"]:
                tS = "%-40s %-16s  %s" % (name, sqlType, notNull)
            elif (sqlType == "DATE") or (sqlType == "DATETIME"):
                tS = "%-40s %-16s  %s" % (name, sqlType, notNull)
            elif (sqlType == "TEXT") or (sqlType == "MEDIUMTEXT") or (sqlType == "LONGTEXT"):
                tS = "%-40s %-16s  %s" % (name, sqlType, notNull)
            elif (sqlType == "DECIMAL") or (sqlType == "NUMERIC"):
                sW = "%-s(%d,%d)" % (sqlType, width, precision)
                tS = "%-40s %-16s  %s" % (name, sW, notNull)
            else:
                pass
            #
            # if ii < len(attributeIdList) -1:
            #    oL.append(tS+",")
            # else:
            #    oL.append(tS)
            oL.append(tS + ",")

        if len(pkL) > 0:
            oL.append("PRIMARY KEY (%s)" % (",".join(pkL)))

        if str(tableDefObj.getType()).upper() == "TRANSACTIONAL":
            oL.append(") ENGINE InnoDB;")
        else:
            oL.append(") ENGINE MyISAM;")
        #
        # return this as list containing a single string command.
        return ["\n".join(oL)]

    def __createTableIndices(self, tableDefObj):
        """Return a list of strings containing the SQL command to create any indices described in
        input table schema definition object.

        """
        oL = []
        tableName = tableDefObj.getName()
        indexNameList = tableDefObj.getIndexNames()
        for indexName in indexNameList:
            indexType = tableDefObj.getIndexType(indexName)
            if str(indexType).upper() == "SEARCH":
                indexType = ""
            tL = []
            tL.append("CREATE %s INDEX %s on %s (" % (indexType, indexName, tableName))
            attributeIdList = tableDefObj.getIndexAttributeIdList(indexName)
            for ii, attributeId in enumerate(attributeIdList):
                name = tableDefObj.getAttributeName(attributeId)
                tS = "%-s" % name
                if ii < len(attributeIdList) - 1:
                    tL.append(tS + ",")
                else:
                    tL.append(tS)
            tL.append(");")
            oL.append(" ".join(tL))

        #
        return oL

    def exportTable(self, databaseName, tableDefObj, exportPath, withDoubleQuotes=False):
        """ """
        tableName = tableDefObj.getName()
        aNames = tableDefObj.getAttributeNameList()
        #
        oL = []
        oL.append("SELECT %s " % ",".join(aNames))
        oL.append(" INTO OUTFILE %s " % exportPath)
        oL.append("FIELDS TERMINATED BY '&##&\\t' ")
        if withDoubleQuotes:
            oL.append(" OPTIONALLY ENCLOSED BY '\"' ")
        oL.append("LINES  TERMINATED BY '$##$\\n' ")
        oL.append("FROM %s.%s " % (databaseName, tableName))
        oL.append(";")
        return "\n".join(oL)

    def importTable(self, databaseName, tableDefObj, importPath, withTruncate=False, withDoubleQuotes=False):
        """Create the SQL commands to data files stored in charactore delimited data files into the
        in put database and table.    Input data may be optionally enclosed in double quotes.

        An options is provied to  pre-truncate the table before loading.

        Return:  a string containing the SQL for the load command.
        """
        tableName = tableDefObj.getName()
        aNames = tableDefObj.getAttributeNameList()
        #
        oL = []
        if withTruncate:
            oL.append("TRUNCATE TABLE %s.%s; " % (databaseName, tableName))

        # oL.append("SET @@GLOBAL.local_infile = 1; ")
        oL.append("LOAD DATA LOCAL INFILE '%s' " % importPath)
        oL.append("INTO TABLE  %s.%s " % (databaseName, tableName))
        oL.append("FIELDS TERMINATED BY '&##&\\t' ")

        if withDoubleQuotes:
            oL.append("OPTIONALLY ENCLOSED BY '\"' ")

        oL.append("LINES  TERMINATED BY '$##$\\n' ")
        oL.append(" (%s) " % ",".join(aNames))
        oL.append(";")
        return " ".join(oL)


class MyDbQuerySqlGen(object):

    """Builds an the SQL command string for a selection query."""

    def __init__(self, schemaDefObj, verbose=False, log=sys.stderr):  # pylint: disable=unused-argument
        """Input:

        schemaDef is instance of class derived from SchemaDefBase().
        """
        self.__schemaDefObj = schemaDefObj
        #
        self.__databaseName = None
        self.__conditionObj = None
        self.__sortOrder = "DESC"
        self.__limitStart = None
        self.__limitLength = None
        self.__setup()

    def __setup(self):
        self.__databaseName = self.__schemaDefObj.getDatabaseName()
        self.__selectList = []
        self.__orderList = []
        self.__conditionObj = None
        self.__sortOrder = "DESC"
        self.__limitStart = None
        self.__limitLength = None
        #

    def setDatabase(self, databaseName):
        self.__databaseName = databaseName

    def clear(self):
        self.__setup()

    def addSelectLimit(self, rowStart=None, rowLength=None):
        try:
            self.__limitStart = int(rowStart)
            self.__limitLength = int(rowLength)
            return True
        except:  # noqa: E722  pylint: disable=bare-except
            return False

    def addSelectAttributeId(self, attributeTuple=(None, None)):
        """Add the input attribute to the current attribute select list.

        where attributeTuple contains (tableId,attributeId)

        """
        self.__selectList.append(attributeTuple)
        return True

    def setOrderBySortOrder(self, dir="ASC"):  # pylint: disable=redefined-builtin
        """The default sort order applied to attributes in the ORDER BY clause. (ASC|DESC)"""
        self.__sortOrder = dir

    def addOrderByAttributeId(self, attributeTuple=(None, None), sortFlag="DEFAULT"):
        """Add the input attribute to the current orderBy list.

        where attributeTuple contains (tableId,attributeId)

        """
        sf = self.__sortOrder if sortFlag == "DEFAULT" else sortFlag
        self.__orderList.append((attributeTuple, sf))
        return True

    def setCondition(self, conditionObj):
        """Set an instance of the condition object from the MyDbConditionSqlGen() class."""
        self.__conditionObj = conditionObj
        return True

    def getSql(self):
        """ """
        return self.__makeSql()

    def __makeSql(self):
        """Builds SQL string for the query from the current list of attributes, list of
        ORDER BY attributes and the constrainObj.
        """
        #
        if len(self.__selectList) == 0:
            return None
        #
        # Attribute names from select list -
        #
        aNames = [self.__schemaDefObj.getQualifiedAttributeName(aTup) for aTup in self.__selectList]
        #
        # Table Id's from the select list -
        #
        tIds = [aTup[0] for aTup in self.__selectList]
        #
        conditionSql = None
        if self.__conditionObj is not None:
            conditionSql = self.__conditionObj.getSql()
            tIds.extend(self.__conditionObj.getTableIdList())
        #
        oNames = []
        if len(self.__orderList) > 0:
            oNames = [self.__schemaDefObj.getQualifiedAttributeName(aTup) + " " + sortFlag for aTup, sortFlag in self.__orderList]
        #
        tIds = list(set(tIds))
        tNames = [self.__databaseName + "." + self.__schemaDefObj.getTableName(tId) for tId in tIds]
        #
        oL = []
        oL.append("SELECT %s " % ",".join(aNames))
        oL.append(" FROM %s " % ",".join(tNames))
        if conditionSql is not None and len(conditionSql) > 0:
            oL.append(" WHERE %s " % conditionSql)
        #
        if len(oNames) > 0:
            oL.append(" ORDER BY %s " % (",".join(oNames)))

        if (self.__limitStart is not None) and (self.__limitLength is not None):
            oL.append(" LIMIT %d, %d " % (self.__limitStart, self.__limitLength))

        oL.append(";")
        #
        #
        return "\n".join(oL)


class MyDbConditionSqlGen(object):

    """Builds the Condition portion of an SQL selection or related query."""

    def __init__(self, schemaDefObj, addKeyJoinFlag=True, verbose=False, log=sys.stderr):
        """Input:

        schemaDef is instance of class derived from SchemaDefBase().
        """
        self.__schemaDefObj = schemaDefObj
        self.__lfh = log
        self.__verbose = verbose
        # self.__ops = ["EQ", "NE", "GE", "GT", "LT", "LE", "LIKE", "NOT LIKE", "IS", "IS NOT"]
        self.__opDict = {"EQ": "=", "NE": "!=", "GE": ">=", "GT": ">", "LT": "<", "LE": "<=", "LIKE": "LIKE", "NOT LIKE": "NOT LIKE", "IS": "IS", "IS NOT": "IS NOT"}
        # self.__logOps = ["AND", "OR", "NOT"]
        # self.__grpOps = ["BEGIN", "END"]
        #
        self.__cList = []
        self.__tableIdList = []
        self.__numConditions = 0
        self.__addKeyJoinFlag = addKeyJoinFlag
        #

    def clear(self):
        self.__cList = []
        self.__tableIdList = []
        self.__numConditions = 0
        return True

    def set(self, conditionDefList=None):
        """Set/reset the current condition list --- The input is used verbatim and unmodified."""
        if conditionDefList is not None:
            self.__cList = conditionDefList
            self.__tableIdList = []
            for c in self.__cList:
                self.__updateTableList(c)
            return True
        else:
            return False

    def __updateTableList(self, cObj):
        """Add the tables included in the input condition to the internal table list."""
        cType = cObj[0]
        if cType in ["VALUE_CONDITION", "VALUE_LIST_CONDITION"]:
            cType, lhsTuple, _opCode, rhsTuple = cObj
            lTableId, _lAttributeId = lhsTuple
            self.__addTable(lTableId)
            return True
        elif cType in ["JOIN_CONDITION"]:
            cType, lhsTuple, _opCode, rhsTuple = cObj
            lTableId, _lAttributeId = lhsTuple
            rTableId, _rAttributeId = rhsTuple
            self.__addTable(lTableId)
            self.__addTable(rTableId)
            return True
        else:
            return False

    def __addTable(self, tableId):
        if tableId not in self.__tableIdList:
            self.__tableIdList.append(tableId)
            return True
        else:
            return False

    def get(self):
        return self.__cList

    def getSql(self):
        if self.__addKeyJoinFlag:
            self.addKeyAttributeEquiJoinConditions()
        return self.__makeSql()

    def getTableIdList(self):
        return self.__tableIdList

    def addTables(self, tableIdList):
        """Add the tables from the input tableIdList to the internal list of tables.

        The internal list of tables is used to materialize join contraints between
        all tables based on primary keys defined in the schema definition.
        """
        for tableId in tableIdList:
            self.__addTable(tableId)
        return True

    def addValueCondition(self, lhsTuple=None, opCode=None, rhsTuple=None, preOp="AND"):
        """Adds a condition to the current contraint list -

         lhsTuple = (TableId,AttributeId)
         opCode   = one of the operations defined in self.__opDict.keys()
         rhsTuple = (value,type) where for

                    simple values -

                    (simpleValue,'CHAR|OTHER')
        preOp = logical operator preceding this contraint in the current contraint list.

        """
        cObj = ("VALUE_CONDITION", lhsTuple, opCode, rhsTuple)
        if cObj not in self.__cList:
            self.__updateTableList(cObj)
            if preOp in ["AND", "OR"]:
                self.addLogicalOp(lOp=preOp)
            self.addBeginGroup()
            self.__cList.append(cObj)
            self.addEndGroup()
            self.__numConditions += 1
        return self.__numConditions

    def addGroupValueConditionList(self, cDefList, preOp="AND"):
        """Add a value alternative condition to the current contraint list
         using the input list of value condition definitions defined as -

         cDefList = [(lPreOp,lhsTuple, opCode, rhsTuple), ...]
           lPreOp   = local logical conjunction used to add condition within the group (leading value is ignored)
           lhsTuple = (TableId,AttributeId)
           opCode   = one of the operations defined in self.__opDict.keys()
           rhsTuple = (value,type) where for

                    simple values are defined as -

                    (simpleValue,'CHAR|<ANY OTHER>')   < CHAR > types are quoted

        preOp is the logical conjuction used to add the group condition to current condition list.

        """
        if len(cDefList) < 1:
            return self.__numConditions
        if preOp in ["AND", "OR"]:
            self.addLogicalOp(lOp=preOp)
        #
        self.addBeginGroup()
        for ii, cDef in enumerate(cDefList):
            (lPreOp, lhsTuple, opCode, rhsTuple) = cDef
            cObj = ("VALUE_CONDITION", lhsTuple, opCode, rhsTuple)
            self.__updateTableList(cObj)
            if ii > 0:
                self.addLogicalOp(lOp=lPreOp)
            self.addBeginGroup()
            self.__cList.append(cObj)
            self.addEndGroup()

            self.__numConditions += 1

        self.addEndGroup()
        return self.__numConditions

    def addJoinCondition(self, lhsTuple=None, opCode=None, rhsTuple=None, preOp="AND"):
        """Adds a join condition to the current contraint list -

        lhsTuple = (TableId,AttributeId)
        opCode   = one of the operations defined in self.__opDict.keys()
        rhsTuple = (TableId,AttributeId)

                    For join conditions rhsTuple values implemented as -

                    (tableId,attributeId)

                    rhsTuple type is currrently only implemented as 'ATTRIBUTE' target but
                             but could be extended to support other targets for different operators.

        preOp = logical operator preceding this contraint in the current contraint list.

        """
        cObj = ("JOIN_CONDITION", lhsTuple, opCode, rhsTuple)
        if cObj not in self.__cList:
            self.__updateTableList(cObj)
            if preOp in ["AND", "OR"]:
                self.addLogicalOp(lOp=preOp)
            self.addBeginGroup()
            self.__cList.append(cObj)
            self.addEndGroup()
            self.__numConditions += 1
        return self.__numConditions

    def addLogicalOp(self, lOp):
        """Adds a logical operation into the current condition list.

        lOp  =  one of 'AND','OR', 'NOT'
        """
        self.__cList.append(("LOG_OP", lOp))

    def addBeginGroup(self):
        """Inserts the beginning of a parenthetical group in the current condition list."""
        self.__cList.append(("GROUPING", "BEGIN"))

    def addEndGroup(self):
        """Inserts the ending of a parenthetical group in the current condition list."""
        self.__cList.append(("GROUPING", "END"))

    def addKeyAttributeEquiJoinConditions(self):
        """Auto add equi-join contraints between tables in the current table list -"""
        if len(self.__tableIdList) < 2:
            return 0
        cList = copy.deepcopy(self.__cList)
        self.__cList = []
        tablePairList = [t for t in itertools.combinations(self.__tableIdList, 2)]
        for (t1, t2) in tablePairList:
            self.__addInterTableJoinContraints(t1, t2)
        for c in cList:
            if c[0] in ["JOIN_CONDITION"] and c in self.__cList:
                continue
            self.__cList.append(c)

        return len(tablePairList)

    def __makeSql(self):
        """Builds SQL string for the query condition encoded in the input contraint command list.

        The condition command list is a sequence of tuples with the following syntax:

        ('VALUE_CONDITION', (tableId,attributeId), 'EQ'|'NE'|GE'|'GT'|'LT'|'LE'|'LIKE'|'IS'|'IS NOT', (Value, 'CHAR'|'OTHER'))

        ('VALUE_LIST_CONDITION', (tableId,attributeId), 'IN'|'NOT IN', (valueList, 'CHAR'|'INT'|'FLOAT'))

        ('JOIN_CONDITION',  (tableId,attributeId), 'EQ'|'NE'|'GE'|'GT'|'LT'|'LE'|'LIKE', (tableId,attributeId) )

        ('LOG_OP', 'AND'|'OR'|'NOT')     -> conjunction / negation

        ('GROUPING', 'BEGIN'|'END')          -> (grouping/parenthetical control)

        """
        #
        cSqlL = []
        #
        #
        cCount = 0
        for c in self.__cList:
            cType = (c[0]).upper()
            if cType == "VALUE_CONDITION":
                cCount += 1
                (_cT, lhsTuple, opId, rhsTuple) = c
                (_tableId, _attributeId) = lhsTuple
                (value, vType) = rhsTuple
                vType = vType.upper()
                qAttributeName = self.__schemaDefObj.getQualifiedAttributeName(tableAttributeTuple=lhsTuple)
                # jdw quote date and datetime
                if vType in ["CHAR", "VARCHAR", "DATE", "DATETIME"]:
                    cSqlL.append(" %s %s '%s' " % (qAttributeName, self.__opDict[opId], value))
                else:
                    cSqlL.append(" %s %s %s " % (qAttributeName, self.__opDict[opId], value))

            elif cType == "VALUE_LIST_CONDITION":
                cCount += 1
                (_cT, lhsTuple, opId, rhsTuple) = c
                # (_tableId, _attributeId) = lhsTuple
                (valueList, vType) = rhsTuple
                vType = vType.upper()
                qAttributeName = self.__schemaDefObj.getQualifiedAttributeName(tableAttributeTuple=lhsTuple)
                #
                qVL = []
                for value in valueList:
                    if vType in ["CHAR", "VARCHAR"]:
                        qV = "'%s'" % value
                    elif vType in ["INT", "INTEGER"]:
                        qV = "%i" % value
                    elif vType in ["FLOAT", "DOUBLE", "DECIMAL"]:
                        qV = "%f" % value
                    else:
                        qV = value
                    qVL.append(qV)
                vS = ",".join
                cSqlL.append(" %s %s [ %s ] " % (qAttributeName, self.__opDict[opId], vS))

            elif cType == "JOIN_CONDITION":
                cCount += 1
                (_cT, lhsTuple, opId, rhsTuple) = c
                # (_lTableId, _lAttributeId) = lhsTuple
                # (_rTableId, _rAttributeId) = rhsTuple
                lAttributeName = self.__schemaDefObj.getQualifiedAttributeName(tableAttributeTuple=lhsTuple)
                rAttributeName = self.__schemaDefObj.getQualifiedAttributeName(tableAttributeTuple=rhsTuple)
                #
                cSqlL.append(" %s %s %s " % (lAttributeName, self.__opDict[opId], rAttributeName))

            elif cType == "LOG_OP":
                (_cT, logOp) = c
                if logOp not in ["NOT"] and cCount > 0:
                    cSqlL.append(" %s " % logOp)
            elif cType == "GROUPING":
                (_cT, group) = c
                if group == "BEGIN":
                    cSqlL.append(" (  ")
                elif group == "END":
                    cSqlL.append(" ) ")
            else:
                pass

        return "\n".join(cSqlL)

    def __addInterTableJoinContraints(self, lTableId, rTableId):
        """The ..."""
        lTdef = self.__schemaDefObj.getTable(lTableId)
        lKeyAttributeIdL = lTdef.getPrimaryKeyAttributeIdList()
        rTdef = self.__schemaDefObj.getTable(rTableId)
        rKeyAttributeIdL = rTdef.getPrimaryKeyAttributeIdList()
        #
        commonAttributeIdSet = set(lKeyAttributeIdL) & set(rKeyAttributeIdL)
        if self.__verbose:
            self.__lfh.write("+MyDbConditionSqlGen.__addInterTableJoinConditions lTable %s rTable %s  common keys %r\n" % (lTableId, rTableId, commonAttributeIdSet))
        #
        for attributeId in commonAttributeIdSet:
            lhsTuple = (lTableId, attributeId)
            opCode = "EQ"
            rhsTuple = (rTableId, attributeId)
            self.addJoinCondition(lhsTuple=lhsTuple, opCode=opCode, rhsTuple=rhsTuple, preOp="AND")
        #
        return len(commonAttributeIdSet)
