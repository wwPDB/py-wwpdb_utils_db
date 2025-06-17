##
# File:  MyDdAdapter.py
# Date:  10-April-2014  J.Westbrook
#
# Updates:
#
# 11-April-2014  jdw Generalized from WFTaskRequestDBAdapter.py
# 13-April-2014  jdw working with workflow schema WFTaskRequest() -
# 19-Feb  -2015  jdw various fixes
# 10-Jul-2015    jdw Change method/class names from MySqlGen
#
#
###
##
"""
Database adapter for managing queries and persistent storage of workflow task status and tracking.

This software was developed as part of the World Wide Protein Data Bank
Common Deposition and Annotation System Project

Copyright (c) 2010-2014 wwPDB

This software is provided under a Creative Commons Attribution 3.0 Unported
License described at http://creativecommons.org/licenses/by/3.0/.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.07"

import copy
import logging
import sys
import time

from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId
from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen, MyDbConditionSqlGen, MyDbQuerySqlGen
from wwpdb.utils.db.MyDbUtil import MyDbConnect, MyDbQuery

logger = logging.getLogger(__name__)


class MyDbAdapter:
    """Database adapter for managing simple access and persistance queries using a relational database store."""

    def __init__(self, schemaDefObj, verbose=False, log=sys.stderr):
        self.__verbose = verbose
        self.__lfh = log
        self.__debug = False
        #
        self.__siteId = getSiteId(defaultSiteId="WWPDB_DEPLOY_MACOSX")
        self.__cI = ConfigInfo(self.__siteId)
        #
        self.__sd = schemaDefObj
        self.__databaseName = self.__sd.getDatabaseName()
        self.__dbCon = None
        self.__defaultD = {}
        self.__attributeParameterMap = {}
        self.__attributeConstraintParameterMap = {}

    def _setDebug(self, flag=True):
        self.__debug = flag

    def _setDataStore(self, dataStoreName):
        """Set/reassign the database for all subsequent transactions."""
        self.__databaseName = dataStoreName

    def _getParameterDefaultValues(self, contextId):
        if contextId is not None and contextId in self.__defaultD:
            return self.__defaultD[contextId]
        return {}

    def _setParameterDefaultValues(self, contextId, valueD):
        """Set the optional lookup dictionary of default values for unspecified parameters...

        valueD = { 'paramName1': <default value1>,  'paramName2' : <default value2>, ...  }
        """
        self.__defaultD[contextId] = copy.deepcopy(valueD)
        return True

    def _setAttributeParameterMap(self, tableId, mapL):
        """Set list of correspondences between method parameters and table attribute IDs.

        These correspondences are used to map key-value parameter pairs to their associated table attribute values.

        mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        self.__attributeParameterMap[tableId] = mapL
        return True

    def _getDefaultAttributeParameterMap(self, tableId):
        """Return default attributeId parameter name mappings for the input tableId.

        mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        return self.__sd.getDefaultAttributeParameterMap(tableId)

    def _getAttributeParameterMap(self, tableId):
        """
        For the input table return the method keyword argument name to table attribute mapping -
        """
        if tableId is not None and tableId in self.__attributeParameterMap:
            return self.__attributeParameterMap[tableId]
        return []

    def _getConstraintParameterMap(self, tableId):
        """
        For the input table return the method keyword argument name to table attribute mapping for
        those attributes that serve as constraints for update transactions -

        """
        if tableId is not None and tableId in self.__attributeConstraintParameterMap:
            return self.__attributeConstraintParameterMap[tableId]
        return []

    def _setConstraintParameterMap(self, tableId, mapL):
        """Set list of correspondences between method parameters and table attribute IDs to be used as
        contraints in update operations.

        These correspondences are used to map key-value paramter pairs to their associated table attribute values.

        mapL=[ (atId1,paramName1),(atId2,paramName2),... ]
        """
        self.__attributeConstraintParameterMap[tableId] = mapL
        return True

    def _open(self, dbServer=None, dbHost=None, dbName=None, dbUser=None, dbPw=None, dbSocket=None, dbPort=None):
        """Open a connection to the data base server hosting WF status and tracking data -

        Internal configuration details will be used if these are not externally supplied.
        """
        #
        # WF Status and tracking data base connection details
        #
        dbHostX = dbHost if dbHost is not None else self.__cI.get("SITE_DB_HOST_NAME")
        dbPortX = dbPort if dbPort is not None else self.__cI.get("SITE_DB_PORT_NUMBER")
        dbNameX = dbName if dbName is not None else self.__cI.get("SITE_DB_DATABASE_NAME")
        dbUserX = dbUser if dbUser is not None else self.__cI.get("SITE_DB_USER_NAME")
        dbPwX = dbPw if dbPw is not None else self.__cI.get("SITE_DB_PASSWORD")
        dbServerX = dbServer if dbServer is not None else self.__cI.get("SITE_DB_SERVER")
        dbSocketX = dbSocket if dbSocket is not None else self.__cI.get("SITE_DB_SOCKET")
        #
        myC = MyDbConnect(
            dbServer=dbServerX,
            dbHost=dbHostX,
            dbName=dbNameX,
            dbUser=dbUserX,
            dbPw=dbPwX,
            dbSocket=dbSocketX,
            dbPort=dbPortX,
            verbose=self.__verbose,
            log=self.__lfh,
        )
        self.__dbCon = myC.connect()
        if self.__dbCon is not None:
            return True
        return False

    def _close(self):
        """Close connection to the data base server hosting WF status and tracking data -"""
        if self.__dbCon is not None:
            self.__dbCon.close()
            self.__dbCon = None

    def _createSchema(self):
        """Create table schema using the current class schema definition"""
        if self.__debug:
            startTime = time.time()
            logger.debug("Starting _createSchema at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableIdList = self.__sd.getTableIdList()
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)

            for tableId in tableIdList:
                sqlL = []
                tableDefObj = self.__sd.getTable(tableId)
                sqlL.extend(myAd.createTableSQL(databaseName=self.__databaseName, tableDefObj=tableDefObj))

                ret = myQ.sqlCommand(sqlCommandList=sqlL)
                if self.__verbose:
                    logger.info("for tableId %s server returns: %s", tableId, ret)
                if self.__debug:
                    logger.debug("SQL: %s", "\n".join(sqlL))
            if iOpened:
                self._close()
        except Exception as e:
            status = " table create error " + str(e)
            logger.error("%s", status)
            if self.__verbose:
                logger.exception("_createSchema")

        if self.__debug:
            endTime = time.time()
            logger.debug("Completed at %s (%.3f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        return ret

    def _getSecondsSinceEpoch(self):
        """Return number of seconds since the epoch at the precision of the local installation.
        Typically a floating point value with microsecond precision.

        This is used as the default time reference (e.g. timestamp) for monitoring task requests.
        """
        return time.time()

    def _insertRequest(self, tableId, contextId, **kwargs):
        """Insert into the input table using the keyword value pairs provided as input arguments -

        The contextId controls the handling default values for unspecified parameters.
        """
        startTime = time.time()
        if self.__debug:
            logger.debug("Starting _insertRequest() %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            # tableName = self.__sd.getTableName(tableId)
            tableDefObj = self.__sd.getTable(tableId)
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            defaultValD = self._getParameterDefaultValues(contextId=contextId)
            #
            # Create the attribute and value list for template --
            #
            vList = []
            aList = []
            for atId, kwId in self._getAttributeParameterMap(tableId=tableId):
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)
                elif kwId in defaultValD and defaultValD[kwId] is not None:
                    vList.append(defaultValD[kwId])
                    aList.append(atId)
                else:
                    # appropriate null handling -- all fields must be assigned on insert --
                    vList.append(tableDefObj.getSqlNullValue(atId))
                    aList.append(atId)

            sqlT = myAd.idInsertTemplateSQL(self.__databaseName, tableDefObj, insertAttributeIdList=aList)
            if self.__debug:
                logger.debug("_insertRequest  aList %d vList %d", len(aList), len(vList))
                logger.debug("_insertRequest insert template sql=%s", sqlT)
                logger.debug("_insertRequest insert values vList=%r", vList)
                # sqlC = sqlT % vList
                # self.__lfh.write("+%s.%s insert sql command =\n%s\n" % (self.__class__.__name__, sys._getframe().f_code.co_name, sqlC))
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)
            if iOpened:
                self._close()

        except Exception as e:
            status = " insert operation error " + str(e)
            logger.error("%s", status)
            if self.__verbose:
                logger.exception("Exception in _insertRequest")
        if self.__debug:
            endTime = time.time()
            logger.debug("_insertRequest completed at %s (%.3f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)

        return ret

    def _updateRequest(self, tableId, contextId, **kwargs):
        """Update the input table using the keyword value pairs provided as input arguments -

        The contextId controls the handling default values for unspecified parameters.

        """
        startTime = time.time()
        if self.__debug:
            logger.debug("Starting _updateRequest at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            # tableName = self.__sd.getTableName(tableId)
            tableDefObj = self.__sd.getTable(tableId)
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            defaultValD = self._getParameterDefaultValues(contextId=contextId)
            cIdList = self._getConstraintParameterMap(tableId)

            #
            # create the value list for template --
            #
            vList = []
            aList = []
            cList = []
            for atId, kwId in self._getAttributeParameterMap(tableId):
                if (atId, kwId) in cIdList:
                    continue
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)
                elif kwId in defaultValD and defaultValD[kwId] is not None:
                    vList.append(defaultValD[kwId])
                    aList.append(atId)

            for atId, kwId in cIdList:
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    cList.append(atId)

            sqlT = myAd.idUpdateTemplateSQL(self.__databaseName, tableDefObj, updateAttributeIdList=aList, conditionAttributeIdList=cList)
            if self.__debug:
                logger.debug("update sql: %s", sqlT)
                logger.debug("update values: %r", vList)
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)
            if iOpened:
                self._close()

        except Exception as e:
            status = " update operation error " + str(e)
            logger.error("_updateRequest %s", status)
            if self.__verbose:
                logger.exception("%s", status)
        if self.__debug:
            endTime = time.time()
            logger.debug("Completed _updateRequest %s (%.3f seconds)\n", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        return ret

    def _select(self, tableId, **kwargs):
        """Construct a selection query for input table and optional constraints provided as keyword value pairs in the
        input arguments.  Return a list of dictionaries of these query details including all table attributes.
        """
        startTime = time.time()
        if self.__debug:
            logger.debug("Starting _select at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        rdList = []
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True
            #
            tableDefObj = self.__sd.getTable(tableId)
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            sqlGen = MyDbQuerySqlGen(schemaDefObj=self.__sd, verbose=self.__verbose, log=self.__lfh)
            sqlGen.setDatabase(databaseName=self.__databaseName)
            sqlConstraint = MyDbConditionSqlGen(schemaDefObj=self.__sd, verbose=self.__verbose, log=self.__lfh)
            #
            atMapL = self._getAttributeParameterMap(tableId=tableId)
            for kwArg in kwargs:
                for atId, kwId in atMapL:
                    if kwId == kwArg:
                        if tableDefObj.isAttributeStringType(atId):
                            cTup = ((tableId, atId), "EQ", (kwargs[kwId], "CHAR"))
                        else:
                            cTup = ((tableId, atId), "EQ", (kwargs[kwId], "OTHER"))
                        sqlConstraint.addValueCondition(cTup[0], cTup[1], cTup[2])
                        break
            #
            # Add optional constraints OR ordering by primary key attributes
            if len(sqlConstraint.get()) > 0:
                sqlGen.setCondition(sqlConstraint)
            else:
                for atId in tableDefObj.getPrimaryKeyAttributeIdList():
                    sqlGen.addOrderByAttributeId(attributeTuple=(tableId, atId))

            atIdList = self.__sd.getAttributeIdList(tableId)
            for atId in atIdList:
                sqlGen.addSelectAttributeId(attributeTuple=(tableId, atId))
            #
            sqlS = sqlGen.getSql()
            if self.__debug:
                logger.debug("_select selection sql: %s", sqlS)

            rowList = myQ.selectRows(queryString=sqlS)
            sqlGen.clear()
            #
            # return the result set as a list of dictionaries
            #
            for iRow, row in enumerate(rowList):
                rD = {}
                for colVal, atId in zip(row, atIdList):
                    rD[atId] = colVal
                if self.__debug:
                    logger.debug("_select result set row %d dictionary %r", iRow, rD.items())
                rdList.append(rD)
            if iOpened:
                self._close()
        except Exception as e:
            status = " operation error " + str(e)
            logger.error("_select %s", status)
            if self.__verbose:
                logger.exception("_select failed")

        if self.__debug:
            endTime = time.time()
            logger.debug("Completed _select at %s (%.3f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        return rdList

    def _deleteRequest(self, tableId, **kwargs):
        """Delete from input table records identified by the keyword value pairs provided as input arguments -"""
        startTime = time.time()
        if self.__debug:
            logger.debug("Starting _deleteRequest at %s", time.strftime("%Y %m %d %H:%M:%S", time.localtime()))
        ret = False
        try:
            iOpened = False
            if self.__dbCon is None:
                self._open()
                iOpened = True

            # tableName = self.__sd.getTableName(tableId)
            tableDefObj = self.__sd.getTable(tableId)
            #
            #
            myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
            myAd = MyDbAdminSqlGen(self.__verbose, self.__lfh)
            #
            # Create the attribute and value list for template --
            #
            vList = []
            aList = []
            for atId, kwId in self._getAttributeParameterMap(tableId):
                if kwId in kwargs and kwargs[kwId] is not None:
                    vList.append(kwargs[kwId])
                    aList.append(atId)

            sqlT = myAd.idDeleteTemplateSQL(self.__databaseName, tableDefObj, conditionAttributeIdList=aList)
            if self.__debug:
                logger.debug("_deleteRequest delete sql: %s", sqlT)
                logger.debug("_deleteReuqest delete values: %r", vList)
            ret = myQ.sqlTemplateCommand(sqlTemplate=sqlT, valueList=vList)

            if iOpened:
                self._close()

        except Exception as e:
            status = " delete operation error " + str(e)
            logger.error("_deleteRequest %s", status)
            if self.__verbose:
                logger.exception("In _deleteRequest")

        if self.__debug:
            endTime = time.time()
            logger.debug("Completed _deleteRequest at %s (%.3f seconds)", time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
        return ret
