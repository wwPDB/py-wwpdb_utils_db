##
# File:    SchemaDefLoader.py
# Author:  J. Westbrook
# Date:    7-Jan-2013
# Version: 0.001 Initial version
#
# Updates:
#  9-Jan-2013 jdw add merging index support for loading tables from multiple
#                 instance categories.
# 10-Jan-2013 jdw add null value filter and maximum string width checks.
# 13-Jan-2013 jdw provide batch file and batch insert loading modes.
# 15-Jan-2013 jdw add pre-load delete options
# 19-Jan-2012 jdw add IoAdapter
# 20-Jan-2013 jdw add append options for batch file loading
# 20-Jan-2013 jdw provide methods for loading container lists
#
#
##
"""
Generic mapper of PDBx/mmCIF instance data to SQL loadable data files based on external
schema definition defined in class SchemaDefBase().

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.001"

import contextlib
import os
import sys
import time
import traceback

from mmcif.io.IoAdapterCore import IoAdapterCore

from wwpdb.utils.db.MyDbSqlGen import MyDbAdminSqlGen
from wwpdb.utils.db.MyDbUtil import MyDbQuery


class SchemaDefLoader:
    """Map PDBx/mmCIF instance data to SQL loadable data using external schema definition."""

    def __init__(self, schemaDefObj, ioObj=None, dbCon=None, workPath=".", cleanUp=False, warnings="default", verbose=True, log=sys.stderr):
        if ioObj is None:
            ioObj = IoAdapterCore()
        self.__lfh = log
        self.__verbose = verbose
        self.__debug = False
        self.__sD = schemaDefObj
        self.__ioObj = ioObj
        #
        self.__dbCon = dbCon
        self.__workingPath = workPath
        self.__cleanUp = cleanUp
        #
        self.__colSep = "&##&\t"
        self.__rowSep = "$##$\n"
        #
        self.__warningAction = warnings
        self.__overWrite = {}

    def setWarning(self, action):
        if action in ["error", "ignore", "default"]:
            self.__warningAction = action
            return True
        self.__warningAction = "default"
        return False

    def setDelimiters(self, colSep=None, rowSep=None):
        """Set column and row delimiters for intermediate data files used for
        batch-file loading operations.
        """
        self.__colSep = colSep if colSep is not None else "&##&\t"
        self.__rowSep = rowSep if rowSep is not None else "$##$\n"
        return True

    def load(self, inputPathList=None, containerList=None, loadType="batch-file", deleteOpt=None):
        """Load data for each table defined in the current schema definition object.
        Data are extracted from the input file list.

        Data source options:

          inputPathList = [<full path of target input file>, ....]

        or

          containerList = [ data container, ...]


        loadType  =  ['batch-file' | 'batch-insert']
        deleteOpt = 'selected' | 'all'

        Loading is performed using the current database server connection.

        Intermediate data files for 'batch-file' loading are created in the current working path.

        Returns True for success or False otherwise.

        """
        if inputPathList is not None:
            tableDataDict, containerNameList = self.__fetch(inputPathList)
        elif containerList is not None:
            tableDataDict, containerNameList = self.__process(containerList)
        else:
            tableDataDict = containerNameList = []
        #
        #
        if self.__verbose:
            if len(self.__overWrite) > 0:
                for k, v in self.__overWrite.items():
                    self.__lfh.write("+SchemaDefLoader(load) %r maximum width %r\n" % (k, v))
        #
        if loadType in ["batch-file", "batch-file-append"]:
            append = True if loadType == "batch-file-append" else False  # noqa: SIM210
            exportList = self.__export(tableDataDict, colSep=self.__colSep, rowSep=self.__rowSep, append=append)
            for tableId, loadPath in exportList:
                self.__batchFileImport(tableId, loadPath, sqlFilePath=None, containerNameList=containerNameList, deleteOpt=deleteOpt)
                if self.__cleanUp:
                    self.__cleanUpFile(loadPath)
            return True
        if loadType == "batch-insert":
            for tableId, rowList in tableDataDict.items():
                if deleteOpt in ["all", "selected"] or len(rowList) > 0:
                    self.__batchInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt)
            return True

        return False

    def __cleanUpFile(self, filePath):
        with contextlib.suppress(Exception):
            os.remove(filePath)

    def makeLoadFilesMulti(self, dataList, procName, optionsD, workingDir):  # noqa: ARG002 pylint: disable=unused-argument
        """Create a loadable data file for each table defined in the current schema
        definition object.   Data is extracted from the input file list.

        Load files are creating in the current working path.

        Return the containerNames for the input path list, and path list for load files that are created.

        """
        r1, r2 = self.makeLoadFiles(inputPathList=dataList, partName=procName)
        return dataList, r1, r2, []

    def makeLoadFiles(self, inputPathList, append=False, partName="1"):
        """Create a loadable data file for each table defined in the current schema
        definition object.   Data is extracted from the input file list.

        Load files are creating in the current working path.

        Return the containerNames for the input path list, and path list for load files that are created.

        """
        tableDataDict, containerNameList = self.__fetch(inputPathList)
        return containerNameList, self.__export(tableDataDict, colSep=self.__colSep, rowSep=self.__rowSep, append=append, partName=partName)

    def loadBatchFiles(self, loadList=None, containerNameList=None, deleteOpt=None):
        """Load data for each table defined in the current schema definition object using

        Data source options:

          loadList = [(tableId, <full path of load file), ....]
          containerNameList = [ data namecontainer, ...]

        deleteOpt = 'selected' | 'all','truncate'

        Loading is performed using the current database server connection.

        Returns True for success or False otherwise.

        """
        #
        startTime = time.time()
        for tableId, loadPath in loadList:
            ok = self.__batchFileImport(tableId, loadPath, sqlFilePath=None, containerNameList=containerNameList, deleteOpt=deleteOpt)
            if not ok:
                break
            if self.__cleanUp:
                self.__cleanUpFile(loadPath)
        #
        endTime = time.time()
        if self.__verbose:
            self.__lfh.write(
                "+SchemaDefLoader(loadBatchFiles) completed with status %r at %s (%.3f seconds)\n"
                % (ok, time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )
        return ok

    def fetchMulti(self, dataList, procName, optionsD, workingDir):  # noqa: ARG002  pylint: disable=unused-argument
        """Method to comply with the MultiProcPoolUtil interface. This method should only
        be used with MultiProcPoolUtil, passing its name through the argument 'workerMethod'.
        """
        tableDataDict, containerNameList = self.__fetch(loadPathList=dataList)
        return dataList, containerNameList, [tableDataDict], []

    def fetch(self, inputPathList):
        """Return a dictionary of loadable data for each table defined in the current schema
        definition object.   Data is extracted from the input file list.
        """
        return self.__fetch(inputPathList)

    def __fetch(self, loadPathList):
        """Internal method to create loadable data corresponding to the table schema definition
        from the input list of data files.

        Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                            and
                 container name list. []

        """
        startTime = time.time()
        #
        containerNameList = []
        tableDataDict = {}
        tableIdList = self.__sD.getTableIdList()
        for lPath in loadPathList:
            myContainerList = self.__ioObj.readFile(lPath)
            self.__mapData(myContainerList, tableIdList, tableDataDict)
            containerNameList.extend([myC.getName() for myC in myContainerList])
        #
        endTime = time.time()
        if self.__verbose:
            self.__lfh.write(
                "+SchemaDefLoader(__fetch) completed at %s (%.3f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )

        return tableDataDict, containerNameList

    def process(self, containerList):
        return self.__process(containerList)

    def __process(self, containerList):
        """Internal method to create loadable data corresponding to the table schema definition
        from the input container list.

        Returns: dicitonary d[<tableId>] = [ row1Dict[attributeId]=value,  row2dict[], .. ]
                            and
                 container name list. []
        """
        startTime = time.time()
        #
        containerNameList = []
        tableDataDict = {}
        tableIdList = self.__sD.getTableIdList()
        self.__mapData(containerList, tableIdList, tableDataDict)
        containerNameList.extend([myC.getName() for myC in containerList])
        #
        if self.__debug:
            self.__lfh.write("+SchemaDefLoader(__process) container name list: %r\n" % containerNameList)
        #
        endTime = time.time()
        if self.__verbose:
            self.__lfh.write(
                "+SchemaDefLoader(__process) completed at %s (%.3f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )

        return tableDataDict, containerNameList

    def export(self, tableDict, append=False, partName="1"):
        """Method to create a loadable file from the table dictionary returned
        from __fetch.

        Returns:
            [type]: [description]
        """
        return self.__export(tableDict=tableDict, append=append, partName=partName)

    def __export(self, tableDict, colSep="&##&\t", rowSep="$##$\n", append=False, partName="1"):
        modeOpt = "a" if append else "w"

        exportList = []
        for tableId, rowList in tableDict.items():
            tObj = self.__sD.getTable(tableId)
            schemaAttributeIdList = tObj.getAttributeIdList()
            #
            if len(rowList) > 0:
                fn = os.path.join(self.__workingPath, tableId + "-loadable-" + partName + ".tdd")
                ofh = open(fn, modeOpt)
                for rD in rowList:
                    ofh.write("%s%s" % (colSep.join([rD[aId] for aId in schemaAttributeIdList]), rowSep))
                ofh.close()
                exportList.append((tableId, fn))
        return exportList

    def __evalMapFunction(self, dataContainer, rowList, attributeId, functionName, functionArgs=None):  # noqa: ARG002  pylint: disable=unused-argument
        if functionName == "datablockid()":
            val = dataContainer.getName()
            for rowD in rowList:
                rowD[attributeId] = val
            return True
        return False

    def __mapData(self, containerList, tableIdList, tableDataDict):
        """
        Process instance data in the input container list and map these data to the
        table schema definitions in the input table list.

        Returns: mapped data as a list of dictionaries with attribute Id key for
                 each schema table.  Data are appended to any existing table in
                 the input dictionary.


        """
        for myContainer in containerList:
            for tableId in tableIdList:
                if tableId not in tableDataDict:
                    tableDataDict[tableId] = []
                tObj = self.__sD.getTable(tableId)
                #
                # Instance categories that are mapped to the current table -
                #
                mapCategoryNameList = tObj.getMapInstanceCategoryList()
                numMapCategories = len(mapCategoryNameList)
                #
                # Attribute Ids that are not directly mapped to the schema (e.g. functions)
                #
                otherAttributeIdList = tObj.getMapOtherAttributeIdList()

                if numMapCategories == 1:
                    rowList = self.__mapInstanceCategory(tObj, mapCategoryNameList[0], myContainer)
                elif numMapCategories >= 1:
                    rowList = self.__mapInstanceCategoryList(tObj, mapCategoryNameList, myContainer)
                else:
                    rowList = []

                for atId in otherAttributeIdList:
                    fName = tObj.getMapAttributeFunction(atId)
                    fArgs = tObj.getMapAttributeFunctionArgs(atId)
                    self.__evalMapFunction(dataContainer=myContainer, rowList=rowList, attributeId=atId, functionName=fName, functionArgs=fArgs)

                tableDataDict[tableId].extend(rowList)
        return tableDataDict

    def __mapInstanceCategory(self, tObj, categoryName, myContainer):
        """Extract data from the input instance category and map these data to the organization
        in the input table schema definition object.

        No merging is performed by this method.

        Return a list of dictionaries with schema attribute Id keys containing data
        mapped from the input instance category.
        """
        #
        retList = []
        catObj = myContainer.getObj(categoryName)
        if catObj is None:
            return retList

        attributeIndexDict = catObj.getAttributeIndexDict()
        schemaTableId = tObj.getId()
        schemaAttributeMapDict = tObj.getMapAttributeDict()
        schemaAttributeIdList = tObj.getAttributeIdList()
        nullValueDict = tObj.getSqlNullValueDict()
        maxWidthDict = tObj.getStringWidthDict()
        curAttributeIdList = tObj.getMapInstanceAttributeIdList(categoryName)

        for row in catObj.getRowList():
            d = {}
            for atId in schemaAttributeIdList:
                d[atId] = nullValueDict[atId]

            for atId in curAttributeIdList:
                try:
                    atName = schemaAttributeMapDict[atId]
                    if atName not in attributeIndexDict:
                        continue
                    val = row[attributeIndexDict[atName]]
                    maxW = maxWidthDict[atId]
                    if maxW > 0:
                        lenVal = len(val)
                        if lenVal > maxW:
                            tup = (schemaTableId, atId)
                            if tup in self.__overWrite:
                                self.__overWrite[tup] = max(self.__overWrite[tup], lenVal)
                            else:
                                self.__overWrite[tup] = lenVal

                        d[atId] = val[:maxW] if ((val != "?") and (val != ".")) else nullValueDict[atId]
                    else:
                        d[atId] = val if ((val != "?") and (val != ".")) else nullValueDict[atId]
                except:  # noqa: E722 pylint: disable=bare-except
                    if self.__verbose:
                        self.__lfh.write("\n+ERROR - processing table %s attribute %s row %r\n" % (schemaTableId, atId, row))
                        traceback.print_exc(file=self.__lfh)

            retList.append(d)

        return retList

    def __mapInstanceCategoryList(self, tObj, categoryNameList, myContainer):
        """Extract data from the input instance categories and map these data to the organization
        in the input table schema definition object.

        Data from contributing categories is merged using attributes specified in
        the merging index for the input table.

        Return a list of dictionaries with schema attribute Id keys containing data
        mapped from the input instance category.
        """
        #
        mD = {}
        for categoryName in categoryNameList:
            catObj = myContainer.getObj(categoryName)
            if catObj is None:
                continue

            attributeIndexDict = catObj.getAttributeIndexDict()
            schemaTableId = tObj.getId()
            schemaAttributeMapDict = tObj.getMapAttributeDict()
            schemaAttributeIdList = tObj.getAttributeIdList()
            nullValueDict = tObj.getSqlNullValueDict()
            maxWidthDict = tObj.getStringWidthDict()
            curAttributeIdList = tObj.getMapInstanceAttributeIdList(categoryName)
            #
            # dictionary of merging indices for each attribute in this category -
            #
            indL = tObj.getMapMergeIndexAttributes(categoryName)

            for row in catObj.getRowList():
                # initialize full table row --
                d = {}
                for atId in schemaAttributeIdList:
                    d[atId] = nullValueDict[atId]

                # assign merge index
                mK = []
                for atName in indL:
                    try:
                        mK.append(row[attributeIndexDict[atName]])
                    except:  # noqa: E722 pylint: disable=bare-except
                        # would reflect a serious issue of missing key-
                        if self.__debug:
                            traceback.print_exc(file=self.__lfh)

                for atId in curAttributeIdList:
                    try:
                        atName = schemaAttributeMapDict[atId]
                        val = row[attributeIndexDict[atName]]
                        maxW = maxWidthDict[atId]
                        if maxW > 0:
                            lenVal = len(val)
                            if lenVal > maxW:
                                self.__lfh.write("+ERROR - Table %s attribute %s length %d exceeds %d\n" % (schemaTableId, atId, lenVal, maxW))
                            d[atId] = val[:maxW] if ((val != "?") and (val != ".")) else nullValueDict[atId]
                        else:
                            d[atId] = val if ((val != "?") and (val != ".")) else nullValueDict[atId]
                    except:  # noqa: E722 pylint: disable=bare-except
                        # only for testing -
                        if self.__debug:
                            traceback.print_exc(file=self.__lfh)
                #
                # Update this row using exact matching of the merging key --
                # jdw  - will later add more complex comparisons
                #
                tk = tuple(mK)
                if tk not in mD:
                    mD[tk] = {}

                mD[tk].update(d)

        return mD.values()

    def delete(self, tableId, containerNameList=None, deleteOpt="all"):  # noqa: ARG002  pylint: disable=unused-argument
        #
        startTime = time.time()
        sqlCommandList = self.__getSqlDeleteList(tableId, containerNameList=None, deleteOpt=deleteOpt)

        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
        myQ.setWarning(self.__warningAction)
        ret = myQ.sqlCommand(sqlCommandList=sqlCommandList)
        #
        #
        endTime = time.time()
        if self.__verbose:
            self.__lfh.write("+SchemaDefLoader(delete) table %s server returns %r\n" % (tableId, ret))
            self.__lfh.write(
                "+SchemaDefLoader(delete) completed at %s (%.3f seconds)\n" % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )
            return ret
        if self.__verbose:
            self.__lfh.write("+SchemaDefLoader(delete) failse for %s\n" % tableId)
        return False

    def __getSqlDeleteList(self, tableId, containerNameList=None, deleteOpt="all"):
        """Return the SQL delete commands for the input table and container name list."""
        databaseName = self.__sD.getDatabaseName()
        sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)

        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()

        sqlDeleteList = []
        if deleteOpt in ["selected", "delete"] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=50)
        elif deleteOpt in ["all", "truncate"]:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]

        if self.__verbose:
            self.__lfh.write("+SchemaDefLoader(__getSqlDeleteList) delete SQL for %s : %r\n" % (tableId, sqlDeleteList))
        return sqlDeleteList

    def __batchFileImport(self, tableId, tableLoadPath, sqlFilePath=None, containerNameList=None, deleteOpt="all"):  # noqa: ARG002 pylint: disable=unused-argument
        """Batch load the input table using data in the input loadable data file.

        if sqlFilePath is provided then any generated SQL commands are preserved in this file.

        deleteOpt None|'selected'| 'all' or 'truncate'
        """
        startTime = time.time()
        databaseName = self.__sD.getDatabaseName()
        sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)

        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        # tableName = tableDefObj.getName()

        #
        if deleteOpt:
            sqlCommandList = self.__getSqlDeleteList(tableId, containerNameList=None, deleteOpt=deleteOpt)
        else:
            sqlCommandList = []

        if os.access(tableLoadPath, os.R_OK):
            tableDefObj = self.__sD.getTable(tableId)

            sqlCommandList.append(sqlGen.importTable(databaseName, tableDefObj, importPath=tableLoadPath))

            if self.__verbose:
                self.__lfh.write("+SchemaDefLoader(__batchFileImport) SQL import command\n%s\n" % sqlCommandList)
            #

        if sqlFilePath is not None:
            try:
                ofh = open(sqlFilePath, "w")
                ofh.write("%s" % "\n".join(sqlCommandList))
                ofh.close()
            except:  # noqa: E722 pylint: disable=bare-except
                pass
        #
        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
        myQ.setWarning(self.__warningAction)
        ret = myQ.sqlCommand(sqlCommandList=sqlCommandList)
        #
        #
        endTime = time.time()
        if self.__verbose:
            self.__lfh.write("+SchemaDefLoader(__batchFileImport) table %s server returns %r\n" % (tableId, ret))
            self.__lfh.write(
                "+SchemaDefLoader(__batchFileImport) completed at %s (%.3f seconds)\n"
                % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )
        return ret

    def loadBatchData(self, tableId, rowList=None, containerNameList=None, deleteOpt="selected"):
        return self.__batchInsertImport(tableId, rowList=rowList, containerNameList=containerNameList, deleteOpt=deleteOpt)

    def __batchInsertImport(self, tableId, rowList=None, containerNameList=None, deleteOpt="selected"):
        """Load the input table using bacth inserts of the input list of dictionaries (i.e. d[attributeId]=value).

        The containerNameList corresponding to the data within loadable data in rowList can be provided
        if 'selected' deletions are to performed prior to the the batch data inserts.

        deleteOpt = ['selected','all'] where 'selected' deletes rows corresponding to the input container
                    list before insert.   The 'all' options truncates the table prior to insert.

                    Deletions are performed in the absence of loadable data.

        """
        startTime = time.time()

        myQ = MyDbQuery(dbcon=self.__dbCon, verbose=self.__verbose, log=self.__lfh)
        myQ.setWarning(self.__warningAction)
        sqlGen = MyDbAdminSqlGen(self.__verbose, self.__lfh)
        #
        databaseName = self.__sD.getDatabaseName()
        tableDefObj = self.__sD.getTable(tableId)
        tableName = tableDefObj.getName()
        tableAttributeIdList = tableDefObj.getAttributeIdList()
        tableAttributeNameList = tableDefObj.getAttributeNameList()
        #
        sqlDeleteList = None
        if deleteOpt in ["selected", "delete"] and containerNameList is not None:
            deleteAttributeName = tableDefObj.getDeleteAttributeName()
            sqlDeleteList = sqlGen.deleteFromListSQL(databaseName, tableName, deleteAttributeName, containerNameList, chunkSize=10)
            if self.__verbose:
                self.__lfh.write("+SchemaDefLoader(batchInsertImport) delete SQL for %s : %r\n" % (tableId, sqlDeleteList))
        elif deleteOpt in ["all", "truncate"]:
            sqlDeleteList = [sqlGen.truncateTableSQL(databaseName, tableName)]

        sqlInsertList = []
        for row in rowList:
            vList = []
            aList = []
            for tid, nm in zip(tableAttributeIdList, tableAttributeNameList):
                if len(row[tid]) > 0 and row[tid] != r"\N":
                    vList.append(row[tid])
                    aList.append(nm)
            sqlInsertList.append((sqlGen.insertTemplateSQL(databaseName, tableName, aList), vList))

        ret = myQ.sqlBatchTemplateCommand(sqlInsertList, prependSqlList=sqlDeleteList)
        if self.__verbose:
            if ret:
                self.__lfh.write("+SchemaDefLoader(__batchInsertImport) batch insert completed for table %s rows %d\n" % (tableName, len(sqlInsertList)))
            else:
                self.__lfh.write("+SchemaDefLoader(__batchInsertImport) batch insert fails for table %s length %d\n" % (tableName, len(sqlInsertList)))

        endTime = time.time()
        if self.__verbose:
            self.__lfh.write(
                "+SchemaDefLoader(__batchInsertImport) completed at %s (%.3f seconds)\n"
                % (time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - startTime)
            )

        return ret


if __name__ == "__main__":
    pass
