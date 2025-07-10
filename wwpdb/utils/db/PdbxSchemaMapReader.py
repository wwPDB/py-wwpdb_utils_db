##
# File:    PdbxSchamaMapReader.py
# Author:  J. Westbrook
# Date:    4-Jan-2013
#
# Updates:
#  7-Jan-2013 jdw Revised organization of instance data item mapping to attributes.
#  9-Jan-2013 jdw revise handling of mege index.
# 11=Jan-2013 jdw add table and attribute abbreviation support.
# 13-Jan-2013 jdw add test for mysql  index size maximum
##
"""
Example reader for RCSB schema map data files exporting the data structure used by the
wwpdb.utils.db.SchemaMapDef class hierarchy.

"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import sys
import traceback

# from mmcif.api.PdbxContainers import *
from mmcif.api.PdbxContainers import CifName
from mmcif.io.PdbxReader import PdbxReader


class PdbxSchemaMapReader:
    def __init__(self, verbose=True, log=sys.stderr):
        self.__lfh = log
        self.__verbose = verbose
        self.__tableNameList = []
        self.__atDefList = []
        self.__atMapList = []
        self.__tableAbbrev = {}
        self.__attribAbbrev = {}

    def read(self, schemaMapFile):
        self.__tableNameList, self.__atDefList, self.__atMapList, self.__tableAbbrev, self.__attribAbbrev = self.__readSchemaMap(schemaMapFile)
        # self.dump(self.__lfh)
        return True

    def dump(self, ofh):
        ofh.write("Table name list: %s\n" % self.__tableNameList)
        for d in self.__atDefList:
            ofh.write("\n\nAttribute def: %s\n" % d.items())

        for d in self.__atMapList:
            ofh.write("\n\nAttribute map: %s\n" % d.items())

        for k, v in self.__tableAbbrev.items():
            ofh.write("Table %s - abbreviation %s\n" % (k, v))

        for tN, d in self.__attribAbbrev.items():
            for k, v in d.items():
                ofh.write("Table %s - attribute %s  abbreviation %s\n" % (tN, k, v))

    def __convertDataType(self, dtype, width=0, precision=0):  # noqa: ARG002 pylint:  disable=unused-argument
        if dtype.lower() in ["char", "varchar", "text"]:
            if width < 65000:
                retType = "VARCHAR"
            else:
                retType = "TEXT"
        elif dtype.lower() == "int":
            retType = "INT"
        elif dtype.lower() == "float":
            retType = "FLOAT"
        elif dtype.lower() == "date" or dtype.lower() == "datetime":
            retType = dtype.upper()
        else:
            retType = None
            self.__lfh.write("+ERROR - UNKNOWN DATA TYPE %s\n" % dtype)

        return retType

    def __toBool(self, flag):
        if flag.lower() == "y" or flag.lower() == "yes" or flag == "1":
            return True
        return False

    def __getTableAbbrev(self, tableName):
        if tableName in self.__tableAbbrev:
            return self.__tableAbbrev[tableName]
        return tableName

    def __getAttributeAbbrev(self, tableName, attributeName):
        try:
            return self.__attribAbbrev[tableName][attributeName]
        except:  # noqa: E722  pylint: disable=bare-except
            return attributeName

    def makeSchemaDef(self):
        sD = {}
        for tableName in self.__tableNameList:
            if tableName in ["rcsb_columninfo", "columninfo", "tableinfo", "rcsb_tableinfo"]:
                continue
            d = {}
            tableAbbrev = self.__getTableAbbrev(tableName)
            tU = tableAbbrev.upper()
            d["TABLE_ID"] = tU
            d["TABLE_NAME"] = tableAbbrev
            d["TABLE_TYPE"] = "transactional"
            d["ATTRIBUTES"] = {}
            d["ATTRIBUTE_INFO"] = {}
            d["ATTRIBUTE_MAP"] = {}
            #
            # create a sub list for this table -
            infoL = []
            for atD in self.__atDefList:
                if atD["table_name"] == tableName:
                    infoL.append(atD)
            #
            mapD = {}
            for atD in self.__atMapList:
                if atD["target_table_name"] == tableName:
                    attributeName = atD["target_attribute_name"]
                    attributeAbbrev = self.__getAttributeAbbrev(tableName, attributeName)
                    atU = attributeAbbrev.upper()
                    itN = atD["source_item_name"] if atD["source_item_name"] not in ["?", "."] else None
                    if itN is not None:
                        catNameM = CifName.categoryPart(itN)
                        attNameM = CifName.attributePart(itN)
                    else:
                        catNameM = None
                        attNameM = None

                    # cId = atD['condition_id'] if atD['condition_id'] not in ['?', '.'] else None
                    fId = atD["function_id"] if atD["function_id"] not in ["?", "."] else None
                    if fId is not None and catNameM is None:
                        mapD[atU] = (catNameM, attNameM, fId, None)
                    else:
                        mapD[atU] = (catNameM, attNameM, fId, None)

            #
            indexList = []
            for ii, atD in enumerate(infoL):
                attributeName = atD["attribute_name"]
                attributeAbbrev = self.__getAttributeAbbrev(tableName, attributeName)
                atU = attributeAbbrev.upper()
                #
                td = {}
                # 'data_type','index_flag','null_flag','width','precision','populated'
                td["SQL_TYPE"] = self.__convertDataType(atD["data_type"], width=int(atD["width"]))
                td["WIDTH"] = int(atD["width"])
                td["PRECISION"] = int(atD["precision"])
                td["NULLABLE"] = not self.__toBool(atD["null_flag"])
                td["PRIMARY_KEY"] = self.__toBool(atD["index_flag"])
                td["ORDER"] = ii + 1
                if td["PRIMARY_KEY"]:
                    indexList.append(atU)
                d["ATTRIBUTES"][atU] = attributeAbbrev
                d["ATTRIBUTE_INFO"][atU] = td
                d["ATTRIBUTE_MAP"][atU] = mapD[atU]

            #
            if self.__verbose and len(indexList) > 16:
                self.__lfh.write("+WARNING - %s index list exceeds max length %d\n" % (tableName, len(indexList)))
            mergeDict = {}
            deleteAttributeList = []
            for atU in indexList:
                tN = d["ATTRIBUTE_MAP"][atU][0]
                aN = d["ATTRIBUTE_MAP"][atU][1]
                fN = d["ATTRIBUTE_MAP"][atU][2]
                if aN is not None:
                    if tN not in mergeDict:
                        mergeDict[tN] = []
                    mergeDict[tN].append(aN)
                #
                # Using RCSB convention of including one attribute in each table corresponding to the datablockId()
                #   this attributeId is used a key pre-insert deletions.
                #
                if fN == "datablockid()":
                    deleteAttributeList.append(atU)
            #
            # Assign a merge index to this instance category
            #
            for k, v in mergeDict.items():
                d["MAP_MERGE_INDICES"] = {k: {"TYPE": "EQUI-JOIN", "ATTRIBUTES": tuple(v)}}

            if len(deleteAttributeList) > 0:
                d["TABLE_DELETE_ATTRIBUTE"] = deleteAttributeList[0]
                d["INDICES"] = {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": tuple(indexList)}, "s1": {"TYPE": "SEARCH", "ATTRIBUTES": tuple(deleteAttributeList)}}
            else:
                d["INDICES"] = {"p1": {"TYPE": "UNIQUE", "ATTRIBUTES": tuple(indexList)}}
                if self.__verbose:
                    self.__lfh.write("+WARNING - No delete attribute for table %s\n" % tableName)

            if self.__verbose and len(mergeDict) < 1:
                self.__lfh.write("+WARNING - No merge index possible for table %s\n" % tableName)

            sD[tU] = d
        return sD

    def __readSchemaMap(self, schemaMapFile):
        """Read RCSB schema map file and return the list of table names, attribute definitions,
        attribute mapping, table and attribute abbreviations.
        """
        tableNameList = []
        atDefList = []
        atMapList = []
        tableAbbrevD = {}
        attribAbbrevD = {}
        try:
            #
            myContainerList = []
            ifh = open(schemaMapFile)
            pRd = PdbxReader(ifh)
            pRd.read(myContainerList)
            ifh.close()
            #
            for myContainer in myContainerList:
                cN = str(myContainer.getName()).lower()
                #
                # read schema details --
                #
                if cN == "rcsb_schema":
                    #
                    catObj = myContainer.getObj("rcsb_table")
                    if catObj is not None:
                        i1 = catObj.getAttributeIndex("table_name")
                        for row in catObj.getRowList():
                            tableNameList.append(row[i1])
                    #
                    catObj = myContainer.getObj("rcsb_attribute_def")
                    atList = ["table_name", "attribute_name", "data_type", "index_flag", "null_flag", "width", "precision", "populated"]
                    indList = []
                    if catObj is not None:
                        for at in atList:
                            indList.append(catObj.getAttributeIndex(at))
                        for row in catObj.getRowList():
                            d = {}
                            for ii, at in enumerate(atList):
                                d[at] = row[indList[ii]]
                            atDefList.append(d)
                    #
                    # _rcsb_table_abbrev.table_name
                    # _rcsb_table_abbrev.table_abbrev
                    #
                    catObj = myContainer.getObj("rcsb_table_abbrev")
                    if catObj is not None:
                        i1 = catObj.getAttributeIndex("table_name")
                        i2 = catObj.getAttributeIndex("table_abbrev")
                        for row in catObj.getRowList():
                            tableAbbrevD[row[i1]] = row[i2]
                    #
                    # _rcsb_attribute_abbrev.table_name
                    # _rcsb_attribute_abbrev.attribute_name
                    # _rcsb_attribute_abbrev.attribute_abbrev

                    catObj = myContainer.getObj("rcsb_attribute_abbrev")
                    if catObj is not None:
                        i1 = catObj.getAttributeIndex("table_name")
                        i2 = catObj.getAttributeIndex("attribute_name")
                        i3 = catObj.getAttributeIndex("attribute_abbrev")
                        for row in catObj.getRowList():
                            if row[i1] not in attribAbbrevD:
                                attribAbbrevD[row[i1]] = {}
                            attribAbbrevD[row[i1]][row[i2]] = row[i3]

                # read attribute mapping details --
                #
                elif cN == "rcsb_schema_map":
                    catObj = myContainer.getObj("rcsb_attribute_map")
                    atList = ["target_table_name", "target_attribute_name", "source_item_name", "condition_id", "function_id"]
                    indList = []
                    if catObj is not None:
                        for at in atList:
                            indList.append(catObj.getAttributeIndex(at))
                        for row in catObj.getRowList():
                            d = {}
                            for ii, at in enumerate(atList):
                                d[at] = row[indList[ii]]
                            atMapList.append(d)

                else:
                    self.__lfh.write("+ERROR -unanticipated data container %s\n" % cN)

        except Exception as e:  # noqa: BLE001
            self.__lfh.write("+ERROR - error processing schema map file %s - %s\n" % (schemaMapFile, str(e)))
            traceback.print_exc(file=self.__lfh)

        return tableNameList, atDefList, atMapList, tableAbbrevD, attribAbbrevD


if __name__ == "__main__":
    pass
