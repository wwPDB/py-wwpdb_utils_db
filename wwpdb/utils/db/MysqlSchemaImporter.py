##
# File: MysqlSchemaImporter.py
# Date: 20-May-2015 jdw
#
# Create a skeleton schema map definition data structure from mysql
# table descriptions.
#
import os
import pprint
import sys


class MysqlSchemaImporter:
    def __init__(self, dbUser, dbPw, dbHost, mysqlPath="/opt/local/bin/mysql", verbose=True, log=sys.stderr):  # noqa: ARG002 pylint:  disable=unused-argument
        self.__lfh = log
        self.__mysqlPath = mysqlPath
        self.__dbUser = dbUser
        self.__dbPw = dbPw
        self.__dbHost = dbHost

    def __import(self, filePath):
        colDataList = []
        ifh = open(filePath)
        for line in ifh:
            if line is not None and len(line) > 0:
                fields = str(line[:-1]).split("\t")
                if len(fields) != 6:
                    self.__lfh.write("bad line in %s  = %s" % (filePath, line))
                    continue
                colDataList.append(fields)
        ifh.close()
        os.remove(filePath)
        #
        return colDataList[1:]

    def __export(self, filePath, db, tableName):
        cmdDetail = ' --user=%s --password=%s --host=%s %s -e "describe %s;" ' % (self.__dbUser, self.__dbPw, self.__dbHost, db, tableName)
        cmd = self.__mysqlPath + cmdDetail + " > %s" % filePath
        return os.system(cmd)  # noqa: S605

    def __buildDef(self, dbName, tableName, colDataList):  # noqa: ARG002 pylint: disable=unused-argument
        defD = {}
        tableId = str(tableName).upper()
        attIdKeyList = []
        attMap = {}
        indD = {}
        attInfo = {}
        attD = {}
        for ii, ff in enumerate(colDataList, start=1):
            attName = str(ff[0])
            attId = str(attName).upper()
            nullFlag = bool(ff[2] == "YES")
            impType = ff[1]
            if "(" in impType:
                width = impType[impType.find("(") + 1 : -1]
                sqlType = impType[: impType.find("(")]
            else:
                width = 10
                sqlType = impType
            precision = 0
            if ff[3] in ["MUL", "PRI"]:
                attIdKeyList.append(attId)
                keyFlag = True
            else:
                keyFlag = False
            attD[attId] = attName
            attMap[attId] = (tableName, attName, None, None)
            attInfo[attId] = {"NULLABLE": nullFlag, "ORDER": ii, "PRECISION": precision, "PRIMARY_KEY": keyFlag, "SQL_TYPE": sqlType.upper(), "WIDTH": width}
        #
        d = {}
        d["ATTRIBUTES"] = tuple(attIdKeyList)
        d["TYPE"] = "UNIQUE"
        indD["p1"] = d
        defD["INDICES"] = indD
        defD["ATTRIBUTES"] = attD
        defD["ATTRIBUTE_INFO"] = attInfo
        defD["ATTRIBUTE_MAP"] = attMap
        #
        defD["TABLE_DELETE_ATTRIBUTE"] = attIdKeyList[0]
        defD["TABLE_ID"] = tableId
        defD["TABLE_NAME"] = tableName
        defD["TABLE_TYPE"] = "transactional"
        # 'MAP_MERGE_INDICES': {'valence_ref': {'ATTRIBUTES': ('id',), 'TYPE': 'EQUI-JOIN'}},
        tD = {}
        tD["ATTRIBUTES"] = tuple(attIdKeyList)
        tD["TYPE"] = "EQUI-JOIN"
        defD["MAP_MERGE_INDICES"] = {}
        defD["MAP_MERGE_INDICES"][tableName] = tD
        #
        return tableId, defD

    def create(self, dbName, tableNameList):
        schemaDef = {}
        for tableName in tableNameList:
            fn = "mysql-schema-" + tableName + ".txt"
            self.__export(fn, dbName, tableName)
            colDataList = self.__import(fn)
            if colDataList and len(colDataList) > 0:
                self.__lfh.write("tableName %s length %d\n" % (tableName, len(colDataList)))
                tableId, defD = self.__buildDef(dbName, tableName, colDataList)
                schemaDef[tableId] = defD
        #
        pprint.pprint(schemaDef, stream=sys.stdout, width=120, indent=3)  # noqa: T203


def __main():
    tableNameList = [
        "PDB_status_information",
        "audit_author",
        "chem_comp",
        "citation",
        "citation_author",
        "deposition_from_09",
        "deposition_from_2",
        "entity",
        "entity_poly",
        "pdb_entry",
        "pdbx_contact_author",
        "pdbx_database_PDB_obs_spr",
        "pdbx_database_status_history",
        "pdbx_depui_entry_details",
        "pdbx_molecule",
        "pdbx_molecule_features",
        "pdbx_prerelease_seq",
        "processing_status",
        "rcsb_status",
        "rcsb_status_t",
        "struct",
        "pdbx_database_status_history",
    ]

    dbUser = os.getenv("MYSQL_DB_USER")
    dbPw = os.getenv("MYSQL_SBKB_PW")
    dbHost = "localhost"
    msi = MysqlSchemaImporter(dbUser, dbPw, dbHost, mysqlPath="/opt/local/bin/mysql", verbose=True, log=sys.stderr)
    msi.create("da_internal", tableNameList)


if __name__ == "__main__":
    __main()
