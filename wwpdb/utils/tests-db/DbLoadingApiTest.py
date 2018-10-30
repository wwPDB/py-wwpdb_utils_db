"""

File:    DbLoadingApiTest.py

     Some test cases ..

"""
import os,sys
from wwpdb.utils.db.DbLoadingApi import DbLoadingApi

if __name__ == '__mainold__':
    
    depId = "D_1000000020"
    sessionDir = "/net/techusers/lchen/sources/da-app/wwpdb/api/status/dbapi"
    #mappingFile path by default in the same location with other mapping files
    mappingFile = "em_admin.cif"
    dbName = "status"
    t = DbLoadingApi(log=sys.stderr, verbose=True)
    #t.doDataLoadingBcp(depId,sessionDir)
    t.doDataLoadingByMapping(depId,sessionDir,mappingFile,dbName)        
            
