"""

File:    DbLoaderImportTest.py

     Some test cases ..

"""

import sys
import unittest

from wwpdb.utils.db.DbLoadingApi import DbLoadingApi


class DBLoaderTests(unittest.TestCase):
    def setUp(self):
        pass

    def testDbLoaderImport(self):
        """Test case -  noop - as cannot instantiate"""
        # pylint: disable=unnecessary-pass


if __name__ == "__mainold__":  # pragma: no cover
    depId = "D_1000000020"
    sessionDir = "/net/techusers/lchen/sources/da-app/wwpdb/api/status/dbapi"
    # mappingFile full path
    mappingFile = "XXXem_admin.cif"
    dbName = "status"
    t = DbLoadingApi(log=sys.stderr, verbose=True)
    # t.doDataLoadingBcp(depId, sessionDir)
    t.doDataLoadingByMapping(depId, sessionDir, mappingFile, dbName)
