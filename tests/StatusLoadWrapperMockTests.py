##
# File:    StatusLoadWrapperMockTests.py
# Date:    6-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for the status load wrapper (no database or site configuration required).
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"

import io
import os
import sys
import unittest
from unittest import mock

if __package__ is None or __package__ == "":  # noqa: PLC1901
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mock_import import mocksetup  # type: ignore  # noqa: F401 pylint: disable=unused-import,import-error
else:
    from .mock_import import mocksetup  # noqa: F401,TID252 pylint: disable=unused-import,import-error

from wwpdb.utils.db.StatusLoadWrapper import StatusLoadWrapper

MODEL_PATH = "/data/deposit/D_1000000001/D_1000000001_model_P1.cif.V3"


class StatusLoadWrapperMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__lfh = io.StringIO()
        pPatch = mock.patch("wwpdb.utils.db.StatusLoadWrapper.PathInfo")
        self.__mockPathInfo = pPatch.start()
        self.addCleanup(pPatch.stop)
        dPatch = mock.patch("wwpdb.utils.db.StatusLoadWrapper.DbLoadingApi")
        self.__mockDbLoadingApi = dPatch.start()
        self.addCleanup(dPatch.stop)
        self.__mockPathInfo.return_value.getModelPdbxFilePath.return_value = MODEL_PATH

    def testConstruction(self) -> None:
        StatusLoadWrapper(siteId="TEST_SITE", verbose=True, log=self.__lfh)
        self.__mockPathInfo.assert_called_once_with(siteId="TEST_SITE", sessionPath=".", verbose=True, log=self.__lfh)
        self.__mockDbLoadingApi.assert_not_called()

    def testLoadDefaults(self) -> None:
        """Successful load with default arguments"""
        self.__mockDbLoadingApi.return_value.doLoadStatus.return_value = True
        slw = StatusLoadWrapper(siteId="TEST_SITE", verbose=False, log=self.__lfh)
        self.assertTrue(slw.dbLoad("D_1000000001"))
        self.__mockPathInfo.return_value.getModelPdbxFilePath.assert_called_once_with(
            dataSetId="D_1000000001", fileSource="deposit", versionId="latest", mileStone="deposit"
        )
        self.__mockDbLoadingApi.assert_called_once_with(log=self.__lfh, verbose=False)
        self.__mockDbLoadingApi.return_value.doLoadStatus.assert_called_once_with(MODEL_PATH, "/data/deposit/D_1000000001")
        self.assertIn("site TEST_SITE loading data set D_1000000001 deposit deposit latest", self.__lfh.getvalue())

    def testLoadArguments(self) -> None:
        """Arguments are passed through and the loader status returned"""
        self.__mockDbLoadingApi.return_value.doLoadStatus.return_value = False
        slw = StatusLoadWrapper(siteId="TEST_SITE", verbose=True, log=self.__lfh)
        self.assertFalse(slw.dbLoad("D_1000000002", fileSource="archive", versionId="1", mileStone=None))
        self.__mockPathInfo.return_value.getModelPdbxFilePath.assert_called_once_with(
            dataSetId="D_1000000002", fileSource="archive", versionId="1", mileStone=None
        )
        self.__mockDbLoadingApi.return_value.doLoadStatus.assert_called_once()

    def testLoadExceptionVerbose(self) -> None:
        """An exception in the loader is reported and False returned"""
        self.__mockDbLoadingApi.return_value.doLoadStatus.side_effect = RuntimeError("load broke")
        slw = StatusLoadWrapper(siteId="TEST_SITE", verbose=True, log=self.__lfh)
        self.assertFalse(slw.dbLoad("D_1000000001"))
        out = self.__lfh.getvalue()
        self.assertIn("dbload failed for D_1000000001 load broke", out)
        self.assertIn("Traceback", out)

    def testLoadExceptionQuiet(self) -> None:
        """Failures are not detailed when not verbose"""
        self.__mockDbLoadingApi.side_effect = RuntimeError("no api")
        slw = StatusLoadWrapper(siteId="TEST_SITE", verbose=False, log=self.__lfh)
        self.assertFalse(slw.dbLoad("D_1000000001"))
        self.assertNotIn("dbload failed", self.__lfh.getvalue())

    def testLoadNoPath(self) -> None:
        """A missing model path is a failure"""
        self.__mockPathInfo.return_value.getModelPdbxFilePath.return_value = None
        slw = StatusLoadWrapper(siteId="TEST_SITE", verbose=True, log=self.__lfh)
        self.assertFalse(slw.dbLoad("D_1000000001"))
        self.__mockDbLoadingApi.return_value.doLoadStatus.assert_not_called()
        self.assertIn("dbload failed for D_1000000001", self.__lfh.getvalue())


if __name__ == "__main__":
    unittest.main()
