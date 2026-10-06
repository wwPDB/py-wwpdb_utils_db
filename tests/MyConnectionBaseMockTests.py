##
# File:    MyConnectionBaseMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for MyConnectionBase (no MySQL server or site configuration required).
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import unittest
from typing import Any, Dict, Optional
from unittest import mock

from wwpdb.utils.db.MyConnectionBase import MyConnectionBase

# Resource name -> {authentication key: site configuration key}
_RESOURCE_KEYS: Dict[str, Dict[str, str]] = {
    "PRD": {
        "DB_NAME": "SITE_REFDATA_PRD_DB_NAME",
        "DB_HOST": "SITE_REFDATA_DB_HOST_NAME",
        "DB_SOCKET": "SITE_REFDATA_DB_SOCKET",
        "DB_PORT": "SITE_REFDATA_DB_PORT_NUMBER",
        "DB_USER": "SITE_REFDATA_DB_USER_NAME",
        "DB_PW": "SITE_REFDATA_DB_PASSWORD",
    },
    "CC": {
        "DB_NAME": "SITE_REFDATA_CC_DB_NAME",
        "DB_HOST": "SITE_REFDATA_DB_HOST_NAME",
        "DB_SOCKET": "SITE_REFDATA_DB_SOCKET",
        "DB_PORT": "SITE_REFDATA_DB_PORT_NUMBER",
        "DB_USER": "SITE_REFDATA_DB_USER_NAME",
        "DB_PW": "SITE_REFDATA_DB_PASSWORD",
    },
    "RCSB_INSTANCE": {
        "DB_NAME": "SITE_INSTANCE_DB_NAME",
        "DB_HOST": "SITE_INSTANCE_DB_HOST_NAME",
        "DB_SOCKET": "SITE_INSTANCE_DB_SOCKET",
        "DB_PORT": "SITE_INSTANCE_DB_PORT_NUMBER",
        "DB_USER": "SITE_INSTANCE_DB_USER_NAME",
        "DB_PW": "SITE_INSTANCE_DB_PASSWORD",
    },
    "DA_INTERNAL": {
        "DB_NAME": "SITE_DA_INTERNAL_DB_NAME",
        "DB_HOST": "SITE_DA_INTERNAL_DB_HOST_NAME",
        "DB_SOCKET": "SITE_DA_INTERNAL_DB_SOCKET",
        "DB_PORT": "SITE_DA_INTERNAL_DB_PORT_NUMBER",
        "DB_USER": "SITE_DA_INTERNAL_DB_USER_NAME",
        "DB_PW": "SITE_DA_INTERNAL_DB_PASSWORD",
    },
    "DA_INTERNAL_COMBINE": {
        "DB_NAME": "SITE_DA_INTERNAL_COMBINE_DB_NAME",
        "DB_HOST": "SITE_DA_INTERNAL_COMBINE_DB_HOST_NAME",
        "DB_SOCKET": "SITE_DA_INTERNAL_COMBINE_DB_SOCKET",
        "DB_PORT": "SITE_DA_INTERNAL_COMBINE_DB_PORT_NUMBER",
        "DB_USER": "SITE_DA_INTERNAL_COMBINE_DB_USER_NAME",
        "DB_PW": "SITE_DA_INTERNAL_COMBINE_DB_PASSWORD",
    },
    "DISTRO": {
        "DB_NAME": "SITE_DISTRO_DB_NAME",
        "DB_HOST": "SITE_DISTRO_DB_HOST_NAME",
        "DB_SOCKET": "SITE_DISTRO_DB_SOCKET",
        "DB_PORT": "SITE_DISTRO_DB_PORT_NUMBER",
        "DB_USER": "SITE_DISTRO_DB_USER_NAME",
        "DB_PW": "SITE_DISTRO_DB_PASSWORD",
    },
    "STATUS": {
        "DB_NAME": "SITE_DB_DATABASE_NAME",
        "DB_HOST": "SITE_DB_HOST_NAME",
        "DB_SOCKET": "SITE_DB_SOCKET",
        "DB_PORT": "SITE_DB_PORT_NUMBER",
        "DB_USER": "SITE_DB_USER_NAME",
        "DB_PW": "SITE_DB_PASSWORD",
    },
    "MESSAGE": {
        "DB_NAME": "SITE_MESSAGE_DB_DATABASE_NAME",
        "DB_HOST": "SITE_MESSAGE_DB_HOST_NAME",
        "DB_SOCKET": "SITE_MESSAGE_DB_SOCKET",
        "DB_PORT": "SITE_MESSAGE_DB_PORT_NUMBER",
        "DB_USER": "SITE_MESSAGE_DB_USER_NAME",
        "DB_PW": "SITE_MESSAGE_DB_PASSWORD",
    },
}


class MyConnectionBaseMockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.__config: Dict[str, Any] = {}
        self.__ciP = mock.patch("wwpdb.utils.db.MyConnectionBase.ConfigInfo")
        mockCI = self.__ciP.start()
        mockCI.return_value.get.side_effect = self.__configGet
        self.__mockConfigInfo = mockCI
        self.__gcP = mock.patch("wwpdb.utils.db.MyConnectionBase.getConnection")
        self.__mockGetConnection = self.__gcP.start()
        self.__dbCon = mock.MagicMock(name="dbcon")
        self.__mockGetConnection.return_value = self.__dbCon

    def tearDown(self) -> None:
        self.__gcP.stop()
        self.__ciP.stop()

    def __configGet(self, key: str, default: Any = None) -> Any:
        return self.__config.get(key, default)

    def __populate(self, resourceName: str, port: Optional[str] = "3310", socket: Optional[str] = "/tmp/mysql.sock") -> Dict[str, Any]:
        """Populate the mock site configuration for the resource and return the expected values"""
        keys = _RESOURCE_KEYS[resourceName]
        expected: Dict[str, Any] = {
            "DB_NAME": "name_%s" % resourceName,
            "DB_HOST": "host_%s" % resourceName,
            "DB_SOCKET": socket,
            "DB_PORT": port,
            "DB_USER": "user_%s" % resourceName,
            "DB_PW": "pw_%s" % resourceName,
        }
        self.__config = {keys[k]: v for k, v in expected.items()}
        return expected

    def testConfigInfoUsesSiteId(self) -> None:
        """The site configuration is looked up for the input site id"""
        MyConnectionBase(siteId="WWPDB_DEPLOY_TEST")
        self.__mockConfigInfo.assert_called_once_with("WWPDB_DEPLOY_TEST")

    def testSetResourceAll(self) -> None:
        """Each supported resource reads its own configuration keys"""
        for resourceName in _RESOURCE_KEYS:
            if resourceName == "MESSAGE":
                # See testSetResourceMessagePassword
                continue
            with self.subTest(resource=resourceName):
                expected = self.__populate(resourceName)
                mcb = MyConnectionBase(siteId="TESTSITE")
                with self.assertLogs("wwpdb.utils.db.MyConnectionBase", level="INFO") as cm:
                    mcb.setResource(resourceName)
                authD = mcb.getAuth()
                self.assertEqual(authD["DB_NAME"], expected["DB_NAME"])
                self.assertEqual(authD["DB_HOST"], expected["DB_HOST"])
                self.assertEqual(authD["DB_USER"], expected["DB_USER"])
                self.assertEqual(authD["DB_PW"], expected["DB_PW"])
                self.assertEqual(authD["DB_SOCKET"], "/tmp/mysql.sock")
                self.assertEqual(authD["DB_PORT"], 3310)
                self.assertIsInstance(authD["DB_PORT"], int)
                self.assertEqual(authD["DB_SERVER"], "mysql")
                self.assertIn("resource name %s" % resourceName, cm.output[0])

    def testSetResourceMessage(self) -> None:
        """MESSAGE resource database, host, user, socket and port settings"""
        expected = self.__populate("MESSAGE")
        mcb = MyConnectionBase()
        mcb.setResource("MESSAGE")
        authD = mcb.getAuth()
        for ky in ["DB_NAME", "DB_HOST", "DB_USER"]:
            self.assertEqual(authD[ky], expected[ky])
        self.assertEqual(authD["DB_PORT"], 3310)

    # Bug: MyConnectionBase.setResource("MESSAGE") reads the password from "SITE__MESSAGE_DB_PASSWORD" (double underscore)
    # whereas every other resource uses SITE_<RESOURCE>_DB_PASSWORD.
    @unittest.expectedFailure
    def testSetResourceMessagePassword(self) -> None:
        """MESSAGE resource password is read from SITE_MESSAGE_DB_PASSWORD"""
        expected = self.__populate("MESSAGE")
        mcb = MyConnectionBase()
        mcb.setResource("MESSAGE")
        self.assertEqual(mcb.getAuth()["DB_PW"], expected["DB_PW"])

    def testSetResourceDefaults(self) -> None:
        """Missing port defaults to 3306 and missing/short sockets are dropped"""
        for socket in [None, "", "x"]:
            with self.subTest(socket=socket):
                self.__populate("STATUS", port=None, socket=socket)
                mcb = MyConnectionBase()
                mcb.setResource("STATUS")
                authD = mcb.getAuth()
                self.assertEqual(authD["DB_PORT"], 3306)
                self.assertIsNone(authD["DB_SOCKET"])
                self.assertEqual(authD["DB_NAME"], "name_STATUS")

    def testSetResourceUnknown(self) -> None:
        """An unknown resource leaves the connection details unset with default port and server"""
        self.__populate("STATUS")
        mcb = MyConnectionBase()
        mcb.setResource("NOT_A_RESOURCE")
        authD = mcb.getAuth()
        self.assertIsNone(authD["DB_NAME"])
        self.assertIsNone(authD["DB_HOST"])
        self.assertIsNone(authD["DB_USER"])
        self.assertIsNone(authD["DB_PW"])
        self.assertIsNone(authD["DB_SOCKET"])
        self.assertEqual(authD["DB_PORT"], 3306)
        self.assertEqual(authD["DB_SERVER"], "mysql")

    def testGetAuthInitiallyEmpty(self) -> None:
        mcb = MyConnectionBase()
        self.assertEqual(mcb.getAuth(), {})

    def testOpenConnectionAfterSetResource(self) -> None:
        """openConnection() requests a pooled connection with the resource connection arguments"""
        self.__populate("DA_INTERNAL")
        mcb = MyConnectionBase()
        mcb.setResource("DA_INTERNAL")
        self.assertTrue(mcb.openConnection())
        self.__mockGetConnection.assert_called_once_with(
            {
                "db": "name_DA_INTERNAL",
                "user": "user_DA_INTERNAL",
                "passwd": "pw_DA_INTERNAL",
                "host": "host_DA_INTERNAL",
                "port": 3310,
                "local_infile": 1,
                "unix_socket": "/tmp/mysql.sock",
            }
        )
        self.assertIs(mcb.getConnection(), self.__dbCon)

    def testOpenConnectionWithoutSocket(self) -> None:
        """No unix_socket argument is passed when no socket is configured"""
        self.__populate("STATUS", socket=None)
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        self.assertTrue(mcb.openConnection())
        connectKw = self.__mockGetConnection.call_args[0][0]
        self.assertNotIn("unix_socket", connectKw)
        self.assertEqual(connectKw["port"], 3310)

    def testSetAuth(self) -> None:
        """setAuth() supplies the connection details used by openConnection()"""
        authD = {"DB_NAME": "db1", "DB_HOST": "h1", "DB_USER": "u1", "DB_PW": "p1", "DB_SOCKET": None, "DB_PORT": "3399", "DB_SERVER": "mysql"}
        mcb = MyConnectionBase()
        mcb.setAuth(authD)
        self.assertIs(mcb.getAuth(), authD)
        self.assertTrue(mcb.openConnection())
        self.__mockGetConnection.assert_called_once_with({"db": "db1", "user": "u1", "passwd": "p1", "host": "h1", "port": 3399, "local_infile": 1})

    def testSetAuthDefaultPort(self) -> None:
        """setAuth() without DB_PORT uses port 3306"""
        authD = {"DB_NAME": "db1", "DB_HOST": "h1", "DB_USER": "u1", "DB_PW": "p1", "DB_SOCKET": "/var/sock", "DB_SERVER": "mysql"}
        mcb = MyConnectionBase()
        mcb.setAuth(authD)
        self.assertTrue(mcb.openConnection())
        connectKw = self.__mockGetConnection.call_args[0][0]
        self.assertEqual(connectKw["port"], 3306)
        self.assertEqual(connectKw["unix_socket"], "/var/sock")

    def testSetAuthIncomplete(self) -> None:
        """An incomplete authentication dictionary is accepted silently - values present before the missing key are applied"""
        mcb = MyConnectionBase()
        mcb.setAuth({"DB_NAME": "db2", "DB_HOST": "h2"})
        self.assertEqual(mcb.getAuth(), {"DB_NAME": "db2", "DB_HOST": "h2"})
        self.assertTrue(mcb.openConnection())
        connectKw = self.__mockGetConnection.call_args[0][0]
        self.assertEqual(connectKw["db"], "db2")
        self.assertEqual(connectKw["host"], "h2")
        self.assertEqual(connectKw["user"], "None")
        self.assertEqual(connectKw["port"], 3306)

    def testOpenConnectionFailure(self) -> None:
        """A connection failure is logged and openConnection() returns False"""
        self.__populate("STATUS")
        self.__mockGetConnection.side_effect = RuntimeError("cannot connect")
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        with self.assertLogs("wwpdb.utils.db.MyConnectionBase", level="ERROR") as cm:
            self.assertFalse(mcb.openConnection())
        self.assertIn("Connection error", cm.output[0])
        self.assertIsNone(mcb.getConnection())
        self.assertFalse(mcb.closeConnection())

    def testOpenConnectionClosesExisting(self) -> None:
        """Opening a second connection closes the first one"""
        first = mock.MagicMock(name="first")
        second = mock.MagicMock(name="second")
        self.__mockGetConnection.side_effect = [first, second]
        self.__populate("STATUS")
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        self.assertTrue(mcb.openConnection())
        with self.assertLogs("wwpdb.utils.db.MyConnectionBase", level="INFO") as cm:
            self.assertTrue(mcb.openConnection())
        self.assertTrue(any("Closing an existing connection" in msg for msg in cm.output))
        first.close.assert_called_once_with()
        second.close.assert_not_called()
        self.assertIs(mcb.getConnection(), second)

    def testCloseConnection(self) -> None:
        self.__populate("STATUS")
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        self.assertFalse(mcb.closeConnection())
        self.assertTrue(mcb.openConnection())
        self.assertTrue(mcb.closeConnection())
        self.__dbCon.close.assert_called_once_with()
        self.assertIsNone(mcb.getConnection())
        self.assertFalse(mcb.closeConnection())

    def testGetCursor(self) -> None:
        self.__populate("STATUS")
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        self.assertTrue(mcb.openConnection())
        self.assertIs(mcb.getCursor(), self.__dbCon.cursor.return_value)

    def testGetCursorNoConnection(self) -> None:
        """getCursor() without an open connection logs the failure and returns None"""
        mcb = MyConnectionBase()
        with self.assertLogs("wwpdb.utils.db.MyConnectionBase", level="ERROR") as cm:
            self.assertIsNone(mcb.getCursor())
        self.assertIn("getCursor", cm.output[0])

    def testGetCursorError(self) -> None:
        """An exception raised creating a cursor is logged and None is returned"""
        self.__populate("STATUS")
        self.__dbCon.cursor.side_effect = RuntimeError("bad cursor")
        mcb = MyConnectionBase()
        mcb.setResource("STATUS")
        self.assertTrue(mcb.openConnection())
        with self.assertLogs("wwpdb.utils.db.MyConnectionBase", level="ERROR"):
            self.assertIsNone(mcb.getCursor())


if __name__ == "__main__":
    unittest.main()
