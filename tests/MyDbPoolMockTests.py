##
# File:    MyDbPoolMockTests.py
# Date:    06-Oct-2026
#
# Updates:
#
##
"""
Mock based test cases for the shared SQLAlchemy engine connection pooling in MyDbPool  (no MySQL server required).
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Creative Commons Attribution 3.0 Unported"
__version__ = "V0.01"

import threading
import time
import unittest
from typing import Any, Dict, List
from unittest import mock

from wwpdb.utils.db.MyDbPool import disposeAll, getConnection, getEngine


class MyDbPoolMockTests(unittest.TestCase):
    def setUp(self) -> None:
        # Start each test with an empty engine cache
        disposeAll()
        self.__ceP = mock.patch("wwpdb.utils.db.MyDbPool.create_engine", side_effect=self.__makeEngine)
        self.__mockCreateEngine = self.__ceP.start()
        self.__engines: List[mock.MagicMock] = []

    def tearDown(self) -> None:
        disposeAll()
        self.__ceP.stop()

    def __makeEngine(self, *args: Any, **kwargs: Any) -> mock.MagicMock:  # noqa: ARG002 pylint: disable=unused-argument
        eng = mock.MagicMock(name="engine%d" % len(self.__engines))
        self.__engines.append(eng)
        return eng

    def testGetEngineCreatesEngineWithPoolOptions(self) -> None:
        """A new engine is created for the mysqldb dialect with a creator and the shared pool options"""
        kw: Dict[str, Any] = {"db": "testdb", "user": "u", "passwd": "p", "host": "h", "port": 3306}
        eng = getEngine(kw)
        self.assertIs(eng, self.__engines[0])
        self.assertEqual(self.__mockCreateEngine.call_count, 1)
        args, kwargs = self.__mockCreateEngine.call_args
        self.assertEqual(args, ("mysql+mysqldb://",))
        self.assertTrue(callable(kwargs["creator"]))
        self.assertEqual(kwargs["pool_size"], 12)
        self.assertEqual(kwargs["max_overflow"], 12)
        self.assertEqual(kwargs["pool_timeout"], 30)
        self.assertEqual(kwargs["pool_recycle"], 1800)
        self.assertTrue(kwargs["echo_pool"])

    def testGetEngineIsCachedIndependentOfKeyOrder(self) -> None:
        """The same connection arguments (in any order) share one engine"""
        e1 = getEngine({"db": "a", "user": "u", "port": 3306})
        e2 = getEngine({"port": 3306, "user": "u", "db": "a"})
        self.assertIs(e1, e2)
        self.assertEqual(self.__mockCreateEngine.call_count, 1)

    def testGetEngineDistinctArguments(self) -> None:
        """Different connection arguments get different engines"""
        e1 = getEngine({"db": "a"})
        e2 = getEngine({"db": "b"})
        e3 = getEngine({"db": "a", "unix_socket": "/tmp/sock"})
        self.assertIsNot(e1, e2)
        self.assertIsNot(e1, e3)
        self.assertIsNot(e2, e3)
        self.assertEqual(self.__mockCreateEngine.call_count, 3)
        # Returning to a previously used argument set reuses its engine
        self.assertIs(getEngine({"db": "b"}), e2)
        self.assertEqual(self.__mockCreateEngine.call_count, 3)

    def testCreatorCallsMySQLdbConnectWithCopyOfArguments(self) -> None:
        """The engine creator opens a MySQLdb connection with the arguments supplied when the engine was made"""
        kw: Dict[str, Any] = {"db": "testdb", "user": "u", "passwd": "p", "host": "h", "port": 3307, "local_infile": 1}
        getEngine(kw)
        creator = self.__mockCreateEngine.call_args[1]["creator"]
        # Mutating the caller's dictionary must not affect the engine's connection arguments
        kw["db"] = "changed"
        with mock.patch("wwpdb.utils.db.MyDbPool.MySQLdb.connect") as mockConnect:
            mockConnect.return_value = "rawcon"
            ret = creator()
        self.assertEqual(ret, "rawcon")
        mockConnect.assert_called_once_with(db="testdb", user="u", passwd="p", host="h", port=3307, local_infile=1)  # noqa: S106

    def testGetConnectionReturnsRawConnection(self) -> None:
        """getConnection() returns the engine's pooled raw DB-API connection"""
        con = getConnection({"db": "x"})
        self.assertIs(con, self.__engines[0].raw_connection.return_value)
        self.__engines[0].raw_connection.assert_called_once_with()
        # A second request uses the same engine (pool)
        getConnection({"db": "x"})
        self.assertEqual(self.__engines[0].raw_connection.call_count, 2)
        self.assertEqual(self.__mockCreateEngine.call_count, 1)

    def testGetConnectionPropagatesErrors(self) -> None:
        """Errors raised while obtaining a pooled connection propagate to the caller"""
        getEngine({"db": "x"})
        self.__engines[0].raw_connection.side_effect = RuntimeError("no server")
        with self.assertRaises(RuntimeError):
            getConnection({"db": "x"})

    def testDisposeAll(self) -> None:
        """disposeAll() disposes each engine and empties the cache so new engines are created afterwards"""
        getEngine({"db": "a"})
        getEngine({"db": "b"})
        e1 = self.__engines[0]
        e2 = self.__engines[1]
        disposeAll()
        e1.dispose.assert_called_once_with()
        e2.dispose.assert_called_once_with()
        self.assertIsNot(getEngine({"db": "a"}), e1)
        self.assertEqual(self.__mockCreateEngine.call_count, 3)
        e3 = self.__engines[2]
        # A second disposeAll() only touches engines created since the first
        disposeAll()
        e1.dispose.assert_called_once_with()
        e3.dispose.assert_called_once_with()

    def testDisposeAllEmpty(self) -> None:
        """disposeAll() with no engines is a no-op"""
        disposeAll()
        disposeAll()
        self.assertEqual(self.__mockCreateEngine.call_count, 0)

    def testGetEngineThreadSafe(self) -> None:
        """Concurrent first requests for the same arguments create a single engine"""

        def slowEngine(*args: Any, **kwargs: Any) -> mock.MagicMock:
            time.sleep(0.05)
            return self.__makeEngine(*args, **kwargs)

        self.__mockCreateEngine.side_effect = slowEngine
        results: List[Any] = []
        lock = threading.Lock()

        def worker() -> None:
            eng = getEngine({"db": "threaded"})
            with lock:
                results.append(eng)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(len(results), 8)
        self.assertEqual(self.__mockCreateEngine.call_count, 1)
        for eng in results:
            self.assertIs(eng, results[0])


if __name__ == "__main__":
    unittest.main()
