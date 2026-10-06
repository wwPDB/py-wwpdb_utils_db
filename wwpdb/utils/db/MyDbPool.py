##
# File:    MyDbPool.py
# Date:    30-Sep-2026
#
# Updates:
#
##
"""
Shared MySQL connection pooling built on SQLAlchemy engines (create_engine).

Replaces the ``sqlalchemy.pool.manage()`` DB-API proxy, which was removed in
SQLAlchemy 2.0.  One engine (with its own QueuePool) is maintained per process
for each distinct set of MySQLdb.connect() keyword arguments, so all callers
(MyDbUtil.MyDbConnect, MyConnectionBase, ...) connecting with the same
arguments share the same pool.

Connections returned by getConnection() are pool-proxied DB-API connections
(cursor(), commit(), rollback(), close() ...); close() returns the connection
to the pool.  Compatible with SQLAlchemy 1.4 and 2.x.
"""

import threading
from typing import Any, Dict, Tuple

import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_poolOptions: Dict[str, Any] = {"pool_size": 12, "max_overflow": 12, "pool_timeout": 30, "pool_recycle": 1800, "echo_pool": True}
_engineCache: Dict[Tuple[Tuple[str, Any], ...], Engine] = {}
_engineLock = threading.Lock()


def getEngine(connectKw: Dict[str, Any]) -> Engine:
    """Return the shared (cached) engine for the input MySQLdb.connect() keyword arguments."""
    key = tuple(sorted(connectKw.items()))
    engine = _engineCache.get(key)
    if engine is None:
        with _engineLock:
            engine = _engineCache.get(key)
            if engine is None:
                kw = dict(connectKw)

                def creator() -> Any:
                    return MySQLdb.connect(**kw)

                engine = create_engine("mysql+mysqldb://", creator=creator, **_poolOptions)
                _engineCache[key] = engine
    return engine


def getConnection(connectKw: Dict[str, Any]) -> Any:
    """Return a pooled DB-API connection for the input MySQLdb.connect() keyword arguments."""
    return getEngine(connectKw).raw_connection()


def disposeAll() -> None:
    """Close all pooled connections and discard all engines (e.g. after fork())."""
    with _engineLock:
        for engine in _engineCache.values():
            engine.dispose()
        _engineCache.clear()
