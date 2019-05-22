import asyncio
from contextlib import asynccontextmanager
from aioaccumulator import Accumulator
from typing import List


class AccumulatorPool:
    def __init__(self, host, port, user, password, schema, max_size=10, min_size=1):
        """
        :param schema: Path to Thrift schema file
        :param max_size: Maximum connections
        :param min_size: Minimum connections (pool shrinks if demand is low)
        """
        self._conn_params = {'host': host, 'port': port, 'user': user, 'password': password, 'schema': schema}
        self._max_size = max_size
        self._min_size = min_size if min_size is not None and min_size > 0 else 1
        self._actual_size = 0
        self._pool: List[Accumulator] = []
        self._lock = asyncio.Lock()
        self._available = asyncio.Event()
        self._checked_out = 0

    @asynccontextmanager
    async def get(self):
        """
        Example usage:
            pool = AccumulatorPool(...)
            async with pool.get() as conn:
                tables = conn.list_tables()

            # use "tables" here, the connection will be returned to the pool after exiting with: block

        :return: Accumulator as a context manager
        """
        conn = await self._check_out()
        yield conn
        await self._check_in(conn)

    async def _new_conn(self):
        conn = Accumulator(**self._conn_params)
        await conn.connect()
        return conn

    def _is_available(self) -> bool:
        return self._available.is_set()

    def _make_available(self):
        self._available.set()

    def _make_unavailable(self):
        self._available.clear()

    def _dispose_one(self):
        self._pool.pop().dispose()
        self._actual_size -= 1

    async def _check_out(self):
        while True:
            async with self._lock:
                if self._is_available():
                    conn = self._pool.pop()
                    if len(self._pool) == 0:
                        self._make_unavailable()
                    self._checked_out += 1
                    return conn
                if self._actual_size < self._max_size:
                    self._actual_size = self._actual_size + 1
                    conn = await self._new_conn()
                    self._checked_out += 1
                    return conn
            await self._available.wait()

    async def _check_in(self, conn):
        async with self._lock:
            self._pool.append(conn)
            self._checked_out -= 1
            if not self._is_available():
                self._make_available()
                return

            size = len(self._pool)
            if size == self._min_size or self._checked_out > size:
                return
            if self._checked_out == 0 or self._checked_out * 2 >= size:
                new_size = size - (size // 2)
                new_size = new_size if new_size > self._min_size else self._min_size
                for _ in range(0, size - new_size):
                    self._dispose_one()
