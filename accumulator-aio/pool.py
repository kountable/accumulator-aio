import asyncio
from contextlib import asynccontextmanager
from accumulator import Accumulator


class AccumulatorPool:
    def __init__(self, host, port, user, password, schema, size=10):
        self._conn_params = {'host': host, 'port': port, 'user': user, 'password': password, 'schema': schema}
        self._max_size = size
        self._actual_size = 0
        self._pool = []
        self._lock = asyncio.Lock()
        self._available = asyncio.Event()

    @asynccontextmanager
    async def get(self):
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

    async def _check_out(self):
        while True:
            async with self._lock:
                if self._is_available():
                    conn = self._pool.pop()
                    if len(self._pool) == 0:
                        self._make_unavailable()
                    return conn
                if self._actual_size < self._max_size:
                    self._actual_size = self._actual_size + 1
                    return await self._new_conn()
            await self._available.wait()

    async def _check_in(self, conn):
        async with self._lock:
            self._pool.append(conn)
            if not self._is_available():
                self._make_available()
