from __future__ import absolute_import

import asyncio
import struct
from io import BytesIO

from thriftpy2.contrib.aio.transport.buffered import readall
from thriftpy2.transport import TTransportBase


class TAsyncFramedTransport(TTransportBase):
    def __init__(self, trans):
        self._trans = trans
        self._rbuf = BytesIO()
        self._wbuf = BytesIO()

    def is_open(self):
        return self._trans.is_open()

    @asyncio.coroutine
    def open(self):
        return self._trans.open()

    def close(self):
        return self._trans.close()

    @asyncio.coroutine
    def read(self, sz):
        if sz == 0:
            return b''

        ret = self._rbuf.read(sz)
        if len(ret) != 0:
            return ret

        yield from self.read_frame()
        return self._rbuf.read(sz)

    @asyncio.coroutine
    def read_frame(self):
        buff = yield from readall(self._trans.read, 4)
        sz, = struct.unpack('!i', buff)
        frame = yield from readall(self._trans.read, sz)
        self._rbuf = BytesIO(frame)

    def write(self, buf):
        self._wbuf.write(buf)

    @asyncio.coroutine
    def flush(self):
        out = self._wbuf.getvalue()
        self._wbuf = BytesIO()
        self._trans.write(struct.pack("!i", len(out)) + out)
        yield from self._trans.flush()

    def getvalue(self):
        return self._trans.getvalue()


class TAsyncFramedTransportFactory(object):
    def get_transport(self, trans):
        return TAsyncFramedTransport(trans)
