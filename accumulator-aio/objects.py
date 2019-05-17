from array import array
from collections import namedtuple

Cell = namedtuple("Cell", "row cf cq cv ts val")


class Mutation(object):
    def __init__(self, thrift, row):
        super(Mutation, self).__init__()
        self.row = row
        self.thrift = thrift
        self.updates = []

    def put(self, cf='', cq='', cv=None, ts=None, val='', is_delete=None):
        self.updates.append(self.thrift.ColumnUpdate(
            colFamily=cf, colQualifier=cq, colVisibility=cv,
            timestamp=ts, value=val, deleteCell=is_delete))


class Range(object):
    def __init__(self, thrift,
                 srow=None, scf=None, scq=None, scv=None, sts=None, sinclude=True,
                 erow=None, ecf=None, ecq=None, ecv=None, ets=None, einclude=True):

        super(Range, self).__init__()
        self.thrift = thrift
        self.srow = srow
        self.scf = scf
        self.scq = scq
        self.scv = scv
        self.sts = sts
        self.sinclude = sinclude

        self.erow = erow
        self.ecf = ecf
        self.ecq = ecq
        self.ecv = ecv
        self.ets = ets
        self.einclude = einclude

    @staticmethod
    def following_prefix(prefix):
        """Returns a String that sorts just after all Strings beginning with a prefix"""
        prefix_bytes = array('B', prefix)

        change_index = len(prefix_bytes) - 1
        while change_index >= 0 and prefix_bytes[change_index] == 0xff:
            change_index = change_index - 1
        if change_index < 0:
            return None
        new_bytes = array('B', prefix[0:change_index + 1])
        new_bytes[change_index] = new_bytes[change_index] + 1
        return new_bytes.tostring()

    @staticmethod
    def prefix(row_prefix):
        """Returns a Range that covers all rows beginning with a prefix"""
        fp = Range.following_prefix(row_prefix)
        return Range(srow=row_prefix, sinclude=True, erow=fp, einclude=False)

    def to_range(self):
        r = self.thrift.Range()
        r.startInclusive = self.sinclude
        r.stopInclusive = self.einclude

        if self.srow:
            r.start = self.thrift.Key(row=self.srow, colFamily=self.scf, colQualifier=self.scq, colVisibility=self.scv,
                                      timestamp=self.sts)
            if not self.sinclude:
                r.start = _following_key(r.start)

        if self.erow:
            r.stop = self.thrift.Key(row=self.erow, colFamily=self.ecf, colQualifier=self.ecq, colVisibility=self.ecv,
                                     timestamp=self.ets)
            if self.einclude:
                r.stop = _following_key(r.stop)

        return r


class AsyncBatchWriter(object):
    @classmethod
    async def create(cls, conn, table, writer_options):
        wr = AsyncBatchWriter()
        wr._conn = conn
        wr._writer = conn.client.createWriter(conn.login, table, writer_options)
        wr._is_closed = False

    def __init__(self):
        self._conn = None
        self._writer = None
        self._is_closed = True

    async def add_mutations(self, mutations):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")

        cells = {}
        for mutation in mutations:
            cells.setdefault(mutation.row, []).extend(mutation.updates)
        await self._conn.client.update(self._writer, cells)

    async def add_mutation(self, mut):
        if self._is_closed:
            raise Exception("Cannot write to a closed writer")
        await self._conn.client.update(self._writer, {mut.row: mut.updates})

    async def flush(self):
        if self._is_closed:
            raise Exception("Cannot flush a closed writer")
        await self._conn.client.flush(self._writer)

    async def close(self):
        await self._conn.client.closeWriter(self._writer)
        self._is_closed = True


def _following_array(val):
    if val:
        return val + b'\0'
    return None


def _following_key(key):
    if key.timestamp is not None:
        key.timestamp -= 1
    elif key.colVisibility is not None:
        key.colVisibility = _following_array(key.colVisibility)
    elif key.colQualifier is not None:
        key.colQualifier = _following_array(key.colQualifier)
    elif key.colFamily is not None:
        key.colFamily = _following_array(key.colFamily)
    elif key.row is not None:
        key.row = _following_array(key.row)
    return key
