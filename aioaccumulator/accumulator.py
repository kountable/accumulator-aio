import thriftpy2
from thriftpy2.rpc import make_aio_client

from aioaccumulator._thriftpy_ext_framed_ import TAsyncFramedTransportFactory
from thriftpy2.contrib.aio.protocol.binary import TAsyncBinaryProtocolFactory
from aioaccumulator.iterators import *
from aioaccumulator.objects import Cell, AsyncBatchWriter, Mutation, Range


class Accumulator(object):
    def __init__(self, schema='../resources/proxy.thrift', host="localhost", port=50096, user='root',
                 password='secret'):
        self.thrift = thriftpy2.load(schema)
        self.login = None
        self.client = None
        self._conn_params = {'host': host, 'port': port, 'user': user, 'password': password}

    async def connect(self):
        self.client = await make_aio_client(self.thrift.AccumuloProxy,
                                            host=self._conn_params['host'],
                                            port=self._conn_params['port'],
                                            trans_factory=TAsyncFramedTransportFactory(),
                                            proto_factory=TAsyncBinaryProtocolFactory())
        self.login = await self.client.login(
            self._conn_params['user'], {'password': self._conn_params['password']})

    def dispose(self):
        self.client.close()

    async def list_tables(self):
        return await self.client.listTables(self.login)

    async def table_exists(self, table):
        return await self.client.tableExists(self.login, table)

    async def create_table(self, table):
        await self.client.createTable(self.login, table, True, self.thrift.TimeType.MILLIS)

    async def delete_table(self, table):
        await self.client.deleteTable(self.login, table)

    async def rename_table(self, oldtable, newtable):
        await self.client.renameTable(self.login, oldtable, newtable)

    async def write(self, table, mutations):
        if not isinstance(mutations, list) and not isinstance(mutations, tuple):
            mutations = [mutations]
        writer = await self.create_batch_writer(table)
        writer.add_mutations(mutations)
        writer.close()

    async def scan(self, table, scanrange=None, cols=None, auths=None, iterators=None, bufsize=None, batchsize=10):
        options = self.thrift.ScanOptions(auths, self._get_range(scanrange), self._get_scan_columns(cols),
                                          self._get_iterator_settings(iterators), bufsize)
        scanner = await self.client.createScanner(self.login, table, options)
        return self.perform_scan(scanner, batchsize)

    async def batch_scan(self, table, scanranges=None, cols=None, auths=None, iterators=None, numthreads=None,
                         batchsize=10):
        options = self.thrift.BatchScanOptions(auths, self._get_ranges(scanranges), self._get_scan_columns(cols),
                                               self._get_iterator_settings(iterators), numthreads)
        scanner = await self.client.createBatchScanner(self.login, table, options)
        return self.perform_scan(scanner, batchsize)

    async def perform_scan(self, scanner, batchsize):
        while True:
            results = await self.client.nextK(scanner, batchsize)
            for e in results.results:
                k, v = e.key, e.value
                yield Cell(k.row, k.colFamily, k.colQualifier, k.colVisibility, k.timestamp, v)

            if not results.more:
                await self.client.closeScanner(scanner)
                return

    async def create_batch_writer(self, table, max_memory=10 * 1024, latency_ms=30 * 1000, timeout_ms=5 * 1000,
                                  threads=10):
        return await AsyncBatchWriter.create(
            self, table, self.thrift.WriterOptions(
                maxMemory=max_memory, latencyMs=latency_ms, timeoutMs=timeout_ms, threads=threads))

    async def delete_rows(self, table, start_row, end_row):
        await self.client.deleteRows(self.login, table, start_row, end_row)

    async def attach_iterator(self, table, setting, scopes):
        await self.client.attachIterator(self.login, table, setting, scopes)

    async def remove_iterator(self, table, iterator, scopes):
        await self.client.removeIterator(self.login, table, iterator, scopes)

    async def following_key(self, key, part):
        return await self.client.getFollowing(key, part)

    async def get_max_row(self, table, authorizations=None, start_row=None,
                          start_inclusive=None, end_row=None, end_inclusive=None):
        return await self.client.getMaxRow(
            self.login, table, authorizations, start_row, start_inclusive, end_row, end_inclusive)

    def create_mutation(self, row):
        return Mutation(row, self.thrift)

    async def flush_mutations(self, table, mutations):
        if not isinstance(mutations, list) and not isinstance(mutations, tuple):
            mutations = [mutations]
        cells = {}
        for mut in mutations:
            cells.setdefault(mut.row, []).extend(mut.updates)
        await self.client.updateAndFlush(self.login, table, cells)

    async def create_user(self, user, password):
        await self.client.createLocalUser(self.login, user, password)

    async def drop_user(self, user):
        await self.client.dropLocalUser(self.login, user)

    async def list_users(self):
        return await self.client.listLocalUsers(self.login)

    async def set_user_authorizations(self, user, authorizations):
        await self.client.changeUserAuthorizations(self.login, user, authorizations)

    async def get_user_authorizations(self, user):
        return await self.client.getUserAuthorizations(self.login, user)

    async def grant_system_permission(self, user, perm):
        await self.client.grantSystemPermission(self.login, user, perm)

    async def revoke_system_permission(self, user, perm):
        await self.client.revokeSystemPermission(self.login, user, perm)

    async def has_system_permission(self, user, perm):
        return await self.client.hasSystemPermission(self.login, user, perm)

    async def grant_table_permission(self, user, table, perm):
        await self.client.grantTablePermission(self.login, user, table, perm)

    async def revoke_table_permission(self, user, table, perm):
        await self.client.revokeTablePermission(self.login, user, table, perm)

    async def has_table_permission(self, user, table, perm):
        return await self.client.hasTablePermission(self.login, user, table, perm)

    async def add_splits(self, table, splits):
        await self.client.addSplits(self.login, table, splits)

    async def add_constraint(self, table, class_name):
        return await self.client.addConstraint(self.login, table, class_name)

    async def list_constraints(self, table):
        return await self.client.listConstraints(self.login, table)

    async def remove_constraint(self, table, constraint):
        await self.client.removeConstraint(self.login, table, constraint)

    def _get_scan_columns(self, cols):
        if not cols:
            return []
        return [self.thrift.ScanColumn(colFamily=col.get('cf'), colQualifier=col.get('cq')) for col in cols]

    def _get_iterator_settings(self, iterators):
        if not iterators:
            return None
        return [self._process_iterator(i) for i in iterators]

    def _process_iterator(self, iter):
        if isinstance(iter, self.thrift.IteratorSetting):
            return iter
        elif isinstance(iter, BaseIterator):
            return iter.get_iterator_setting()
        else:
            raise Exception("Cannot process iterator: %s" % iter)

    @staticmethod
    def _get_range(scanrange):
        if scanrange:
            return scanrange.to_range()
        else:
            return None

    @staticmethod
    def _get_ranges(scanranges):
        if scanranges:
            return [scanrange.to_range() for scanrange in scanranges]
        else:
            return None

    # # # # # # # # # # HELPER FACTORY METHODS FOR DIFFERENT OBJECT TYPES # # # # # # # # # #
    def range(self,
              start_row=None,
              start_col_fam=None,
              start_col_qual=None,
              start_col_vis=None,
              start_timestamp=None,
              start_inclusive=True,
              end_row=None,
              end_col_fam=None,
              end_col_qual=None,
              end_col_vis=None,
              end_timestamp=None,
              end_inclusive=True
              ) -> Range:
        return Range(self.thrift, start_row, start_col_fam, start_col_qual, start_col_vis, start_timestamp,
                     start_inclusive, end_row, end_col_fam, end_col_qual, end_col_vis, end_timestamp, end_inclusive)

    def mutation(self, row) -> Mutation:
        return Mutation(self.thrift, row)

    def sum_combiner(self, priority=10, columns=None, combine_all_columns=True) -> SummingCombiner:
        return SummingCombiner(
            thrift=self.thrift, priority=priority, columns=columns, combine_all_columns=combine_all_columns)

    def sum_array_combiner(self, priority=10, combine_all_columns=True) -> SummingArrayCombiner:
        return SummingArrayCombiner(
            thrift=self.thrift, priority=priority, combine_all_columns=combine_all_columns)

    def min_combiner(self, columns=None, priority=10, combine_all_columns=True) -> MinCombiner:
        return MinCombiner(
            thrift=self.thrift, columns=columns, priority=priority, combine_all_columns=combine_all_columns)

    def max_combiner(self, columns=None, priority=10, combine_all_columns=True) -> MaxCombiner:
        return MaxCombiner(
            thrift=self.thrift, columns=columns, priority=priority, combine_all_columns=combine_all_columns)

    def row_delete_iterator(self, priority=10) -> RowDeletingIterator:
        return RowDeletingIterator(thrift=self.thrift, priority=priority)

    def grep_iterator(self, term, priority=10, negate=False) -> GrepIterator:
        return GrepIterator(thrift=self.thrift, term=term, priority=priority, negate=negate)

    def regex_filter(self, row_regex=None, cf_regex=None, cq_regex=None, val_regex=None, or_fields=False,
                     match_substring=False, priority=10) -> RegExFilter:
        return RegExFilter(thrift=self.thrift, row_regex=row_regex, cf_regex=cf_regex, cq_regex=cq_regex,
                           val_regex=val_regex, or_fields=or_fields,
                           match_substring=match_substring, priority=priority)

    def intersect_iterator(self, terms, not_flags=None, priority=10) -> IntersectingIterator:
        return IntersectingIterator(thrift=self.thrift, terms=terms, not_flags=not_flags, priority=priority)

    def indexed_doc_iterator(self, terms, not_flags=None, priority=10,
                             index_col_fam='i', doc_col_fam='e') -> IndexedDocIterator:
        return IndexedDocIterator(thrift=self.thrift, terms=terms, not_flags=not_flags, priority=priority,
                                  index_colf=index_col_fam, doc_colf=doc_col_fam)
