class BaseIterator(object):
    def __init__(self, thrift, name, priority, classname):
        super(BaseIterator, self).__init__()
        self.name = name
        self.priority = priority
        self.classname = classname
        self.thrift = thrift

    def get_iterator_setting(self):
        i = self.thrift.IteratorSetting()
        i.priority = self.priority
        i.name = self.name
        i.iteratorClass = self.classname
        i.properties = self._get_iterator_properties()
        return i

    def _get_iterator_properties(self):
        return {}

    def attach(self, conn, table, scopes=None):
        if scopes is None:
            scopes = {
                self.thrift.IteratorScope.SCAN,
                self.thrift.IteratorScope.MINC,
                self.thrift.IteratorScope.MAJC
            }
        conn.client.attachIterator(conn.login, table, self.get_iterator_setting(), scopes)


class BaseCombiner(BaseIterator):
    def __init__(self, thrift, name, priority, classname, columns=None, combine_all_columns=True,
                 encoding_type="STRING"):
        super(BaseCombiner, self).__init__(thrift, name, priority, classname)
        self.columns = columns
        self.encoding_type = encoding_type

        if not columns:
            self.combine_all_columns = combine_all_columns
        else:
            self.combine_all_columns = False

    def add_column(self, colf, colq=None):
        self.combine_all_columns = False
        if colq:
            self.columns.append([colf, colq])
        else:
            self.columns.append([colf])

    def _get_iterator_properties(self):
        return {
            "type": self.encoding_type,
            "all": str(self.combine_all_columns).lower(),
            "columns": ",".join([self._encode_column(col) for col in self.columns])
        }

    def _encode_column(self, col):
        return col[0] if len(col) == 1 else ":".join(col)


class SummingCombiner(BaseCombiner):
    def __init__(self, thrift, name="SummingCombiner", priority=10, columns=None, combine_all_columns=True,
                 encoding_type="STRING"):
        super(SummingCombiner, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.SummingCombiner",
            columns=columns, combine_all_columns=combine_all_columns,
            encoding_type=encoding_type)


class SummingArrayCombiner(BaseCombiner):
    def __init__(self, thrift, name="SummingArrayCombiner", priority=10, columns=None, combine_all_columns=True,
                 encoding_type="STRING"):
        super(SummingArrayCombiner, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.SummingArrayCombiner",
            columns=columns, combine_all_columns=combine_all_columns,
            encoding_type=encoding_type)


class MaxCombiner(BaseCombiner):
    def __init__(self, thrift, name="MaxCombiner", priority=10, columns=None, combine_all_columns=True,
                 encoding_type="STRING"):
        super(MaxCombiner, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.MaxCombiner",
            columns=columns, combine_all_columns=combine_all_columns,
            encoding_type=encoding_type)


class MinCombiner(BaseCombiner):
    def __init__(self, thrift, name="MinCombiner", priority=10, columns=None, combine_all_columns=True,
                 encoding_type="STRING"):
        super(MinCombiner, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.MinCombiner",
            columns=columns, combine_all_columns=combine_all_columns,
            encoding_type=encoding_type)


class GrepIterator(BaseIterator):
    def __init__(self, thrift, term, negate=False, priority=10, name="GrepIterator"):
        super(GrepIterator, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.GrepIterator")
        self.term = term
        self.negate = negate

    def _get_iterator_properties(self):
        return {
            "term": self.term,
            "negate": str(self.negate).lower()
        }


class RowDeletingIterator(BaseIterator):
    """
    An iterator for deleting whole rows. After setting this iterator up for
    your table, to delete a row insert a row with empty column family, empty
    column qualifier, empty column visibility, and a value of DEL_ROW. Do not
    use empty columns for anything else when using this iterator. When using
    this iterator the locality group containing the row deletes will always be
    read. The locality group containing the empty column family will contain
    row deletes. Always reading this locality group can have an impact on
    performance. For example assume there are two locality groups, one
    containing large images and one containing small metadata about the images.
    If row deletes are in the same locality group as the images, then this will
    significantly slow down scans and major compactions that are only reading
    the metadata locality group. Therefore, you would want to put the empty
    column family in the locality group that contains the metadata. Another
    option is to put the empty column in its own locality group. Which is best
    depends on your data.
    """
    def __init__(self, thrift, name="RowDeletingIterator", priority=10):
        super(RowDeletingIterator, self).__init__(
            thrift=thrift, name=name,
            priority=priority,
            classname="org.apache.accumulo.core.iterators.user.RowDeletingIterator")


class RegExFilter(BaseIterator):
    def __init__(self, thrift, row_regex=None, cf_regex=None, cq_regex=None, val_regex=None, or_fields=False,
                 match_substring=False, priority=10, name="RegExFilter"):
        super(RegExFilter, self).__init__(
            thrift=thrift, name=name, priority=priority,
            classname="org.apache.accumulo.core.iterators.user.RegExFilter")
        self.row_regex = row_regex
        self.cf_regex = cf_regex
        self.cq_regex = cq_regex
        self.val_regex = val_regex
        self.or_fields = or_fields
        self.match_substring = match_substring

    def _get_iterator_properties(self):
        props = {}

        if self.row_regex:
            props["rowRegex"] = self.row_regex
        if self.cf_regex:
            props["colfRegex"] = self.cf_regex
        if self.cq_regex:
            props["colqRegex"] = self.cq_regex
        if self.val_regex:
            props["valueRegex"] = self.val_regex

        props["orFields"] = str(self.or_fields).lower()
        props["matchSubstring"] = str(self.match_substring).lower()

        return props


class IntersectingIterator(BaseIterator):
    def __init__(self, thrift, terms, not_flags=None, priority=10, name="IntersectingIterator"):
        super(IntersectingIterator, self).__init__(
            thrift=thrift,
            name=name,
            priority=priority,
            classname="org.apache.accumulo.core.iterators.user.IntersectingIterator"
        )
        self.terms = terms
        self.not_flags = not_flags

    def _get_iterator_properties(self):
        props = {
            "columnFamilies": self._encode_columns(self.terms)
        }
        if self.not_flags:
            props["notFlag"] = self._encode_not_flags(self.not_flags)
        return props

    def _encode_columns(self, cols):
        return "".join([col.encode("base64") for col in cols]).rstrip()

    def _encode_not_flags(self, flags):
        if flags:
            return "".join([self._convert_flag(flag) for flag in flags]).encode("base64")
        else:
            return None

    def _convert_flag(self, flag):
        if flag == 0:
            return "\0"
        elif flag == 1:
            return "\001"
        else:
            raise Exception("invalid flag")


class IndexedDocIterator(IntersectingIterator):
    def __init__(self, thrift, terms, not_flags=None, priority=10, name="IndexedDocIterator", index_colf="i",
                 doc_colf="e"):
        super(IndexedDocIterator, self).__init__(
            thrift=thrift, name=name, priority=priority, terms=terms,
            not_flags=not_flags)
        self.classname = "org.apache.accumulo.core.iterators.user.IndexedDocIterator"
        self.index_colf = index_colf
        self.doc_colf = doc_colf

    def _get_iterator_properties(self):
        props = super(IndexedDocIterator, self)._get_iterator_properties()
        props["indexFamily"] = self.index_colf
        props["docFamily"] = self.doc_colf
        return props
