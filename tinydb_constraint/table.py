from tinydb.database import Table

import os
import dateutil.parser
from datetime import datetime

from .util import remove_control_chars
from .exception import ConstraintException


class ConstraintTable(Table):
    view_dict = None
    _schema = None

    def insert(self, element):
        return super().insert(element)

    def insert_multiple(self, elements):
        return super().insert_multiple(elements)

    def update(self, fields, cond=None, doc_ids=None, eids=None):
        if doc_ids is None:
            doc_ids = list()

        if callable(fields):
            _update = lambda data, eid: self._sanitize_one(fields(data[eid]))
        else:
            _update = lambda data, eid: data[eid].update(self._sanitize_one(fields))

        return self.process_elements(_update, cond, doc_ids, eids)

    def _sanitize_multiple(self, records):
        """Sanitizes records, e.g. from Excel spreadsheet

        Arguments:
            records {iterable} -- Iterable of records

        Keyword Arguments:
            schema {dict} -- Dictionary of schemas (default: {None})
            table_name {str} -- Table name to get from schema (default: {None})

        Returns:
            list -- List of records
        """

        def _records():
            for record in records:
                record_schema = tuple(self._parse_record(record))
                for _k, _v in record_schema:
                    if _v is not table_schema[_k]:
                        raise ConstraintException('{} not in table schema {}'.format(_v, table_schema))

                table_schema.update(record_schema)

                yield dict(self._parse_record(record, yield_type=False))

        if bool(int(os.getenv('TINYDB_SANITIZE', '0'))):
            return records
        else:
            table_schema = self.schema
            for v in table_schema.values():
                assert not isinstance(v, (list, tuple, set))

            return list(_records())

    def _sanitize_one(self, record):
        return self._sanitize_multiple([record])[0]

    @property
    def schema(self):
        if self._schema is None:
            self._schema = dict()

        for record in self.all():
            for k, v in self._parse_record(record):
                self._schema.setdefault(k, set()).add(v)

        for k, v in self._schema.items():
            if len(v) == 1:
                self._schema[k] = v.pop()
            else:
                self._schema[k] = list(v)

        return self._schema

    @schema.setter
    def schema(self, constraint):
        if self._schema is None:
            self._schema = dict()

        self._schema.update(constraint)

    @staticmethod
    def _parse_record(record, yield_type=True):
        def _yield_switch(x):
            if yield_type:
                return type(x)
            else:
                if isinstance(x, datetime):
                    return x.isoformat()
                else:
                    return x

        for k, v in record.items():
            if bool(int(os.getenv('TINYDB_DATETIME', '1'))):
                if isinstance(v, str):
                    v = remove_control_chars(v.strip())
                    if v.isdigit():
                        v = int(v)
                    elif '.' in v and v.replace('.', '', 1).isdigit():
                        v = float(v)
                    elif v in {'', '-'}:
                        continue
                    else:
                        try:
                            v = dateutil.parser.parse(v)
                        except ValueError:
                            pass

            yield k, _yield_switch(v)
