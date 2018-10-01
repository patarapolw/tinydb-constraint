from tinydb.database import Table

import os
import dateutil.parser
from datetime import datetime
from copy import deepcopy

from .util import remove_control_chars
from .constraint import ConstraintMapping
from .exception import NonUniformTypeException, NotNullException, NotUniqueException


class ConstraintTable(Table):
    constraint_mapping = ConstraintMapping()

    def insert(self, element):
        return super().insert(self._sanitize_one(element))

    def insert_multiple(self, elements):
        return super().insert_multiple(self._sanitize_multiple(elements))

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
                record_schema = tuple(self._parse_record(record, yield_type=True))
                for _k, _v in record_schema:
                    if _v is not self.constraint_mapping.type_[_k]:
                        raise NonUniformTypeException('{} not in table schema {}'
                                                      .format(_v, self.get_schema(refresh=False)))

                self.update_schema(record_schema)
                yield dict(self._parse_record(record, yield_type=False))

        if bool(int(os.getenv('TINYDB_SANITIZE', '1'))):
            self.refresh()
            for v in self.get_schema(refresh=False).values():
                assert not isinstance(v.type_, list)

            records = list(_records())
            self.refresh()

            return records
        else:
            return records

    def _sanitize_one(self, record):
        return self._sanitize_multiple([record])[0]

    @property
    def schema(self):
        return self.get_schema(refresh=True)

    @schema.setter
    def schema(self, schema_dict):
        self.update_schema(schema_dict)

    def get_schema(self, refresh=False):
        if refresh:
            return self.refresh(output=True)
        else:
            return self.constraint_mapping.view()

    def update_schema(self, schema_dict):
        self.constraint_mapping.update(schema_dict)

    def update_uniqueness(self, k, v):
        self.constraint_mapping.preexisting[k].add(v)

    def refresh(self, output=False):
        output_mapping = None
        if output:
            output_mapping = deepcopy(self.constraint_mapping)

        for record in self.all():
            for k, v in self._parse_record(record, yield_type=True):
                expected_type = self.constraint_mapping.type_.get(k, None)
                if expected_type and v is not expected_type:
                    raise NonUniformTypeException('{} type is not {}'.format(v, expected_type))

                if output_mapping:
                    output_mapping.type_.setdefault(k, []).append(v)

            record = dict(self._parse_record(record, yield_type=False))
            is_null = self.constraint_mapping.not_null - set(record.keys())

            if len(is_null) > 0:
                raise NotNullException('{} is null'.format(list(is_null)))

            for k, v in record.items():
                if k in self.constraint_mapping.preexisting.keys():
                    if v in self.constraint_mapping.preexisting[k]:
                        raise NotUniqueException('Duplicate {} for {} exists'.format(v, k))
                    else:
                        self.update_uniqueness(k, v)

        if output_mapping:
            for k, v in output_mapping.type_.items():
                if isinstance(v, list) and len(v) == 1:
                    output_mapping.type_[k] = v[0]

            return output_mapping.view()

    @staticmethod
    def _parse_record(record, yield_type):
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
