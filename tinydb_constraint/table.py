from tinydb.database import Table
from tinydb import Query

import os
import dateutil.parser
from datetime import datetime, date
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
        if isinstance(cond, (list, tuple)):
            con0 = (Query()[cond[0]] == fields.pop(cond[0]))
            for con in cond[1:]:
                con0 = (con0 & con)
            cond = con0
        elif isinstance(cond, str):
            cond = (Query()[cond] == fields.pop(cond))

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

        Returns:
            list -- List of records
        """

        def _records():
            for record in records:
                record_schema = dict(self._parse_record(record, yield_type=True))
                num_to_str = set()
                for k, v in record_schema.items():
                    expected_type = self.constraint_mapping.type_.get(k, None)
                    if expected_type and v is not expected_type:
                        if expected_type is str and v in (int, float):
                            # v = str
                            num_to_str.add(k)
                        else:
                            raise NonUniformTypeException('{} not in table schema {}'
                                                          .format(v, self.get_schema(refresh=False)))

                self.update_schema(record_schema)

                record = dict(self._parse_record(record, yield_type=False))
                for k, v in record.items():
                    if k in num_to_str:
                        record[k] = str(v)

                yield record

        if bool(int(os.getenv('TINYDB_SANITIZE', '1'))):
            self.refresh()
            for c in self.get_schema(refresh=False).values():
                assert not isinstance(c.type_, list)

            records = list(_records())
            self.refresh()

            return records
        else:
            return records

    def _sanitize_one(self, record):
        return self._sanitize_multiple([record])[0]

    @property
    def schema(self):
        """Get table's latest schema
        
        Returns:
            dict -- dictionary of constraints
        """

        return self.get_schema(refresh=True)

    def set_schema(self, schema_dict):
        """Reset and set a new schema
        
        Arguments:
            schema_dict {dict} -- dictionary of constraints or types
        """

        self.constraint_mapping = ConstraintMapping()
        self.update_schema(schema_dict)

    def get_schema(self, refresh=False):
        """Get table's schema, while providing an option to disable refreshing to allow faster getting of schema
        
        Keyword Arguments:
            refresh {bool} -- disable refreshing to allow faster getting of schema (default: {False})
        
        Returns:
            dict -- dictionary of constraints
        """

        if refresh:
            return self.refresh(output=True)
        else:
            return self.constraint_mapping.view()

    def update_schema(self, schema_dict):
        """Update the schema
        
        Arguments:
            schema_dict {dict} -- dictionary of constraints or types
        """

        self.constraint_mapping.update(schema_dict)

    def _update_uniqueness(self, k, v):
        self.constraint_mapping.preexisting[k].add(v)

    def refresh(self, output=False):
        """Refresh the schema table
        
        Keyword Arguments:
            output {bool} -- if False, there will be no output, and maybe a little faster (default: {False})
        
        Raises:
            NonUniformTypeException -- Type constraint failed
            NotNullException -- NotNull constraint failed
            NotUniqueException -- Unique constraint failed
        
        Returns:
            dict -- dictionary of constraints
        """

        output_mapping = None
        if output:
            output_mapping = deepcopy(self.constraint_mapping)

        for record in self.all():
            for k, v in self._parse_record(record, yield_type=True):
                expected_type = self.constraint_mapping.type_.get(k, None)
                if expected_type and v is not expected_type:
                    if expected_type is str and v in (int, float):
                        v = str
                    else:
                        raise NonUniformTypeException('{} type is not {}'.format(v, expected_type))

                if output_mapping:
                    type_list = output_mapping.type_.get(k, [])
                    if isinstance(type_list, type):
                        type_list = [type_list]

                    if v not in type_list:
                        type_list.append(v)

                    output_mapping.type_[k] = type_list

            record = dict(self._parse_record(record, yield_type=False))
            is_null = self.constraint_mapping.not_null - set(record.keys())

            if len(is_null) > 0:
                raise NotNullException('{} is null'.format(list(is_null)))

            for k, v in record.items():
                if k in self.constraint_mapping.preexisting.keys():
                    if v in self.constraint_mapping.preexisting[k]:
                        raise NotUniqueException('Duplicate {} for {} exists'.format(v, k))
                    else:
                        self._update_uniqueness(k, v)

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
                if isinstance(x, (datetime, date)):
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

            if isinstance(v, date):
                v = datetime.combine(v, datetime.min.time())

            yield k, _yield_switch(v)
