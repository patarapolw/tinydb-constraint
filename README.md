# tinydb-constraint

[![PyPI version shields.io](https://img.shields.io/pypi/v/tinydb-constraint.svg)](https://pypi.python.org/pypi/tinydb-constraint/)
[![PyPI license](https://img.shields.io/pypi/l/tinydb-constraint.svg)](https://pypi.python.org/pypi/tinydb-constraint/)

Apply constraints before inserting and updating TinyDB records.

## Installation

Method 1:

```commandline
$ pip install tinydb-constraint
```

Method 2:

- Clone the project from GitHub
- [Get poetry](https://github.com/sdispater/poetry) and `poetry install tinydb-constraint --path PATH/TO/TINYDB/CONSTRAINT`

## Usage

```python
>>> from tinydb import TinyDB
>>> from tinydb_constraint import ConstraintTable
>>> from datetime import datetime
>>> db = TinyDB('db.json')
>>> db.table_class = ConstraintTable
>>> db.schema = {
...     'record_id': int,
...     'modified': datetime
... }
>>> db.schema
{
    'record_id': Constraint(type_=int, unique=False, not_null=False),
    'modified': Constraint(type_=datetime.datetime, unique=False, not_null=False)
}
```

If you want to enable TinyDB-constraint for all databases in a session, run:

```python
>>> from tinydb import TinyDB
>>> from tinydb_constraint import ConstraintTable
>>> TinyDB.table_class = ConstraintTable
```

## Note

I haven't modified the serialization yet, so `datetime` type will actually produce `datetime.isoformat()`, and to set `datetime`, you have to pass a `dateutil.parser.parse()`-parsable string.

## Advanced usage

Database schema is also settable via `Constraint` object.

```python
>>> from tinydb_constraint import Constraint
>>> db.schema = {
...     'user_id': Constraint(type_=int, unique=True, not_null=True)
... }
```

If you want to disable certain string sanitization features, like stripping spaces or checking if string can be converted to datetime, this can be done by setting environmental variables.

```
TINYDB_SANITIZE=0
TINYDB_DATETIME=0
```

## Plan

- Add ForeignKey constraints.

## Related projects

- [tinydb-viewer](https://github.com/patarapolw/tinydb-viewer) - View records generated from TinyDB and alike (e.g. list of dictionaries.)
