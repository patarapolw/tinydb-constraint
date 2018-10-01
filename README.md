# tinydb-constraint

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
```

If you want to enable TinyDB-constraint for all databases in a session, run:

```python
>>> from tinydb import TinyDB
>>> from tinydb_constraint import ConstraintTable
>>> TinyDB.table_class = ConstraintTable
```

## Note

I haven't modified the serialization yet, so `datetime` type will actually produce `datetime.isoformat()`, and to set `datetime`, you have to pass a `dateutil.parser.parse()`-parsable string.

## Related projects

- [tinydb-viewer](https://github.com/patarapolw/tinydb-viewer) - View records generated from TinyDB and alike (e.g. list of dictionaries.)
