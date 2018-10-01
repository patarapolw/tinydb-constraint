import unicodedata
import re
from datetime import datetime
import os
import dateutil.parser


all_chars = (chr(i) for i in range(0x110000))
control_chars = ''.join(c for c in all_chars if unicodedata.category(c) in {'Cc'})

control_char_re = re.compile('[%s]' % re.escape(control_chars))


def remove_control_chars(s):
    return unicodedata.normalize("NFKD", control_char_re.sub('', s))


def jsonify(d):
    for k, v in d.items():
        if isinstance(v, datetime):
            yield k, v.isoformat()
        else:
            yield k, v


def parse_record(record):
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

        yield k, v
