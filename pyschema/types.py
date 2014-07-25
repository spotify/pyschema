# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import datetime

import core
from core import ParseError, Field
import binascii


class Text(Field):
    def __init__(self, nullable=True, **kwargs):
        super(Text, self).__init__(**kwargs)
        self.nullable = nullable

    def load(self, obj):
        if not isinstance(obj, (unicode, type(None))):
            raise ParseError("%r not a unicode object" % obj)
        return obj

    def dump(self, obj):
        if isinstance(obj, (unicode, type(None))):
            return obj
        else:
            try:
                return obj.decode('utf8')
            except:
                raise ValueError(
                    "%r is not a valid UTF-8 string" % obj
                )


class Bytes(Field):
    """Binary data"""

    def __init__(self, custom_encoding=False, nullable=True, **kwargs):
        super(Bytes, self).__init__(**kwargs)
        self.custom_encoding = custom_encoding
        self.nullable = nullable

    def _load_utf8_codepoints(self, obj):
        return obj.encode("iso-8859-1")

    def _dump_utf8_codepoints(self, binary_data):
        return binary_data.decode("iso-8859-1")

    def _load_b64(self, obj):
        return binascii.a2b_base64(obj.encode("ascii"))

    def _dump_b64(self, binary_data):
        return binascii.b2a_base64(binary_data).rstrip('\n')

    def load(self, obj):
        if not self.custom_encoding:
            return self._load_utf8_codepoints(obj)
        return self._load_b64(obj)

    def dump(self, binary_data):
        if isinstance(binary_data, unicode):
            raise ValueError(
                "Unicode objects are not accepted values for Bytes (%r)"
                % (binary_data,)
            )
        if not self.custom_encoding:
            return self._dump_utf8_codepoints(binary_data)
        return self._dump_b64(binary_data)


class List(Field):
    def __init__(self, field_type=Text(), nullable=False, **kwargs):
        super(List, self).__init__(**kwargs)
        self.field_type = field_type
        self.nullable = nullable

    def load(self, obj):
        if not isinstance(obj, list):
            raise ParseError("%r is not a list object" % obj)
        return [self.field_type.load(o) for o in obj]

    def dump(self, obj):
        if not isinstance(obj, (tuple, list)):
            raise ValueError("%r is not a list object" % obj)
        return [self.field_type.dump(o) for o in obj]

    def set_parent(self, schema):
        self.field_type.set_parent(schema)


class Enum(Field):
    _field_type = Text()  # don't change

    def __init__(self, values, nullable=True, **kwargs):
        super(Enum, self).__init__(**kwargs)
        self.values = set(values)
        self.nullable = nullable

    def dump(self, obj):
        if obj not in self.values:
            raise ValueError(
                "%r is not an allowed value of Enum%r"
                % (obj, tuple(self.values)))
        return self._field_type.dump(obj)

    def load(self, obj):
        parsed = self._field_type.load(obj)
        if parsed not in self.values and parsed is not None:
            raise ParseError(
                "Parsed value %r not in allowed value of Enum(%r)"
                % (parsed, tuple(self.values)))
        return parsed


class Integer(Field):
    def __init__(self, nullable=True, size=8, **kwargs):
        super(Integer, self).__init__(**kwargs)
        self.nullable = nullable
        self.size = size

    def dump(self, obj):
        if not isinstance(obj, (int, type(None))):
            raise ValueError("%r is not a valid Integer" % (obj,))
        return obj

    def load(self, obj):
        if not isinstance(obj, (int, type(None))):
            raise ParseError("%r is not a valid Integer" % (obj,))
        return obj


class Boolean(Field):
    VALUE_MAP = {True: '1', 1: '1',
                 False: '0', 0: '0'}

    def __init__(self, nullable=True, **kwargs):
        super(Boolean, self).__init__(**kwargs)
        self.nullable = nullable

    def dump(self, obj):
        if obj not in self.VALUE_MAP:
            raise ValueError(
                "Invalid value for Boolean field: %r" % obj)
        return bool(obj)

    def load(self, obj):
        if obj not in self.VALUE_MAP:
            raise ParseError(
                "Invalid value for Boolean field: %r" % obj)
        return bool(obj)


class Float(Field):
    def __init__(self, nullable=True, size=8, **kwargs):
        super(Float, self).__init__(**kwargs)
        self.nullable = nullable
        self.size = size

    def dump(self, obj):
        if not isinstance(obj, float):
            raise ValueError("Invalid value for Float field: %r" % obj)
        return float(obj)

    def load(self, obj):
        if not isinstance(obj, float):
            raise ParseError("Invalid value for Float field: %r" % obj)
        return float(obj)


class Date(Text):
    def __init__(self, nullable=True, **kwargs):
        super(Date, self).__init__(**kwargs)
        self.nullable = nullable

    def dump(self, obj):
        if not isinstance(obj, datetime.date):
            raise ValueError("Invalid value for Date field: %r" % obj)
        return str(obj)

    def load(self, obj):
        try:
            return datetime.datetime.strptime(obj, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("Invalid value for Date field: %r" % obj)


class DateTime(Text):
    def __init__(self, nullable=True, **kwargs):
        super(DateTime, self).__init__(**kwargs)
        self.nullable = nullable

    def dump(self, obj):
        if not isinstance(obj, datetime.datetime):
            raise ValueError("Invalid value for DateTime field: %r" % obj)
        return str(obj)

    def load(self, obj):
        try:
            if '.' in obj:
                return datetime.datetime.strptime(obj, "%Y-%m-%d %H:%M:%S.%f")
            return datetime.datetime.strptime(obj, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("Invalid value for DateField field: %r" % obj)


SELF = object()


class SubRecord(Field):
    "Field for storing :class:`record.Record`s as fields "
    "in other :class:`record.Record`s"

    def __init__(self, record_class, nullable=True, **kwargs):
        super(SubRecord, self).__init__(**kwargs)
        self._record_class = record_class
        self.nullable = nullable

    def dump(self, obj):
        if not isinstance(obj, self._record_class):
            raise ValueError("%r is not a %r"
                             % (obj, self._record_class))
        return core.to_json_compatible(obj)

    def load(self, obj):
        return core.from_json_compatible(self._record_class, obj)

    def set_parent(self, schema):
        if self._record_class == SELF:
            self._record_class = schema


class Map(Field):
    def __init__(self, value_type, nullable=False, **kwargs):
        super(Map, self).__init__(**kwargs)
        self.value_type = value_type
        self.nullable = nullable
        self.key_type = Text()

    def load(self, obj):
        return dict([
            (self.key_type.load(k),
             self.value_type.load(v))
            for k, v in obj.iteritems()
        ])

    def dump(self, obj):
        if not isinstance(obj, dict):
            raise ValueError("%r is not a dict" % (obj,))

        return dict([
            (self.key_type.dump(k),
             self.value_type.dump(v))
            for k, v in obj.iteritems()
        ])

    def set_parent(self, schema):
        self.value_type.set_parent(schema)
