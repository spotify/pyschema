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

import core
from core import ParseError, Field
import binascii


class Text(Field):
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
                raise ParseError(
                    "%r is not a valid UTF-8 string" % obj
                )


class Bytes(Field):
    """Binary data"""

    def __init__(self, human_readable=False, **kwargs):
        super(Bytes, self).__init__(**kwargs)
        self.human_readable = human_readable

    def _load_utf8_codepoints(self, obj):
        return obj.encode("iso-8859-1")

    def _dump_utf8_codepoints(self, binary_data):
        return binary_data.decode("iso-8859-1")

    def _load_b64(self, obj):
        return binascii.a2b_base64(obj.encode("ascii"))

    def _dump_b64(self, binary_data):
        return binascii.b2a_base64(binary_data).rstrip('\n')

    def load(self, obj):
        if self.human_readable:
            return self._load_utf8_codepoints(obj)
        return self._load_b64(obj)

    def dump(self, binary_data):
        if isinstance(binary_data, unicode):
            raise ValueError("Unicode objects are not accepted values for Bytes (%r)" % (binary_data,))
        if self.human_readable:
            return self._dump_utf8_codepoints(binary_data)
        return self._dump_b64(binary_data)


class List(Field):
    def __init__(self, field_type=Text(), nullable=False, **kwargs):
        super(List, self).__init__(**kwargs)
        self.field_type = field_type
        self.nullable = nullable

    def load(self, obj):
        return [self.field_type.load(o) for o in obj]

    def dump(self, obj):
        return [self.field_type.dump(o) for o in obj]


class Enum(Field):
    _field_type = Text()  # don't change

    def __init__(self, values, **kwargs):
        super(Enum, self).__init__(**kwargs)
        self.values = set(values)

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
    def dump(self, obj):
        if not isinstance(obj, (int, type(None))):
            raise ValueError("%r is not a valid Integer" % (obj,))
        return obj

    def load(self, obj):
        return self.dump(obj)


class Boolean(Field):
    VALUE_MAP = {True: '1', 1: '1',
                 False: '0', 0: '0'}

    def dump(self, obj):
        if obj not in self.VALUE_MAP:
            raise ParseError(
                "Invalid value for Boolean field: %r" % obj)
        return bool(obj)

    def load(self, obj):
        return self.dump(obj)


class Float(Field):
    def dump(self, obj):
        return float(obj)

    def load(self, obj):
        return self.dump(obj)


class SubRecord(Field):
    "Field for storing :class:`record.Record`s as fields "
    "in other :class:`record.Record`s"

    def __init__(self, record_class, **kwargs):
        super(SubRecord, self).__init__(**kwargs)
        self._record_class = record_class

    def dump(self, obj):
        if not isinstance(obj, self._record_class):
            raise ParseError("%r is not a %r"
                             % (obj, self._record_class))
        return core.to_json_compatible(obj)

    def load(self, obj):
        return core.from_json_compatible(self._record_class, obj)


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
        return dict([
            (self.key_type.dump(k),
             self.value_type.dump(v))
            for k, v in obj.iteritems()
        ])
