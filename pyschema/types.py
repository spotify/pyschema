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
import copy
from core import ParseError, Field, auto_store, PySchema
import binascii
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


def ordereddict_push_front(dct, key, value):
    """Set a value at the front of an OrderedDict

    The original dict isn't modified, instead a copy is returned
    """
    d = OrderedDict()
    d[key] = value
    d.update(dct)
    return d


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
                raise ValueError(
                    "%r is not a valid UTF-8 string" % obj
                )


class Bytes(Field):
    """Binary data"""

    def __init__(self, custom_encoding=False, **kwargs):
        super(Bytes, self).__init__(**kwargs)
        self.custom_encoding = custom_encoding

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

    def is_similar_to(self, other):
        return super(Bytes, self).is_similar_to(other) and self.custom_encoding == other.custom_encoding


class List(Field):
    """List of one other Field type

    Differs from other fields in that it is not nullable
    and defaults to empty array instead of null
    """
    def __init__(self, field_type=Text(), nullable=False, default=[], **kwargs):
        super(List, self).__init__(nullable=nullable, default=default, **kwargs)
        self.field_type = field_type

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

    def default_value(self):
        #  avoid default-sharing between records
        return copy.deepcopy(self.default)

    def is_similar_to(self, other):
        return super(List, self).is_similar_to(other) and self.field_type.is_similar_to(other.field_type)

    def repr_vars(self):
        return ordereddict_push_front(
            super(List, self).repr_vars(),
            "field_type",
            repr(self.field_type)
        )


class Enum(Field):
    _field_type = Text()  # don't change

    def __init__(self, values, name=None, **kwargs):
        super(Enum, self).__init__(**kwargs)
        self.values = set(values)
        self.name = name

        if name is not None and PySchema.auto_register:
            auto_store.add_enum(self)

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

    def is_similar_to(self, other):
        return super(Enum, self).is_similar_to(other) and self.values == other.values

    def repr_vars(self):
        return OrderedDict([
            ("values", self.values),
            ("name", repr(self.name))
        ] + super(Enum, self).repr_vars().items()
        )


class Integer(Field):
    def __init__(self, size=8, **kwargs):
        super(Integer, self).__init__(**kwargs)
        self.size = size

    def dump(self, obj):
        if not isinstance(obj, (int, long, type(None))) or isinstance(obj, bool):
            raise ValueError("%r is not a valid Integer" % (obj,))
        return obj

    def load(self, obj):
        if not isinstance(obj, (int, long, type(None))) or isinstance(obj, bool):
            raise ParseError("%r is not a valid Integer" % (obj,))
        return obj

    def is_similar_to(self, other):
        return super(Integer, self).is_similar_to(other) and self.size == other.size

    def repr_vars(self):
        return ordereddict_push_front(
            super(Integer, self).repr_vars(),
            "size",
            self.size
        )


class Boolean(Field):
    VALUE_MAP = {True: '1', 1: '1',
                 False: '0', 0: '0'}

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
    def __init__(self, size=8, **kwargs):
        super(Float, self).__init__(**kwargs)
        self.size = size

    def dump(self, obj):
        if not isinstance(obj, float):
            raise ValueError("Invalid value for Float field: %r" % obj)
        return float(obj)

    def load(self, obj):
        if not isinstance(obj, (float, int, long)):
            raise ParseError("Invalid value for Float field: %r" % obj)
        return float(obj)

    def is_similar_to(self, other):
        return super(Float, self).is_similar_to(other) and self.size == other.size


class Date(Text):
    def dump(self, obj):
        if not isinstance(obj, datetime.date):
            raise ValueError("Invalid value for Date field: %r" % obj)
        return str(obj)

    def load(self, obj):
        try:
            # This is much faster than calling strptime
            (year, month, day) = obj.split('-')
            return datetime.date(int(year), int(month), int(day))
        except ValueError:
            raise ValueError("Invalid value for Date field: %r" % obj)


class DateTime(Text):
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


# special value for SubRecord's schema parameter
# that signifies a SubRecord accepting records of the
# same type as the container/parent Record.
SELF = object()


class SubRecord(Field):
    """"Field for storing other :class:`record.Record`s"""

    def __init__(self, schema, **kwargs):
        super(SubRecord, self).__init__(**kwargs)
        self._schema = schema

    def dump(self, obj):
        if not isinstance(obj, self._schema):
            raise ValueError("%r is not a %r"
                             % (obj, self._schema))
        return core.to_json_compatible(obj)

    def load(self, obj):
        return core.from_json_compatible(self._schema, obj)

    def set_parent(self, schema):
        """This method gets called by the metaclass
        once the container class has been created
        to let the field store a reference to its
        parent if needed. Its needed for SubRecords
        in case it refers to the container record.
        """
        if self._schema == SELF:
            self._schema = schema

    def default_value(self):
        #  avoid default-sharing between records
        return copy.deepcopy(self.default)

    def is_similar_to(self, other):
        return super(SubRecord, self).is_similar_to(other) and self._schema == other._schema

    def repr_vars(self):
        return ordereddict_push_front(
            super(SubRecord, self).repr_vars(),
            "schema",
            self._schema._schema_name
        )


class Map(Field):
    """List of one other Field type

    Differs from other fields in that it is not nullable
    and defaults to empty array instead of null
    """
    def __init__(self, value_type, nullable=False, default={}, **kwargs):
        super(Map, self).__init__(nullable=nullable, default=default, **kwargs)
        self.value_type = value_type
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

    def default_value(self):
        #  avoid default-sharing between records
        return copy.deepcopy(self.default)

    def is_similar_to(self, other):
        return super(Map, self).is_similar_to(other) and self.value_type.is_similar_to(other.value_type)

    def repr_vars(self):
        return ordereddict_push_front(
            super(Map, self).repr_vars(),
            "value_type",
            repr(self.value_type)
        )
