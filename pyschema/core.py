# Copyright (c) 2012 Spotify AB
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


""" Schema definition toolkit using Python classes

Example:

class Foo(Record):
    bin = Blob()


class MyRecord(Record):
    a_string = String()
    a_float = Float()
    record = List(SubRecord(Foo))


rec = MyRecord(a_string="hej")
rec.record = [Foo(bin="bar")]

s = dumps(rec)
print loads(s)

"""
from abc import ABCMeta, abstractmethod
from itertools import izip
try:
    import simplejson as json
except:
    import json
import warnings


class ParseException(Exception):
    """ Generic exception type for Record parse errors """
    pass


class RecordStore(object):
    def __init__(self):
        self._recordmap = {}

    def __str__(self):
        return str(self._recordmap.keys())

    def add_record(self, record_class):
        """ Add record class to record store for retrieval at record load time.

            Can be used as a class decorator
        """
        existing = self._recordmap.get(record_class.__name__, None)
        if existing:
            warnings.warn("%s replacing previous definition in %s"
                          % (record_class.__name__, existing.__module__))

        self._recordmap[record_class.__name__] = record_class
        return record_class

    def remove_record(self, record_class):
        del self._recordmap[record_class.__name__]

    def get(self, record_name):
        return self._recordmap[record_name]

    def clear(self):
        self._recordmap.clear()

    def clone(self):
        r = RecordStore()
        r._recordmap = self._recordmap.copy()
        return r

    def __contains__(self, record_class):
        return record_class in self._recordmap.values()


class Field(object):
    __metaclass__ = ABCMeta
    _next_index = 0

    def __init__(self, description=None):
        self.description = description
        self._index = Field._next_index
        Field._next_index += 1  # used for arg order in initialization

    def __repr__(self):
        return self.__class__.__name__

    @abstractmethod
    def dump(self, obj):
        pass

    @abstractmethod
    def load(self, obj):
        pass


auto_store = RecordStore()


class RecordMetaclass(ABCMeta):
    """Metaclass for Records

    Builds schema on Record declaration and remembers Record types
    for easy generic parsing
    """
    auto_register = True

    def __new__(metacls, name, bases, dct):
        base_schema = []
        for b in bases:
            try:
                base_schema += b._schema
            except AttributeError:
                pass

        schema = []
        for field_name, value in dct.iteritems():
            if isinstance(value, Field):
                schema.append((field_name, value))

        schema.sort(key=lambda x: x[1]._index)
        schema = base_schema + schema
        dct["_schema"] = schema
        dct["_fields"] = dict(schema)
        dct["_record_name"] = name

        cls = ABCMeta.__new__(metacls, name, bases, dct)
        if metacls.auto_register:
            auto_store.add_record(cls)
        return cls


def disable_auto_register():
    RecordMetaclass.auto_register = False


def enable_auto_register():
    RecordMetaclass.auto_register = True


def no_auto_store():
    """ Decorator factory used to temporarily disable automatic registration of records in the auto_store

    >>> @no_auto_store()
    >>> def MyRecord(Record)
    >>>    pass

    >>> MyRecord in auto_store
    False

    """
    original_auto_register = RecordMetaclass.auto_register
    disable_auto_register()

    def decorator(cls):
        RecordMetaclass.auto_register = original_auto_register
        return cls
    return decorator


@no_auto_store()
class Record(object):
    """Abstract base class for structured logging records"""
    __metaclass__ = RecordMetaclass

    def __init__(self, *args, **kwargs):
        if args:
            # TODO: allow this...?
            raise TypeError('Non-keyword arguments not allowed'
                            ' when initializing Records')
        for k in self._fields:  # None-defualt everything
            object.__setattr__(self, k, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if name not in self._fields:
            raise AttributeError(
                "No field %r in %s"
                % (name, self._record_name)
            )

        super(Record, self).__setattr__(name, value)

    def __unicode__(self):
        return str(self).decode('ascii')

    def __str__(self):
        return repr(self)

    def __repr__(self):
        schema = self._schema
        strings = ('%s=%r' % (fname, getattr(self, fname))
                   for fname, f in schema)

        return self._record_name + '(' + ', '.join(strings) + ')'

    def __cmp__(self, other):
        if self._record_name != other._record_name:
            return cmp(self._record_name, other._record_name)
        fields = list(self._fields)
        a = (getattr(self, key) for key in fields)
        b = (getattr(other, key) for key in fields)

        for _a, _b in izip(a, b):
            r = cmp(_a, _b)
            if r:
                return r
        return 0

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    # TODO: refactor so Record and Fields themselves are serialization-agnostic
    def _to_json_compatible(self):
        "Dump record in json-encodable object format"
        d = {}
        for fname, f in self._schema:
            val = getattr(self, fname)
            if val is not None:
                d[fname] = f.dump(val)
        return d

    @classmethod
    def _from_json_compatible(cls, dct):
        "Load from json-encodable"
        kwargs = {}
        schema = cls._schema
        for field_name, field_type in schema:
            if field_name in dct:
                kwargs[field_name] = field_type.load(dct[field_name])

        return cls(**kwargs)


def _load_json_dct(dct, record_store=None):
    if record_store is None:
        record_store = auto_store
    record_name = dct.pop("$record_name")
    try:
        record_class = record_store.get(record_name)
    except KeyError:
        raise ParseException(
            "Can't recognize record type %r"
            % (record_name,), record_name)

    record = record_class._from_json_compatible(dct)
    return record


def loads(line, record_store=None):
    if not isinstance(line, unicode):
        line = line.decode('utf8')
    if line.startswith(u"{"):
        json_dct = json.loads(line)
        return _load_json_dct(json_dct, record_store)
    else:
        raise ParseException("Not a json record")


def dumps(obj):
    json_dct = obj._to_json_compatible()
    json_dct["$record_name"] = obj._record_name
    json_string = json.dumps(json_dct)
    return json_string
