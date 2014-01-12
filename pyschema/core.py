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


""" Schema definition toolkit using Python classes

Example:

class Foo(Record):
    bin = Bytes()


class MyRecord(Record):
    a_string = Text()
    a_float = Float()
    record = List(SubRecord(Foo))


rec = MyRecord(a_string="hej")
rec.record = [Foo(bin="bar")]

s = dumps(rec)
print loads(s)


Internals:

A valid PySchema class is required to contain the following class variables:

_schema:
    A list of (`field_name`, `field_type`) tuples.
    `field_type` should be an instance of a Field subclass

_record_name:
    The qualifying name for this schema. This is used for registering a record
    in a `RecordStore` and for auto-identification of serialized records.
    Should be unique within a specific RecordStore, so if auto registering is
    used it should be unique within the execution chain of the current program.

_field_names:
    A set with the attribute names (the keys of _schema)

"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
from itertools import izip
try:
    import simplejson as json
except:
    import json
import warnings
import types


class ParseError(Exception):
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

    @classmethod
    def mixin(cls, mixin_cls):
        """Decorator for mixing in additional functionality into field type

        Example:

        @Integer.mixin
        class IntegerPostgresExtensions:
            postgres_type = 'INT'

            def postgres_dump(self, obj):
                self.dump(obj) + "::integer"

        Is roughly equivalent to:

        Integer.postgres_type = 'INT'

        def postgres_dump(self, obj):
            self.dump(obj) + "::integer"

        Integer.postgres_dump = postgres_dump

        """
        for item_name in dir(mixin_cls):
            if item_name.startswith("__"):
                # don't copy magic properties
                continue
            item = getattr(mixin_cls, item_name)

            if isinstance(item, types.MethodType):
                # unbound method will cause problems
                # so get the underlying function instead
                item = item.im_func

            setattr(cls, item_name, item)
        return mixin_cls


auto_store = RecordStore()


class PySchema(ABCMeta):
    """Metaclass for Records

    Builds schema on Record declaration and remembers Record types
    for easy generic parsing
    """
    auto_register = True

    def __new__(metacls, name, bases, dct):
        schema_attrs = metacls._get_schema_attributes(
            name=name,
            bases=bases,
            dct=dct
        )
        dct.update(schema_attrs)
        cls = ABCMeta.__new__(metacls, name, bases, dct)
        if metacls.auto_register:
            auto_store.add_record(cls)
        return cls

    @classmethod
    def _get_schema_attributes(metacls, name, bases, dct):
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
        return {
            "_schema": schema,
            "_record_name": name,
            "_field_names": set(x for x, y in schema)
        }

    @classmethod
    def from_class(metacls, cls, auto_store=True):
        """Create proper PySchema class from cls

        Any methods and attributes will be transferred to the
        new object
        """
        if auto_store:
            def wrap(cls):
                return cls
        else:
            wrap = no_auto_store()

        return wrap(metacls.__new__(
            metacls,
            cls.__name__,
            (Record,),
            dict(cls.__dict__)
        ))


def disable_auto_register():
    PySchema.auto_register = False


def enable_auto_register():
    PySchema.auto_register = True


def no_auto_store():
    """ Temporarily disable automatic registration of records in the auto_store

    Decorator factory.

    >>> @no_auto_store()
    >>> def MyRecord(Record)
    >>>    pass

    >>> MyRecord in auto_store
    False

    """
    original_auto_register_value = PySchema.auto_register
    disable_auto_register()

    def decorator(cls):
        PySchema.auto_register = original_auto_register_value
        return cls
    return decorator


@no_auto_store()
class Record(object):
    """Abstract base class for structured logging records"""
    __metaclass__ = PySchema

    def __init__(self, *args, **kwargs):
        if args:
            # TODO: allow this...?
            # The idea behind only allowing keyword arguments
            # is to prevent accidental misuse of a changed schema
            raise TypeError('Non-keyword arguments not allowed'
                            ' when initializing Records')
        for k, _ in self._schema:  # None-defualt everything
            object.__setattr__(self, k, None)
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if name not in self._field_names:
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
        fields = [x for x, _ in self._schema]
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


def to_json_compatible(record):
    "Dump record in json-encodable object format"
    d = {}
    for fname, f in record._schema:
        val = getattr(record, fname)
        if val is not None:
            d[fname] = f.dump(val)
    return d


def from_json_compatible(record_class, dct):
    "Load from json-encodable"
    kwargs = {}
    schema = record_class._schema
    for field_name, field_type in schema:
        if field_name in dct:
            kwargs[field_name] = field_type.load(dct[field_name])

    return record_class(**kwargs)


def ispyschema(schema):
    """ Is object PySchema instance?

    Returns true for PySchema Record *classes*
    i.e. NOT when schema is a *Record* instance

    class MyRecord(Record):
        pass

    >>> ispyschema(MyRecord)
    True
    >>> ispyschema(MyRecord())
    False
    """
    return isinstance(schema, PySchema)


def load_json_dct(dct, record_store=None, record_class=None, loader=from_json_compatible):
    """ Create a Record instance from a json-compatible dictionary

    The dictionary values should have types that are json compatible, as if just loaded from a json serialized record string.

    :param dct:
    Python dictionary with key/value pairs for the record

    :param record_store:
    Record store to use for schema lookups (when $record_name field is present)

    :param record_class:
    PySchema Record class for the record to load. This will override any $record_name fields specified in `dct`

    """
    if record_class is None:
        if record_store is None:
            record_store = auto_store
        try:
            record_name = dct.pop("$record_name")
        except KeyError:
            raise ParseError(
                "Serialized record missing '$record_name' record identifier and no record_class supplied"
            )
        try:
            record_class = record_store.get(record_name)
        except KeyError:
            raise ParseError(
                "Can't recognize record type %r"
                % (record_name,), record_name)

    record = loader(record_class, dct)
    return record


def loads(s, record_store=None, record_class=None, loader=from_json_compatible):
    """ Create a Record instance from a json serialized dictionary

    :param s:
    String with a json-serialized dictionary

    :param record_store:
    Record store to use for schema lookups (when $record_name field is present)

    :param record_class:
    PySchema Record class for the record to load. This will override any $record_name fields specified in `s`

    """
    if not isinstance(s, unicode):
        s = s.decode('utf8')
    if s.startswith(u"{"):
        json_dct = json.loads(s)
        return load_json_dct(json_dct, record_store, record_class, loader)
    else:
        raise ParseError("Not a json record")


def dumps(obj, attach_record_name=True):
    json_dct = to_json_compatible(obj)
    if attach_record_name:
        json_dct["$record_name"] = obj._record_name
    json_string = json.dumps(json_dct)
    return json_string
