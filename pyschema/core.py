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

Usage example:

>>> class Foo(Record):
... bin = Bytes()
...
... class MyRecord(Record):
...     a_string = Text()
...     a_float = Float()
...     record = List(SubRecord(Foo))
...
... rec = MyRecord(a_string="hej")
... rec.record = [Foo(bin="bar")]
...
... s = dumps(rec)
... print loads(s)


Internals:

A valid PySchema class contains the following class variables:

`_fields`
    An OrderedDict of `field_name` => `field_type`
    where `field_type` is an instance of a Field subclass

`_schema_name`
    The qualifying name for this schema. This is used for registering a record
    in a `SchemaStore` and for auto-identification of serialized records.
    Should be unique within a specific SchemaStore, so if auto registering is
    used it should be unique within the execution chain of the current program.

"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
from itertools import izip

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

import warnings
import types
try:
    import simplejson as json
except ImportError:
    import json


SCHEMA_FIELD_NAME = "$schema"


def set_schema_name_field(name):
    global SCHEMA_FIELD_NAME
    SCHEMA_FIELD_NAME = name


class ParseError(Exception):
    """ Generic exception type for Record parse errors """
    pass


class SchemaStore(object):
    def __init__(self):
        self._schema_map = {}

    def __str__(self):
        return str(self._schema_map.keys())

    def add_record(self, schema, _bump_stack_level=False):
        """ Add record class to record store for retrieval at record load time.

            Can be used as a class decorator
        """
        existing = self._schema_map.get(schema.__name__, None)
        if existing:
            warnings.warn(
                "{new_module}.{class_name} replaces record from {prev_module}"
                .format(class_name=schema.__name__,
                        prev_module=existing.__module__,
                        new_module=schema.__module__),
                stacklevel=3 if _bump_stack_level else 2)

        self._schema_map[schema.__name__] = schema
        return schema

    def remove_record(self, schema):
        del self._schema_map[schema.__name__]

    def get(self, record_name):
        return self._schema_map[record_name]

    def clear(self):
        self._schema_map.clear()

    def clone(self):
        r = SchemaStore()
        r._schema_map = self._schema_map.copy()
        return r

    def __contains__(self, schema):
        return schema in self._schema_map.values()


# NO_DEFAULT is a special value to signify that a field has no default value
# and should fail to serialize unless a value has been assigned
# it's the default default-value for all non-nullable fields
NO_DEFAULT = object()

_UNTOUCHED = object()


class Field(object):
    __metaclass__ = ABCMeta
    _next_index = 0

    def __init__(self, description=None, nullable=True, default=_UNTOUCHED):
        self.description = description
        self._index = Field._next_index
        self.nullable = nullable
        if default is _UNTOUCHED:
            # if default isn't explicitly set
            # use None for Nullables, and NO_DEFAULT for others
            if nullable:
                default = None
            else:
                default = NO_DEFAULT

        self.default = default
        Field._next_index += 1  # used for arg order in initialization

    def __repr__(self):
        return self.__class__.__name__

    def set_parent(self, schema):
        # no-op by default but can be overridden by types
        # that need parent references
        pass

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

        >>> @Integer.mixin
        ... class IntegerPostgresExtensions:
        ...     postgres_type = 'INT'
        ...
        ...     def postgres_dump(self, obj):
        ...         self.dump(obj) + "::integer"

        Is roughly equivalent to:

        >>> Integer.postgres_type = 'INT'
        ...
        ... def postgres_dump(self, obj):
        ...     self.dump(obj) + "::integer"
        ...
        ... Integer.postgres_dump = postgres_dump

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

    def default_value(self):
        return self.default

auto_store = SchemaStore()


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

        # allow self-references etc.
        for field_name, field in cls._fields.iteritems():
            field.set_parent(cls)

        if metacls.auto_register:
            auto_store.add_record(cls, _bump_stack_level=True)
        return cls

    @classmethod
    def _field_dupe_warning(metacls, name, fields):
        warnings.warn(
            "{schema}: Duplicate field definition for field{plural} {field}"
                .format(
                    schema=name,
                    field=fields,
                    plural="s" if len(fields) > 1 else ""
                ),
            stacklevel=4
        )

    @classmethod
    def _get_schema_attributes(metacls, name, bases, dct):
        fields = OrderedDict()
        for b in bases:
            if not isinstance(b, metacls):
                continue

            field_intersection = set(fields) & set(b._fields)
            if field_intersection:
                metacls._field_dupe_warning(name, field_intersection)
            fields.update(b._fields)

        new_fields = []
        for field_name, field_def in dct.iteritems():
            if isinstance(field_def, Field):
                new_fields.append((field_name, field_def))

        new_fields.sort(key=lambda fd: fd[1]._index)
        for field_name, field_def in new_fields:
            if field_name in fields:
                metacls._field_dupe_warning(name, (field_name,))
            fields[field_name] = field_def

        return {
            "_fields": fields,
            "_schema_name": name,
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

    Decorator factory. This is _NOT_ thread safe

    >>> @no_auto_store()
    ... class BarRecord(Record):
    ...     pass
    >>> BarRecord in auto_store
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
            # The idea behind only allowing keyword arguments
            # is to prevent accidental misuse of a changed schema
            raise TypeError('Non-keyword arguments not allowed'
                            ' when initializing Records')
        for k, field_type in self._fields.iteritems():
            object.__setattr__(self, k, field_type.default_value())
        for k, v in kwargs.iteritems():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if name not in self._fields:
            raise AttributeError(
                "No field %r in %s"
                % (name, self._schema_name)
            )

        super(Record, self).__setattr__(name, value)

    def __unicode__(self):
        return str(self).decode('ascii')

    def __str__(self):
        return repr(self)

    def __repr__(self):
        strings = ('%s=%r' % (fname, getattr(self, fname))
                   for fname, f in self._fields.iteritems())

        return self._schema_name + '(' + ', '.join(strings) + ')'

    def __cmp__(self, other):
        if not isinstance(other, Record):
            # return default implementation cmp value
            return cmp(id(self), other)
        if self._schema_name != other._schema_name:
            return cmp(self._schema_name, other._schema_name)
        fields = self._fields.keys()
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
    for fname, f in record._fields.iteritems():
        val = getattr(record, fname)
        if val is not None:
            d[fname] = f.dump(val)
    return d


def from_json_compatible(schema, dct):
    "Load from json-encodable"
    kwargs = {}

    for key in dct:
        field_type = schema._fields.get(key)
        if field_type is None:
            raise ParseError("Unexpected field encountered in line for record %s: %s" % (schema.__name__, key))
        kwargs[key] = field_type.load(dct[key])

    return schema(**kwargs)


def ispyschema(schema):
    """ Is object PySchema instance?

    Returns true for PySchema Record *classes*
    i.e. NOT when schema is a *Record* instance

    >>> class FooRecord(Record):
    ...     pass
    >>> ispyschema(FooRecord)
    True
    >>> ispyschema(FooRecord())
    False
    """
    return isinstance(schema, PySchema)


def load_json_dct(
        dct,
        record_store=None,
        schema=None,
        loader=from_json_compatible
):
    """ Create a Record instance from a json-compatible dictionary

    The dictionary values should have types that are json compatible,
    as if just loaded from a json serialized record string.

    :param dct:
        Python dictionary with key/value pairs for the record

    :param record_store:
        Record store to use for schema lookups (when $schema field is present)

    :param schema:
        PySchema Record class for the record to load.
        This will override any $schema fields specified in `dct`

    """
    if schema is None:
        if record_store is None:
            record_store = auto_store
        try:
            schema_name = dct.pop(SCHEMA_FIELD_NAME)
        except KeyError:
            raise ParseError((
                "Serialized record missing '{0}' "
                "record identifier and no schema supplied")
                .format(SCHEMA_FIELD_NAME)
            )
        try:
            schema = record_store.get(schema_name)
        except KeyError:
            raise ParseError(
                "Can't recognize record type %r"
                % (schema_name,), schema_name)

    # if schema is explicit, use that instead of SCHEMA_FIELD_NAME
    elif SCHEMA_FIELD_NAME in dct:
        dct.pop(SCHEMA_FIELD_NAME)

    record = loader(schema, dct)
    return record


def loads(
        s,
        record_store=None,
        schema=None,
        loader=from_json_compatible
):
    """ Create a Record instance from a json serialized dictionary

    :param s:
        String with a json-serialized dictionary

    :param record_store:
        Record store to use for schema lookups (when $schema field is present)

    :param schema:
        PySchema Record class for the record to load.
        This will override any $schema fields specified in `s`

    """
    if not isinstance(s, unicode):
        s = s.decode('utf8')
    if s.startswith(u"{"):
        json_dct = json.loads(s)
        return load_json_dct(json_dct, record_store, schema, loader)
    else:
        raise ParseError("Not a json record")


def dumps(obj, attach_schema_name=True):
    json_dct = to_json_compatible(obj)
    if attach_schema_name:
        json_dct[SCHEMA_FIELD_NAME] = obj._schema_name
    json_string = json.dumps(json_dct)
    return json_string
