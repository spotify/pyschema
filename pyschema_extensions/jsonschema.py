# Copyright (c) 2014 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the 'License'); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
'''
Extension for generating JSON schema schemas from PySchema classes

JSON schema: http://json-schema.org/

When dumping to JSON schema, all fields in a record are mandatory, although
a list or map can be empty (but must be present). These records are still
dump-able, but they will not validate against the schema.

Usage:

>>> class MyRecord(pyschema.Record):
>>>     foo = Text()
>>>     bar = Integer()
>>>
>>> [pyschema_extensions.jsonschema.]get_root_schema_string(MyRecord)

'{"additionalProperties": false, "required": ["bar", "foo"], "type": "object", "id": "MyRecord", "properties": {"foo": {"t
ype": "string"}, "bar": {"type": "integer"}}}  '
'''

from pyschema import core
from pyschema.types import Field, Boolean, Integer, Float
from pyschema.types import Text, Enum, List, Map, SubRecord
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
try:
    import simplejson as json
except ImportError:
    import json


# Bytes are not supported
Boolean.jsonschema_type_name = 'boolean'
Integer.jsonschema_type_name = 'integer'
Float.jsonschema_type_name = 'number'
Text.jsonschema_type_name = 'string'
Enum.jsonschema_type_name = 'string'
List.jsonschema_type_name = 'array'
Map.avro_type_name = 'object'


@Field.mixin
class FieldMixin:
    def jsonschema_type_schema(self, state):
        return {
            'type': self.jsonschema_type_name,
        }


@Enum.mixin
class EnumMixin:
    def jsonschema_type_schema(self, state):
        return {
            'type': self.jsonschema_type_name,
            'enum': sorted(list(self.values)),
        }


@List.mixin
class ListMixin:
    def jsonschema_type_schema(self, state):
        return {
            'type': self.jsonschema_type_name,
            'items': self.field_type.jsonschema_type_schema(state),
        }


@Map.mixin
class MapMixin:
    def jsonschema_type_schema(self, state):
        return {
            'type': 'object',
            'additionalProperties': True,
            'patternProperties': {
                '^.*$': self.value_type.jsonschema_type_schema(state),
            },
        }


@SubRecord.mixin
class SubRecordMixin:
    def jsonschema_type_schema(self, state):
        cls = self._schema
        state.record_schemas[cls._schema_name] = get_schema_dict(cls, state)
        return {
            '$ref': self.jsonschema_type_name,
        }

    @property
    def jsonschema_type_name(self):
        return '#/definitions/{0}'.format(self._schema._schema_name)


# Schema generation
class SchemaGeneratorState(object):
    def __init__(self):
        self.record_schemas = dict()


def get_schema_dict(record, state=None):
    """Return a python dict representing the jsonschema of a record

    Any references to sub-schemas will be URI fragments that won't be
    resolvable without a root schema, available from get_root_schema_dict.
    """
    state = state or SchemaGeneratorState()
    schema = OrderedDict([
        ('type', 'object'),
        ('id', record._schema_name),
    ])
    fields = dict()
    for field_name, field_type in record._fields.iteritems():
        fields[field_name] = field_type.jsonschema_type_schema(state)

    required = set(fields.keys())
    schema['properties'] = fields
    schema['required'] = sorted(list(required))
    schema['additionalProperties'] = False

    state.record_schemas[record._schema_name] = schema
    return schema


def get_root_schema_dict(record):
    """Return a root jsonschema for a given record

    A root schema includes the $schema attribute and all sub-record
    schemas and definitions.
    """
    state = SchemaGeneratorState()
    schema = get_schema_dict(record, state)
    del state.record_schemas[record._schema_name]
    if state.record_schemas:
        schema['definitions'] = dict()
        for name, sub_schema in state.record_schemas.iteritems():
            schema['definitions'][name] = sub_schema
    return schema


def get_root_schema_string(record):
    return json.dumps(get_root_schema_dict(record))


def dumps(record):
    return json.dumps(core.to_json_compatible(record))


def loads(s, record_store=None, schema=None):
    return core.loads(s, record_store, schema, core.from_json_compatible)
