# Copyright (c) 2014 Spotify AB
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

try:
    import simplejson as json
except ImportError:
    import json

from unittest import TestCase
from jsonschema import validate, ValidationError

from pyschema import Record, Text, Integer
from pyschema import Enum, List, SubRecord, Map
from pyschema_extensions import jsonschema


class SimpleRecord(Record):
    alpha = Text()
    beta = Integer()


class EnumRecord(Record):
    gamma = Enum(['foo', 'bar', 'baz'])


class ListRecord(Record):
    delta = List(Text())


class MapRecord(Record):
    epsilon = Map(Integer())


class SubRecordRecord(Record):
    zeta = SubRecord(SimpleRecord)


class TestJsonSchema(TestCase):
    def serialize_validate(self, valid_record):
        """Serialize and validate a record

        The record is dumped to JSON and then loaded into a Python dictionary,
        which is checked against the known schema
        """
        schema = jsonschema.get_root_schema_dict(valid_record)
        record_json = jsonschema.dumps(valid_record)
        record_dict = json.loads(record_json)
        validate(record_dict, schema)

    def test_simple_schema(self):
        expected = {
            'id': 'SimpleRecord',
            'type': 'object',
            'properties': {
                'alpha': {'type': 'string'},
                'beta': {'type': 'integer'}
            },
            'required': ['alpha', 'beta'],
            'additionalProperties': False,
        }
        self.assertEqual(expected, jsonschema.get_root_schema_dict(SimpleRecord))

    def test_simple_serialize(self):
        record = SimpleRecord()
        record.alpha = 'hello'
        record.beta = 14
        self.serialize_validate(record)

    def test_enum_schema(self):
        expected = {
            'id': 'EnumRecord',
            'type': 'object',
            'properties': {
                'gamma': {
                    'type': 'string',
                    'enum': ['bar', 'baz', 'foo']
                },
            },
            'required': ['gamma'],
            'additionalProperties': False,
        }
        self.assertEqual(expected, jsonschema.get_root_schema_dict(EnumRecord))
        schema = jsonschema.get_root_schema_dict(EnumRecord)
        validate({'gamma': 'bar'}, schema)

    def test_enum_serialize(self):
        record = EnumRecord()
        record.gamma = 'bar'
        self.serialize_validate(record)

    def test_list_schema(self):
        expected = {
            'id': 'ListRecord',
            'type': 'object',
            'properties': {
                'delta': {
                    'type': 'array',
                    'items': {
                        'type': 'string',
                    },
                },
            },
            'required': ['delta'],
            'additionalProperties': False,
        }
        self.assertEqual(expected, jsonschema.get_root_schema_dict(ListRecord))
        schema = jsonschema.get_root_schema_dict(ListRecord)
        validate({'delta': ['foo', 'bar', 'baz']}, schema)

    def test_list_serialize(self):
        record = ListRecord()
        record.delta = ['monkey', 'banana']
        self.serialize_validate(record)

    def test_map_schema(self):
        expected = {
            'id': 'MapRecord',
            'type': 'object',
            'properties': {
                'epsilon': {
                    'type': 'object',
                    'additionalProperties': True,
                    'patternProperties': {
                        '^.*$': {
                            'type': 'integer',
                        }
                    }
                },
            },
            'required': ['epsilon'],
            'additionalProperties': False,
        }
        self.assertEqual(expected, jsonschema.get_root_schema_dict(MapRecord))
        schema = jsonschema.get_root_schema_dict(MapRecord)
        validate({'epsilon': {'foo': 14}}, schema)

    def test_map_value_type_restricted(self):
        """Ensure that maps are restricted based on the value type"""
        schema = jsonschema.get_root_schema_dict(MapRecord)
        self.assertRaises(ValidationError, validate, {'epsilon': {'foo': 'bar'}}, schema)

    def test_map_serialize(self):
        record = MapRecord()
        record.epsilon = {
            'foo': 14,
            'bar': 12,
        }
        self.serialize_validate(record)

    def test_subrecord_schema(self):
        expected = {
            'additionalProperties': False,
            'id': 'SubRecordRecord',
            'properties': {
                'zeta': {'$ref': '#/definitions/SimpleRecord'},
                },
            'required': ['zeta'],
            'type': 'object',
            'definitions': {
                'SimpleRecord': {
                    'additionalProperties': False,
                    'id': 'SimpleRecord',
                    'properties': {'alpha': {'type': 'string'},
                                   'beta': {'type': 'integer'}
                    },
                    'required': ['alpha', 'beta'],
                    'type': 'object'}
            },
        }

        self.assertEqual(expected, jsonschema.get_root_schema_dict(SubRecordRecord))
        schema = jsonschema.get_root_schema_dict(SubRecordRecord)
        validate({'zeta': {'alpha': 'foo', 'beta': 14}}, schema)

    def test_subrecord_serialize(self):
        record = SubRecordRecord()
        record.zeta = SimpleRecord()
        record.zeta.alpha = 'foo'
        record.zeta.beta = 14
        self.serialize_validate(record)
