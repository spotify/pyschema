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
"""
Extension for generating Avro schemas from PySchema Record classes

Usage:

>>> class MyRecord(pyschema.Record):
>>>     foo = Text()
>>>     bar = Integer()
>>>
>>> [pyschema.contrib.avro.]get_schema_string(MyRecord)

'{"fields": [{"type": "string", "name": "foo"},
{"type": "long", "name": "bar"}],
"type": "record", "name": "MyRecord"}'

"""

from pyschema.types import Field, Boolean, Integer, Float, Bytes, Text, Enum, List
import simplejson as json


Boolean._avro_type = "boolean"
Integer._avro_type = "long"
Float._avro_type = "double"
Bytes._avro_type = "bytes"
Text._avro_type = "string"


@Field.mixin
def _avro_spec(self):
    return {"type": [self._avro_type, "null"]}


@Field.mixin
def avro_json(self, s):
    return {self._avro_type: self.dump(s)}


@List.mixin
def _avro_spec(self):
    # TODO: support complex types
    field_avro_type = self.field_type._avro_type
    return {
        "type": {
            "type": "array",
            "items": field_avro_type
        }  # don't allow None in list types, use empty lists instead
    }


@Enum.mixin
def _avro_spec(self):
    return {
        "type": [
            {
                "type": "enum",
                "name": "ENUM",  # FIXME: don't know what to do with this, but enum name is required in avro
                "symbols": list(self.values)
            },
            "null"
        ]
    }


def get_schema_dict(record):
    avro_record = {
        "type": "record",
        "name": record._record_name,
    }
    avro_fields = []
    for field_name, field_type in record._schema:
        field_spec = {
            "name": field_name,
        }
        field_spec.update(field_type._avro_spec())
        avro_fields.append(field_spec)

    avro_record["fields"] = avro_fields
    return avro_record


def get_schema_string(record):
    return json.dumps(get_schema_dict(record))
