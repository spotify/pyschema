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

from pyschema.types import Boolean, Integer, Float, Blob, Text, Enum, List
import simplejson as json


Boolean._avro_spec = {"type": "boolean"}
Integer._avro_spec = {"type": "long"}
Float._avro_spec = {"type": "double"}
Blob._avro_spec = {"type": "bytes"}
Text._avro_spec = {"type": "string"}


@List.mixin
@property
def _avro_spec(self):
    field_spec = self.field_type._avro_spec
    # currently only supports simple types in arrays...
    assert len(field_spec) == 1 and isinstance(field_spec["type"], basestring)
    return {
        "type": "array",
        "items": field_spec["type"]
    }


@Enum.mixin
@property
def _avro_spec(self):
    return {
        "type": "enum",
        "symbols": list(self.values)
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
        field_spec.update(field_type._avro_spec)
        avro_fields.append(field_spec)

    avro_record["fields"] = avro_fields
    return avro_record


def get_schema_string(record):
    return json.dumps(get_schema_dict(record))
