# Copyright (c) 2015 Spotify AB
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

import pyschema
from functools import partial
from pyschema_extensions import avro  # import to get avro mixins for fields
assert avro  # silence linter


SIMPLE_FIELD_MAP = {
    "string": pyschema.Text,
    "float": partial(pyschema.Float, size=4),
    "double": pyschema.Float,
    "int": partial(pyschema.Integer, size=4),
    "boolean": pyschema.Boolean,
    "long": pyschema.Integer,
    "bytes": pyschema.Bytes
}


class AVSCParseException(Exception):
    pass


def parse_schema_string(schema_string):
    """
    Load and return a PySchema class from an avsc string
    """
    schema_struct = json.loads(schema_string)
    return AvroSchemaParser().parse_schema_struct(schema_struct)


class AvroSchemaParser(object):

    def __init__(self):
        self.schema_store = pyschema.core.SchemaStore()

    def parse_schema_struct(self, schema_struct, _schemas=None):
        record_name = schema_struct["name"]
        field_dct = {}
        for field_def in schema_struct["fields"]:
            field_name = field_def["name"]
            field_builder = self._get_field_builder(field_def["type"])

            if "default" in field_def:
                if field_def["default"] is not None:
                    default_parser = field_builder(nullable=False).avro_load
                    default_value = default_parser(field_def["default"])
                else:
                    default_value = None
            else:
                default_value = pyschema.core.NO_DEFAULT

            field = field_builder(
                description=field_def.get("doc"),
                default=default_value
            )
            field_dct[field_name] = field
            field_dct["__module__"] = "__avro_parser_runtime__"  # not great, but better than "abc"
            if "namespace" in schema_struct:
                field_dct["_namespace"] = schema_struct["namespace"]

        if "doc" in schema_struct:
            field_dct["__doc__"] = schema_struct["doc"]
        schema = pyschema.core.PySchema(record_name.encode("ascii"), (pyschema.core.Record,), field_dct)
        return schema

    def _get_field_builder(self, type_def_struct):
        if isinstance(type_def_struct, list):
            return self._parse_union(type_def_struct)
        elif isinstance(type_def_struct, dict):
            return self._parse_complex(type_def_struct)
        elif type_def_struct in SIMPLE_FIELD_MAP:
            field_builder = SIMPLE_FIELD_MAP.get(type_def_struct)
            return partial(field_builder, nullable=False)
        else:
            # Default case is that we reference an already defined sub record
            return self._parse_subrecord(type_def_struct)

    def _parse_union(self, union_struct):
        filtered = [subtype for subtype in union_struct if subtype != "null"]
        if len(filtered) > 1:
            raise AVSCParseException(
                "Unions with multiple non-null as type are unsupported: {0}".format(union_struct)
            )
        nullable = "null" in union_struct
        actual_type = filtered[0]
        field_builder = self._get_field_builder(actual_type)

        return partial(field_builder, nullable=nullable)

    def _parse_map(self, type_def_struct):
        values_field_builder = self._get_field_builder(type_def_struct["values"])
        return partial(
            pyschema.Map,
            # NOTE: values field builder is executed here, so the field index
            # will be created at this point, but that shouldn't matter for the map type
            value_type=values_field_builder(),
            nullable=False
        )

    def _parse_subrecord(self, type_def_struct):
        if isinstance(type_def_struct, dict):
            if "fields" not in type_def_struct:
                raise AVSCParseException((
                    "No 'fields' definition found in subrecord"
                    " declaration: {0!r}"
                ).format(type_def_struct))
            schema_class = self.parse_schema_struct(type_def_struct)
            self.schema_store.add_record(schema_class)
        else:
            if not isinstance(type_def_struct, basestring):
                raise AVSCParseException((
                    "Subrecord types need to be either declarations or a "
                    "string referring to another schema, got: {0!r}"
                ).format(type_def_struct))
            try:
                schema_class = self.schema_store.get(type_def_struct)
            except KeyError:
                raise AVSCParseException((
                    "A schema type '{0!r}' was referenced"
                    " without prior declaration."
                ).format(type_def_struct))

        return partial(
            pyschema.SubRecord,
            schema_class,
            nullable=False
        )

    def _parse_array(self, type_def_struct):
        item_type = self._get_field_builder(type_def_struct["items"])()
        return partial(
            pyschema.List,
            item_type,
            nullable=False
        )

    def _parse_enum(self, type_def_struct):
        # copy and make a tuple just to ensure it isn't modified later
        values = tuple(type_def_struct["symbols"])
        return partial(
            pyschema.Enum,
            values,
            nullable=False
        )

    COMPLEX_MAPPING = {
        "map": _parse_map,
        "record": _parse_subrecord,
        "array": _parse_array,
        "enum": _parse_enum
    }

    def _parse_complex(self, type_def_struct):
        typename = type_def_struct["type"]
        parser_func = self.COMPLEX_MAPPING.get(typename)
        if parser_func:
            return parser_func(self, type_def_struct)
        raise AVSCParseException("Unknown complex type: {0}".format(type_def_struct))
