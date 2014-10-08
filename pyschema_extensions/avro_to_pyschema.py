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

"""
Helper functions for converting an Avro schema definition (json) to
a PySchema python source definition.

TODO: Another idea is to read avro schema and create Python classes
dynamically without generating python source code.
"""

field_map = {
    'string': 'pyschema.Text',
    'float': 'pyschema.Float',
    'double': 'pyschema.Float',
    'int': 'pyschema.Integer',
    'boolean': 'pyschema.Boolean',
    'long': 'pyschema.Integer',
}

extra_args_map = {
    'float': 'size=4',
    'int': 'size=4',
}

complex_field_map = {
    'array': 'pyschema.List',
    'map': 'pyschema.Map',
    'record': 'pyschema.SubRecord',
}


def get_first_type(field_type):
    if isinstance(field_type, list):
        field_type = field_type[0]
    return field_type


def get_name(field):
    if isinstance(field['type'], basestring):
        return field['name']
    if isinstance(field['type'], list):
        return field['type'][0]['name']
    if isinstance(field['type'], dict):
        return field['type']['name']


def is_nullable(field_type):
    if isinstance(field_type, list):
        return True
    return False


def get_field_type_name(field_type):
    field_type = get_first_type(field_type)
    if isinstance(field_type, dict):
        field_type = field_type['type']
    return field_type


def nullable_str(field_type):
    if not is_nullable(field_type):
        return 'nullable=False'
    return ''


def get_sub_fields_name(sub_type):
    sub_map = {'record': 'fields', 'array': 'items', 'map': 'values'}
    return sub_map[sub_type]


def get_sub_field(field):
    field_type = get_field_type_name(field['type'])
    if field_type == 'record':
        return field['fields']
    sub_field = field['type'][get_sub_fields_name(field_type)]
    if isinstance(sub_field, list):
        return sub_field[0]
    return sub_field


def get_field_definition(field, sub_records):
    if isinstance(field, basestring):
        if field in field_map.keys():
            return field_map[field] + '()'
        return field
    nullable = 'nullable=False'
    if is_nullable(field['type']):
        nullable = ''
    field_type = get_field_type_name(field['type'])
    # simple types
    if field_type in field_map.keys():
        args = [
            arg for arg in [nullable, extra_args_map.get(field_type, '')]
            if arg
        ]
        return "%s(%s)" % (field_map[field_type], ', '.join(args))

    # complex types
    elif field_type == 'record':
        name = get_name(field)
        sub_rec = get_pyschema_record(field, sub_records)
        sub_records.append(sub_rec)
        return "%s(%s, %s)" % (complex_field_map[field_type], name, nullable)
    elif field_type in complex_field_map.keys():
        sub_field = get_sub_field(field)
        sub_definition = get_field_definition(sub_field, sub_records)
        return "%s(%s, %s)" % (
            complex_field_map[field_type], sub_definition, nullable)


def get_pyschema_record(schema, sub_records):
    name = get_name(schema)

    record_def = "class %s(pyschema.Record):\n" % name
    if is_nullable(schema['type']):
        fields = schema['type'][0]['fields']
    else:
        fields = schema['fields']
    for field in fields:
        name = field['name']
        record_def += "    %s = %s\n" % (
            name, get_field_definition(field, sub_records))
    return record_def
