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

"""Postgres style SQL generation based on pyschemas

Quite incomplete and still a work in progress
"""

import re

from pyschema.types import Integer, Text, Float, Boolean, Date, DateTime


Integer.pg_type = "BIGINT"
Text.pg_type = "TEXT"
Float.pg_type = "FLOAT"
Boolean.pg_type = "BOOLEAN"
Date.pg_type = "DATE"
DateTime.pg_type = "TIMESTAMP WITHOUT TIME ZONE"


def camel_case_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def types(schema):
    all_types = []
    for name, field_type in schema._fields.iteritems():
        all_types.append((name, field_type.pg_type))
    return all_types


def _create_statement(table_name, types):
    parts = []
    for fielddef in types:
        parts.append("%s %s" % fielddef)
    coldefs = ", ".join(parts)
    return "CREATE TABLE %s (" % (table_name,) + coldefs + ")"


def create_statement(schema, table_name=None):
    table_name = table_name or camel_case_to_underscore(
        schema._schema_name)
    return _create_statement(
        table_name,
        types(schema)
    )
