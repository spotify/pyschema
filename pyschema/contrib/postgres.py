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

from pyschema.types import Integer, Text


@Integer.mixin
@property
def pg_type(self):
    return "INT"


@Text.mixin
@property
def pg_type(self):
    return "TEXT"


def types(record_class):
    for name, field_type in record_class._schema:
        yield (name, field_type.pg_type)


def _create_statement(table_name, types):
    parts = []
    for fielddef in types:
        parts.append("%s %s" % fielddef)
    coldefs = ", ".join(parts)
    return "CREATE TABLE %s (" % (table_name,) + coldefs + ")"


def create_statement(record_class):
    return _create_statement(
        record_class._record_name,
        types(record_class)
    )
