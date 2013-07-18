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
