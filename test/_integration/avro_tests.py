# Copyright (c) 2013 Spotify AB
#

import logging
import subprocess
from unittest import TestCase
from pyschema import Record, Text, Integer, Boolean, Bytes
from pyschema import Float, Enum, List, SubRecord, Map
import pyschema
import pyschema_extensions.avro
import os


@pyschema.no_auto_store()
class TextRecord(Record):
    t = Text()


@pyschema.no_auto_store()
class NonnullableTextRecord(Record):
    t = Text(nullable=False)


@pyschema.no_auto_store()
class IntegerRecord(Record):
    i = Integer()


@pyschema.no_auto_store()
class NonnullableIntegerRecord(Record):
    i = Integer(nullable=False)


@pyschema.no_auto_store()
class BooleanRecord(Record):
    b = Boolean()


@pyschema.no_auto_store()
class NonnullableBooleanRecord(Record):
    b = Boolean(nullable=False)


@pyschema.no_auto_store()
class FloatRecord(Record):
    f = Float()


@pyschema.no_auto_store()
class NonnullableFloatRecord(Record):
    f = Float(nullable=False)


@pyschema.no_auto_store()
class BytesRecord(Record):
    b = Bytes()


@pyschema.no_auto_store()
class NonnullableBytesRecord(Record):
    b = Bytes(nullable=False)


@pyschema.no_auto_store()
class EnumRecord(Record):
    e = Enum(["FOO", "BAR"])


@pyschema.no_auto_store()
class NamedEnumRecord(Record):
    e = Enum(["FOO", "BAR"], name="EType")


@pyschema.no_auto_store()
class NonnullableEnumRecord(Record):
    e = Enum(["FOO", "BAR"], nullable=False)


@pyschema.no_auto_store()
class ListRecord(Record):
    l = List(Text())


@pyschema.no_auto_store()
class NullableListRecord(Record):
    nl = List(Text(), nullable=True)


@pyschema.no_auto_store()
class SubRecordRecord(Record):
    r = SubRecord(TextRecord)


@pyschema.no_auto_store()
class NonnullableSubRecordRecord(Record):
    r = SubRecord(TextRecord, nullable=False)


@pyschema.no_auto_store()
class MapRecord(Record):
    m = Map(Integer())


@pyschema.no_auto_store()
class NestedListRecord(Record):
    l = List(SubRecord(TextRecord))


@pyschema.no_auto_store()
class NestedMapRecord(Record):
    m = Map(SubRecord(TextRecord))


@pyschema.no_auto_store()
class RepeatedSubRecordRecord(Record):
    r1 = SubRecord(TextRecord)
    r2 = SubRecord(TextRecord)


# typically, this could be "java -jar /my/path/to/avro-tools-1.7.5.jar"
# but if you have installed avro-tools as a package there might be a command shortcut
avro_tools_cmd = os.environ.get("AVRO_TOOLS_CMD", "avro-tools")


def avro_roundtrip(record_type, record, schema=None, json_record=None):
    schema = schema or pyschema_extensions.avro.get_schema_string(record_type)
    json_record = json_record or pyschema_extensions.avro.dumps(record)

    read_cmd = [avro_tools_cmd, "fragtojson", schema, "-"]
    write_cmd = [avro_tools_cmd, "jsontofrag", schema, "-"]
    writer = subprocess.Popen(
        write_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    )
    reader = subprocess.Popen(
        read_cmd,
        stdin=writer.stdout,
        stdout=subprocess.PIPE
    )
    writer.stdin.write(json_record)
    writer.stdin.close()
    writer_result = writer.wait()

    if writer_result != 0:
        logging.error("Error when writing record")
        logging.error("Schema:")
        logging.error(schema)
        logging.error("Invalid Record:")
        logging.error(json_record)
        return False

    reader_result = reader.wait()
    # fragtojson outputs json without closing braces :/
    # this might cause problems...
    broken_json = reader.stdout.read()
    missing_braces = sum(
        1 if c == '{' else -1
        for c in broken_json
        if c in ('{', '}')
    )
    fixed_json = broken_json + '}' * missing_braces
    if reader_result != 0:
        logging.error("Error when reading back record")
        logging.error("Schema:")
        logging.error(schema)
        logging.error("Invalid Record:")
        logging.error(json_record)
        return False
    try:
        pyschema_extensions.avro.loads(fixed_json, schema=record.__class__)
    except pyschema.ParseError:
        logging.error("Could not parse read-back record:")
        logging.error(fixed_json)
        logging.exception("")
        return False

    return True


class TestExternalAvroValidation(TestCase):
    """Validate the Avro schema against given records"""

    def test_text(self):
        record = TextRecord(t="foo")
        self.assertTrue(avro_roundtrip(TextRecord, record))

    def test_nonnullable_text(self):
        record = NonnullableTextRecord(t="foo")
        self.assertTrue(avro_roundtrip(NonnullableTextRecord, record))

    def test_integer(self):
        record = IntegerRecord(i=17)
        self.assertTrue(avro_roundtrip(IntegerRecord, record))

    def test_nonnullable_integer(self):
        record = NonnullableIntegerRecord(i=17)
        self.assertTrue(avro_roundtrip(NonnullableIntegerRecord, record))

    def test_boolean(self):
        record = BooleanRecord(b=True)
        self.assertTrue(avro_roundtrip(BooleanRecord, record))

    def test_nonnullable_boolean(self):
        record = NonnullableBooleanRecord(b=True)
        self.assertTrue(avro_roundtrip(NonnullableBooleanRecord, record))

    def test_float(self):
        record = FloatRecord(f=0.1)
        self.assertTrue(avro_roundtrip(FloatRecord, record))

    def test_nonnullable_float(self):
        record = NonnullableFloatRecord(f=0.1)
        self.assertTrue(avro_roundtrip(NonnullableFloatRecord, record))

    def test_byte(self):
        record = BytesRecord(b=b"12345")
        self.assertTrue(avro_roundtrip(BytesRecord, record))

    def test_nonnullable_byte(self):
        record = NonnullableBytesRecord(b=b"12345")
        self.assertTrue(avro_roundtrip(NonnullableBytesRecord, record))

    def test_enum(self):
        record = EnumRecord(e="FOO")
        self.assertTrue(avro_roundtrip(EnumRecord, record))

    def test_named_enum(self):
        record = NamedEnumRecord(e="FOO")
        self.assertTrue(avro_roundtrip(NamedEnumRecord, record))

    def test_nonnullable_enum(self):
        record = NonnullableEnumRecord(e="FOO")
        self.assertTrue(avro_roundtrip(NonnullableEnumRecord, record))

    def test_list(self):
        record = ListRecord(l=["foo", "bar", "baz"])
        self.assertTrue(avro_roundtrip(ListRecord, record))

    def test_nullable_list_not_null(self):
        record = NullableListRecord(nl=["foo", "bar", "baz"])
        self.assertTrue(avro_roundtrip(NullableListRecord, record))

    def test_nullable_list_null(self):
        record = NullableListRecord(nl=None)
        self.assertTrue(avro_roundtrip(NullableListRecord, record))

    def test_subrecord(self):
        record = SubRecordRecord(r=TextRecord(t="foo"))
        self.assertTrue(avro_roundtrip(SubRecordRecord, record))

    def test_nonnullable_subrecord(self):
        record = NonnullableSubRecordRecord(r=TextRecord(t="foo"))
        self.assertTrue(avro_roundtrip(NonnullableSubRecordRecord, record))

    def test_nested_list(self):
        record = NestedListRecord(l=[
            TextRecord(t="foo"),
            TextRecord(t="bar"),
            TextRecord(t="baz"),
        ])
        self.assertTrue(avro_roundtrip(NestedListRecord, record))

    def test_map(self):
        record = MapRecord(m={"foo": 4, "baz": 27})
        self.assertTrue(avro_roundtrip(MapRecord, record))

    def test_nested_map(self):
        record = NestedMapRecord(m={
            "foo": TextRecord(t="bar"),
            "baz": TextRecord(t="qux"),
        })
        self.assertTrue(avro_roundtrip(NestedMapRecord, record))

    def test_repeated_subrecord(self):
        record = RepeatedSubRecordRecord(
            r1=TextRecord(t="foo"),
            r2=TextRecord(t="bar")
        )
        self.assertTrue(avro_roundtrip(RepeatedSubRecordRecord, record))
