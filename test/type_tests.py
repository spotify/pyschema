from unittest import TestCase
from pyschema.types import *
import pyschema


class TypeTests(TestCase):
    def setUp(self):
        self.loader = pyschema.loads
        self.dumper = pyschema.dumps

    def assertValue(self, record_cls, input_value, output_value):
        rec = record_cls(field=input_value)
        self.assertEquals(rec.field, input_value)
        serialized = self.dumper(rec)
        deserialized = self.loader(serialized, schema=record_cls)
        self.assertEquals(deserialized.field, output_value)

    def assertCompliant(self, record_cls, allowed_values, forbidden_values):
        for value in allowed_values:
            self.assertValue(record_cls, value, value)

        for value in forbidden_values:
            rec = record_cls(field=value)
            self.assertEquals(rec.field, value)

            def dump_fail():
                str(self.dumper(rec))

            self.assertRaises(
                ValueError,
                dump_fail
            )

    def test_text(self):
        @pyschema.no_auto_store()
        class TextRecord(pyschema.Record):
            field = Text()
        allowed = [u"foo", "bar"]
        forbidden = [100, False]
        self.assertCompliant(TextRecord, allowed, forbidden)

    def test_integer(self):
        @pyschema.no_auto_store()
        class IntegerRecord(pyschema.Record):
            field = Integer()
        allowed = [100, 2**32-1, -(2**32), 1L]
        forbidden = ["1", 0.12, False, True]
        self.assertCompliant(IntegerRecord, allowed, forbidden)

    def test_boolean(self):
        @pyschema.no_auto_store()
        class BooleanRecord(pyschema.Record):
            field = Boolean()
        allowed = [True, False, 0, 1]
        forbidden = ["True", "t", "f", "False", 2, -1]
        self.assertCompliant(BooleanRecord, allowed, forbidden)

    def test_float(self):
        @pyschema.no_auto_store()
        class FloatRecord(pyschema.Record):
            field = Float()
        allowed = [0.1, -0.5, 1.1e5, 2.0]
        forbidden = ["0.1", "0.123", True, False]
        self.assertCompliant(FloatRecord, allowed, forbidden)

    def test_date(self):
        @pyschema.no_auto_store()
        class DateRecord(pyschema.Record):
            field = Date()
        allowed = [datetime.date(2014,4,20)]
        forbidden = ['2014-02-02', 1201212121.0]
        self.assertCompliant(DateRecord, allowed, forbidden)

    def test_datetime(self):
        @pyschema.no_auto_store()
        class DateTimeRecord(pyschema.Record):
            field = DateTime()
        allowed = [datetime.datetime(2014,4,20,12,0,0), datetime.datetime(2012,1,1,23,3,3,12345)]
        forbidden = ['2014-02-02 12:00:00', 1201212121.0]
        self.assertCompliant(DateTimeRecord, allowed, forbidden)

    def test_bytes(self):
        @pyschema.no_auto_store()
        class BytesRecord(pyschema.Record):
            field = Bytes()

        allowed = ["hej"]
        forbidden = [u"hej"]
        self.assertCompliant(BytesRecord, allowed, forbidden)

    def test_enum(self):
        @pyschema.no_auto_store()
        class EnumRecord(pyschema.Record):
            field = Enum(["FOO", "BAR"])

        allowed = ["FOO", "BAR", u"FOO"]
        forbidden = ["foo", "bar", "BAZ", True]
        self.assertCompliant(EnumRecord, allowed, forbidden)

    def test_list(self):
        @pyschema.no_auto_store()
        class ListRecord(pyschema.Record):
            field = List(Text())

        allowed = [[u"foo", u"bar"], []]
        forbidden = [[1], {}, set()]
        self.assertCompliant(ListRecord, allowed, forbidden)
        self.assertValue(ListRecord, (), [])  # tuple should be allowed
        self.assertValue(ListRecord, (u"baz",), [u"baz"])

    def test_subrecord(self):
        @pyschema.no_auto_store()
        class SubRecordRecord(pyschema.Record):
            @pyschema.no_auto_store()
            class Inner(pyschema.Record):
                field = Text()
            field = SubRecord(Inner)

        allowed = [SubRecordRecord.Inner(field=u"foo")]
        forbidden = [u"foo", {"field": u"foo"}]
        self.assertCompliant(SubRecordRecord, allowed, forbidden)

    def test_subrecord_self(self):
        @pyschema.no_auto_store()
        class SelfReference(pyschema.Record):
            field = SubRecord(SELF)

        allowed = [SelfReference(
            field=SelfReference(
                field=SelfReference()
            )
        )]
        self.assertCompliant(SelfReference, allowed, [])

    def test_map(self):
        @pyschema.no_auto_store()
        class MapRecord(pyschema.Record):
            field = Map(Integer())
        allowed = [{"foo": 213}, {u"foo": 2}, {}]
        forbidden = [[("foo", 213)], {"foo": "2"}]
        self.assertCompliant(MapRecord, allowed, forbidden)

    def test_nested(self):
        @pyschema.no_auto_store()
        class NestedRecord(pyschema.Record):
            @pyschema.no_auto_store()
            class Inner(pyschema.Record):
                field = Text()
            field = Map(List(SubRecord(Inner)))

        allowed = [{"foo": [NestedRecord.Inner(field=u"bar")]}]
        forbidden = []
        self.assertCompliant(NestedRecord, allowed, forbidden)

    def test_nested_self(self):
        @pyschema.no_auto_store()
        class NestedRecord(pyschema.Record):
            field = Map(List(SubRecord(SELF)))

        allowed = [{"foo": [
            NestedRecord(field={
                "bar": [NestedRecord()]
                }
            )]}
        ]
        forbidden = []
        self.assertCompliant(NestedRecord, allowed, forbidden)
