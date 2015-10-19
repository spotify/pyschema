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
import datetime
import warnings
from unittest import TestCase
from common import BaseTest
import pyschema
from pyschema import Record, no_auto_store
from pyschema.types import Boolean, Integer, Float, Bytes, Text, Enum, List
from pyschema.types import SubRecord, Map, Date, DateTime
from pyschema.core import ParseError
import pyschema_extensions.avro
try:
    import simplejson as json
except ImportError:
    import json


class TextRecord(Record):
    t = Text()


class TextRecord2(Record):
    t = Text()


class TextRecord3(Record):
    _namespace = "blah.blah"
    t = Text()


class SomeAvroRecord(Record):
    """SomeAvroRecord contains a lot of cool stuff"""
    a = Text()
    b = Integer()
    c = Bytes()
    d = Boolean()
    e = Float(description="hello world!")
    f = Enum([
        "FOO", "bar"
    ])
    g = List(Text(), nullable=True)
    h = List(SubRecord(TextRecord))
    i = Map(Text(), nullable=True)
    j = Map(SubRecord(TextRecord))
    k = Date()
    l = DateTime()
    m = Text(nullable=False)
    n = Integer(nullable=False)
    o = Bytes(nullable=False)
    p = Boolean(nullable=False)
    q = Float(nullable=False)
    r = Enum(["FOO", "bar"], nullable=False)
    s = SubRecord(TextRecord, nullable=False)
    t = SubRecord(TextRecord2, nullable=False)
    u = SubRecord(TextRecord3)
    v = SubRecord(TextRecord3) 


hand_crafted_schema_dict = {
    "type": "record",
    "name": "SomeAvroRecord",
    "doc": "SomeAvroRecord contains a lot of cool stuff",
    "fields": [
        {"name": "a", "type": ["null", "string"], "default": None},
        {"name": "b", "type": ["null", "long"], "default": None},
        {"name": "c", "type": ["null", "bytes"], "default": None},
        {"name": "d", "type": ["null", "boolean"], "default": None},
        {"name": "e", "type": ["null", "double"], "default": None, "doc": "hello world!"},
        {"name": "f", "type": ["null", {
            "type": "enum",
            "name": "ENUM",
            "symbols": ["FOO", "bar"]
        }], "default": None},
        {"name": "g", "type": [{"type": "array", "items": ["null", "string"]},
                               "null"], "default": []},
        {"name": "h",
         "type": {
          "type": "array",
          "items": [
           "null",
           {
            "name": "TextRecord",
            "type": "record",
            "fields": [{"name": "t", "type": ["null", "string"], "default": None}]
           }
          ]
         },
         "default": []
        },
        {"name": "i", "type": [{"type": "map", "values": ["null", "string"]}, "null"], "default": {}},

        # The second instance of TextRecord should use type name
        {"name": "j", "type": {"type": "map", "values": ["null", "TextRecord"]}, "default": {}},
        {"name": "k", "type": ["null", "string"], "default": None},
        {"name": "l", "type": ["null", "string"], "default": None},
        {"name": "m", "type": "string"},
        {"name": "n", "type": "long"},
        {"name": "o", "type": "bytes"},
        {"name": "p", "type": "boolean"},
        {"name": "q", "type": "double"},
        {"name": "r", "type": {
            "type": "enum",
            "name": "ENUM",
            "symbols": ["FOO", "bar"]
        }},
        {"name": "s", "type": "TextRecord"},
        {"name": "t", "type": {
            "name": "TextRecord2",
            "type": "record",
            "fields": [{"name": "t", "type": ["null", "string"], "default": None}]}
        },
        {"name": "u", "type": ["null", {
             "name": "TextRecord3",
             "type": "record",
             "namespace": "blah.blah",
             "fields": [{"name": "t", "type": ["null", "string"], "default": None}]}],
             "default": None
        },
        {"name": "v", "type": ["null", "blah.blah.TextRecord3"],
             "default": None
        }
    ]
}


class NullableDefaultRecord(Record):
    field_without_default = Text(nullable=False, default=pyschema.NO_DEFAULT)
    field_with_default = Text(nullable=False, default=u"my_default")
    nullable_field_without_default = Text(default=pyschema.NO_DEFAULT)
    nullable_field_with_null_default = Text()
    nullable_field_with_other_default = Text(default=u"my_other_default")
    map_default_field = Map(Text())
    list_default_field = List(Text(nullable=False))


nullable_default_record_schema = {
    "type": "record",
    "name": "NullableDefaultRecord",
    "fields": [
        {"name": "field_without_default", "type": "string"},
        {"name": "field_with_default", "type": "string", "default": "my_default"},
        {"name": "nullable_field_without_default", "type": ["null", "string"]},  # this could just as well be ["string", "null"]
        {"name": "nullable_field_with_null_default", "type": ["null", "string"], "default": None},
        {"name": "nullable_field_with_other_default", "type": ["string", "null"], "default": u"my_other_default"},
        {"name": "map_default_field", "type": {"type": "map", "values": ["null", "string"]}, "default": {}},
        {"name": "list_default_field", "type": {"type": "array", "items": "string"}, "default": []}
    ]
}


class TestNullableDefaultRecord(BaseTest):
    def test_empty_construction(self):
        record = NullableDefaultRecord()
        self.assertEquals(record.field_with_default, u"my_default")
        self.assertEquals(record.field_without_default, pyschema.NO_DEFAULT)
        self.assertEquals(record.nullable_field_with_null_default, None)
        self.assertEquals(
            record.nullable_field_with_other_default,
            u"my_other_default"
        )

    def test_avro_schema(self):
        generated_schema_dct = pyschema_extensions.avro.get_schema_dict(NullableDefaultRecord)
        self.recursive_compare(generated_schema_dct, nullable_default_record_schema)

    def test_original_default_not_affected(self):
        a = NullableDefaultRecord()
        b = NullableDefaultRecord()
        a.map_default_field["something"] = "foo"
        self.assertFalse("something" in b.map_default_field)
        b.list_default_field.append("foo")
        self.assertEquals(len(a.list_default_field), 0)


class TestAvro(BaseTest):
    def test_avro_schema(self):
        schema = pyschema_extensions.avro.get_schema_dict(SomeAvroRecord)
        fields = schema["fields"]
        names = tuple(x["name"] for x in fields)
        self.assertEquals(
            names,
            ("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
             "n", "o", "p", "q", "r", "s", "t", "u", "v")
        )
        self.assertEquals(schema["type"], "record")
        self.assertEquals(schema["name"], "SomeAvroRecord")

    def test_serialization(self):
        schema_string = pyschema_extensions.avro.get_schema_string(SomeAvroRecord)
        schema_dict = json.loads(schema_string)
        try:
            self.recursive_compare(schema_dict, hand_crafted_schema_dict)
            self.recursive_compare(hand_crafted_schema_dict, schema_dict)
        except:
            print "Intended:", json.dumps(hand_crafted_schema_dict)
            print "    Output:", json.dumps(schema_dict)
            raise

    def test_internal_roundtrip(self):
        all_bytes = ''.join([chr(i) for i in xrange(256)])
        s = SomeAvroRecord(
            a=u"yolo",
            b=4,
            c=chr(1),
            d=False,
            e=0.1,
            f="FOO",
            g=["wtf", "bbq"],
            h=[TextRecord(t="yolo"), TextRecord(t="swag")],
            i={"bar": "baz"},
            j={"foo": TextRecord(t="bar"), "bar": TextRecord(t="baz")},
            k=datetime.date(2014,4,20),
            l=datetime.datetime(2014,4,20,12,0,0),
            m=u"spotify",
            n=1,
            o=all_bytes,
            p=True,
            q=0.5,
            r="bar",
            s=TextRecord(t="ace"),
            t=TextRecord2(t="look"),
            u=TextRecord3(t="dog"),
            v=TextRecord3(t="namespaceTest")
        )
        avro_string = pyschema_extensions.avro.dumps(s)
        new_s = pyschema_extensions.avro.loads(
            avro_string,
            schema=SomeAvroRecord
        )
        self.assertEquals(new_s.a, u"yolo")
        self.assertEquals(new_s.b, 4)
        self.assertEquals(new_s.c, chr(1))
        self.assertEquals(new_s.d, False)
        self.assertEquals(new_s.e, 0.1)
        self.assertEquals(new_s.f, u"FOO")
        self.assertEquals(tuple(new_s.g), (u"wtf", u"bbq"))
        self.assertEquals(len(new_s.h), 2)
        self.assertEquals((new_s.h[0].t, new_s.h[1].t), (u"yolo", u"swag"))
        self.assertEquals(new_s.i, {"bar": "baz"})
        self.assertTrue("foo" in new_s.j)
        self.assertTrue("bar" in new_s.j)
        self.assertEqual(new_s.j["foo"].t, "bar")
        self.assertEqual(new_s.j["bar"].t, "baz")
        self.assertEquals(new_s.k, datetime.date(2014, 4, 20))
        self.assertEquals(new_s.l, datetime.datetime(2014, 4, 20, 12, 0, 0))
        self.assertEquals(new_s.m, u"spotify")
        self.assertEquals(new_s.n, 1)
        self.assertEquals(new_s.o, all_bytes)
        self.assertEquals(new_s.p, True)
        self.assertEquals(new_s.q, 0.5)
        self.assertEquals(new_s.r, u"bar")
        self.assertEquals(new_s.s.t, u"ace")
        self.assertEquals(new_s.t.t, u"look")
        self.assertEquals(new_s.u.t, u"dog")
        self.assertEquals(new_s.u._namespace, u"blah.blah")
        self.assertEquals(new_s.v._namespace, u"blah.blah")

    def test_unset_list(self):
        @no_auto_store()
        class ListRecord(Record):
            a = List(Text())

        lr = ListRecord()
        str_rec = pyschema_extensions.avro.dumps(lr)
        reborn = pyschema_extensions.avro.loads(str_rec, schema=ListRecord)
        self.assertTrue(isinstance(reborn.a, list))

    def test_list_roundtrip(self):
        @no_auto_store()
        class ListRecord(Record):
            a = List(Text())

        lr = ListRecord(a=["c", "a", "b"])
        str_rec = pyschema_extensions.avro.dumps(lr)
        reborn = pyschema_extensions.avro.loads(str_rec, schema=ListRecord)

        self.assertEquals(
            tuple(reborn.a),
            ("c", "a", "b")
        )

    def test_unset_map(self):
        @no_auto_store()
        class MapRecord(Record):
            a = Map(Text())

        mr = MapRecord()
        str_rec = pyschema_extensions.avro.dumps(mr)
        reborn = pyschema_extensions.avro.loads(str_rec, schema=MapRecord)
        self.assertTrue(isinstance(reborn.a, dict))

    def test_map_roundtrip(self):
        @no_auto_store()
        class MapRecord(Record):
            a = Map(Text())

        mr = MapRecord(a={"a": "b", "c": "d"})
        str_rec = pyschema_extensions.avro.dumps(mr)
        reborn = pyschema_extensions.avro.loads(str_rec, schema=MapRecord)

        self.assertEquals(
            reborn.a,
            {"a": "b", "c": "d"}
        )

    def test_short_types(self):
        short_int = Integer(size=4)
        self.assertEquals(short_int.avro_type_name, 'int')
        long_int = Integer()
        self.assertEquals(long_int.avro_type_name, 'long')
        short_float = Float(size=4)
        self.assertEquals(short_float.avro_type_name, 'float')
        long_float = Float()
        self.assertEquals(long_float.avro_type_name, 'double')


class TestSubRecord(Record):
    def test_subrecord_null(self):
        @no_auto_store()
        class TestRecord(Record):
            @no_auto_store()
            class Inner(Record):
                field = Integer()
            i = SubRecord(Inner)

        r = TestRecord()
        s = pyschema_extensions.avro.dumps(r)
        pyschema_extensions.avro.loads(s, schema=TestRecord)


class TestExtraFields(TestCase):

    def test_fields(self):
        @pyschema.no_auto_store()
        class ValidRecord(pyschema.Record):
            field = Integer()

        line = '{"field": {"long": 8}, "invalid_field": {"long": 8}}'

        self.assertRaises(ParseError, lambda: pyschema_extensions.avro.loads(line, schema=ValidRecord))


class TestNamespaceMigration(TestCase):
    def test_old_data(self):
        """
        Test that we are able to read data created without namespace after namespace is added.
        """
        test_store = pyschema.core.SchemaStore()

        @test_store.add_record
        @no_auto_store()
        class NamespacedSubRecord(Record):
            _namespace = 'pyschema'
            a = Text()

        @test_store.add_record
        @no_auto_store()
        class NamespacedMainRecord(Record):
            _namespace = 'pyschema'
            sub_record = SubRecord(NamespacedSubRecord)

        # This data does not have a subrecord but we should be able to parse it anyways
        legacy_data = '{"sub_record": {"NamespacedSubRecord": {"a": {"string": "test"}}}}'
        pyschema_extensions.avro.loads(legacy_data, schema=NamespacedMainRecord)

        legacy_data_with_record_class = ('{"$schema": "NamespacedMainRecord", "sub_record":'
                                         '{"NamespacedSubRecord": {"a": {"string": "test"}}}}')
        pyschema_extensions.avro.loads(legacy_data_with_record_class, record_store=test_store)


class TestEnumTypeName(TestCase):
    def test_no_name(self):
        # when no name is declared, use the dummy "ENUM" name
        val = Enum(["FOO", "BAR"], nullable=True).avro_dump("FOO")
        self.assertIn("ENUM", val)

    def test_name(self):
        val = Enum(["FOO", "BAR"], nullable=True, name="EType").avro_dump("BAR")
        self.assertIn("EType", val)
        self.assertNotIn("ENUM", val)


class TestExtraFields(TestCase):
    def test_ignore_extra(self):
        test_store = pyschema.core.SchemaStore()

        @test_store.add_record
        @no_auto_store()
        class EmptySchema(Record):
            pass

        data = '{"$schema": "EmptySchema", "not_in_schema": 1}'
        with warnings.catch_warnings(record=True) as ws:
            pyschema_extensions.avro.loads(data, record_store=test_store)
        self.assertEquals(len(ws), 1)
