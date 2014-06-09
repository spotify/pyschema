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

from common import BaseTest
from pyschema import Record, no_auto_store
from pyschema.types import Boolean, Integer, Float, Bytes, Text, Enum, List
from pyschema.types import SubRecord, Map, Date, DateTime
import pyschema.contrib.avro
import simplejson as json


class TextRecord(Record):
    t = Text()

class TextRecord2(Record):
    t = Text()

class SomeAvroRecord(Record):
    a = Text()
    b = Integer()
    c = Bytes()
    d = Boolean()
    e = Float()
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


hand_crafted_schema_dict = {
    "type": "record",
    "name": "SomeAvroRecord",
    "fields": [
        {"name": "a", "type": ["string", "null"]},
        {"name": "b", "type": ["long", "null"]},
        {"name": "c", "type": ["bytes", "null"]},
        {"name": "d", "type": ["boolean", "null"]},
        {"name": "e", "type": ["double", "null"]},
        {"name": "f", "type": [{
            "type": "enum",
            "name": "ENUM",
            "symbols": ["FOO", "bar"]
        }, "null"]},
        {"name": "g", "type": [{"type": "array", "items": ["string", "null"]},
                               "null"]},
        {"name": "h", "type": {"type": "array", "items": [{
            "name": "TextRecord",
            "type": "record",
            "fields": [{"name": "t", "type": ["string", "null"]}]
        }, "null"]}},
        {"name": "i", "type": [{"type": "map", "values": ["string", "null"]}, "null"]},

        # The second instance of TextRecord should use type name
        {"name": "j", "type": {"type": "map", "values": ["TextRecord", "null"]}},
        {"name": "k", "type": ["string", "null"]},
        {"name": "l", "type": ["string", "null"]},
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
            "fields": [{"name": "t", "type": ["string", "null"]}]}
        }
    ]
}


class TestAvro(BaseTest):
    def test_avro_schema(self):
        schema = pyschema.contrib.avro.get_schema_dict(SomeAvroRecord)
        fields = schema["fields"]
        names = tuple(x["name"] for x in fields)
        self.assertEquals(
            names,
            ("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
             "n", "o", "p", "q", "r", "s", "t")
        )
        self.assertEquals(schema["type"], "record")
        self.assertEquals(schema["name"], "SomeAvroRecord")

    def test_serialization(self):
        schema_string = pyschema.contrib.avro.get_schema_string(SomeAvroRecord)
        schema_dict = json.loads(schema_string)
        try:
            self.recursive_compare(schema_dict, hand_crafted_schema_dict)
            self.recursive_compare(hand_crafted_schema_dict, schema_dict)
        except:
            print "Intended:", hand_crafted_schema_dict
            print "    Output:", schema_dict
            raise

    def test_internal_roundtrip(self):
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
        )
        avro_string = pyschema.contrib.avro.dumps(s)
        new_s = pyschema.contrib.avro.loads(
            avro_string,
            record_class=SomeAvroRecord
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
        self.assertEquals(new_s.k, datetime.date(2014,4,20))
        self.assertEquals(new_s.l, datetime.datetime(2014,4,20,12,0,0))

    def test_unset_list(self):
        @no_auto_store()
        class ListRecord(Record):
            a = List(Text())

        lr = ListRecord()
        str_rec = pyschema.contrib.avro.dumps(lr)
        reborn = pyschema.contrib.avro.loads(str_rec, record_class=ListRecord)
        self.assertTrue(reborn.a is None)

    def test_list_roundtrip(self):
        @no_auto_store()
        class ListRecord(Record):
            a = List(Text())

        lr = ListRecord(a=["c", "a", "b"])
        str_rec = pyschema.contrib.avro.dumps(lr)
        reborn = pyschema.contrib.avro.loads(str_rec, record_class=ListRecord)

        self.assertEquals(
            tuple(reborn.a),
            ("c", "a", "b")
        )

    def test_unset_map(self):
        @no_auto_store()
        class MapRecord(Record):
            a = Map(Text())

        mr = MapRecord()
        str_rec = pyschema.contrib.avro.dumps(mr)
        reborn = pyschema.contrib.avro.loads(str_rec, record_class=MapRecord)
        self.assertTrue(reborn.a is None)

    def test_map_roundtrip(self):
        @no_auto_store()
        class MapRecord(Record):
            a = Map(Text())

        mr = MapRecord(a={"a": "b", "c": "d"})
        str_rec = pyschema.contrib.avro.dumps(mr)
        reborn = pyschema.contrib.avro.loads(str_rec, record_class=MapRecord)

        self.assertEquals(
            reborn.a,
            {"a": "b", "c": "d"}
        )


class TestSubRecord(Record):
    def test_subrecord_null(self):
        @no_auto_store()
        class TestRecord(Record):
            @no_auto_store()
            class Inner(Record):
                field = Integer()
            i = SubRecord(Inner)

        r = TestRecord()
        s = pyschema.contrib.avro.dumps(r)
        pyschema.contrib.avro.loads(s, record_class=TestRecord)
