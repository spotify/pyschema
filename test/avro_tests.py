from common import BaseTest
from pyschema import Record
from pyschema.types import Boolean, Integer, Float, Bytes, Text, Enum, List
import pyschema.contrib.avro
from pprint import pprint
import simplejson as json


class SomeAvroRecord(Record):
    a = Text()
    b = Integer()
    c = Bytes()
    d = Boolean()
    e = Float()
    f = Enum([
        "FOO", "bar"
    ])
    g = List(Text())


hand_crafted_schema_dict = {
    "type": "record",
    "name": "SomeAvroRecord",
    "fields": [
        {"name": "a", "type": ["string", "null"]},
        {"name": "b", "type": ["long", "null"]},
        {"name": "c", "type": ["bytes", "null"]},
        {"name": "d", "type": ["boolean", "null"]},
        {"name": "e", "type": ["double", "null"]},
        {"name": "f", "type": "enum", "symbols": ["FOO", "bar"]},
        {"name": "g", "type": "array", "items": "string"},
    ]
}


class TestAvro(BaseTest):
    def test_avro_schema(self):
        schema = pyschema.contrib.avro.get_schema_dict(SomeAvroRecord)
        fields = schema["fields"]
        names = tuple(x["name"] for x in fields)
        self.assertEquals(
            names,
            ("a", "b", "c", "d", "e", "f", "g")
        )
        self.assertEquals(schema["type"], "record")
        self.assertEquals(schema["name"], "SomeAvroRecord")

    def test_roundtrip(self):
        schema_string = pyschema.contrib.avro.get_schema_string(SomeAvroRecord)
        schema_dict = json.loads(schema_string)
        try:
            self.recursive_compare(schema_dict, hand_crafted_schema_dict)
            self.recursive_compare(hand_crafted_schema_dict, schema_dict)
        except:
            print "Intended:", hand_crafted_schema_dict
            print "Output:", schema_dict
            raise


# if the avro package is installed, also include avro integration tests
try:
    __import__("avro")
except ImportError:
    pass
else:
    from _avro_tests import *
