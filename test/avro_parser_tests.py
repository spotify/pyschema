from unittest import TestCase
try:
    import simplejson as json
except ImportError:
    import json

import pyschema
from pyschema_extensions import avro_parser, avro


class NoAutoRegister(TestCase):
    def setUp(self):
        pyschema.disable_auto_register()

    def tearDown(self):
        pyschema.enable_auto_register()


class ParseThreeIncludingNullable(NoAutoRegister):
    schema_name = "FooRecord"
    avsc = """
{
  "type" : "record",
  "name" : "FooRecord",
  "fields" : [ {
    "name" : "field_a",
    "type" : [ "null", "string" ],
    "default" : null
  }, {
    "name" : "long_b",
    "type" : [ "null", "long" ],
    "default" : null
  }, {
    "name" : "required_c",
    "type" : "string"
  } ]
}"""

    references = [
        ("field_a", pyschema.Text()),
        ("long_b", pyschema.Integer()),
        ("required_c", pyschema.Text(nullable=False))
    ]

    def test_can_parse(self):
        schema_class = avro_parser.parse_schema_string(self.avsc)
        self.assertTrue(pyschema.ispyschema(schema_class))
        self.assertEqual(schema_class._schema_name, self.schema_name)

        for (gen_name, gen_type), (ref_name, ref_type) in zip(schema_class._fields.items(), self.references):
            self.assertEqual(gen_name, ref_name)
            self.assertTrue(gen_type.congruent(ref_type), "Types for field {0!r} don't match".format(ref_name))

    def test_roundtrip(self):
        schema_class = avro_parser.parse_schema_string(self.avsc)
        schema_struct = json.loads(self.avsc)
        regenerated_struct = avro.get_schema_dict(schema_class)
        self.assertEqual(schema_struct, regenerated_struct)


class ParseMaps(ParseThreeIncludingNullable):
    schema_name = "MySchema"
    avsc = """
{
    "name": "MySchema",
    "type": "record",
    "fields": [{
        "default": {},
        "type": {
            "values": ["null", "long"],
            "type": "map"
        },
        "name": "m"
    }, {
        "default": {},
        "type": [{
            "values": ["null", "long"],
            "type": "map"
        }, "null"],
        "name": "n"
    }]
}"""
    references = [
        ("m", pyschema.Map(pyschema.Integer())),
        ("n", pyschema.Map(pyschema.Integer(), nullable=True))
    ]


class RetainDocs(NoAutoRegister):
    avsc = """
{
    "name": "Foo",
    "type": "record",
    "doc": "hello world\\nfoo",
    "fields": [
        {"name": "a", "type": "string", "doc": "this is a field"}
    ]
}
    """

    def test_doc_intact(self):
        schema = avro_parser.parse_schema_string(self.avsc)
        self.assertEqual(schema.__doc__, u"hello world\nfoo")
        self.assertEqual(schema.a.description, u"this is a field")


class BigRecord(NoAutoRegister):
    avsc = """
{
  "type" : "record",
  "name" : "BigRecord",
  "namespace" : "com.my.Super.NameSpace",
  "fields" : [ {
    "name" : "a",
    "type" : "double"
  }, {
    "name" : "b",
    "type" : "string"
  }, {
    "name" : "c",
    "type" : "string"
  }, {
    "name" : "d",
    "type" : "int"
  }, {
    "name" : "e",
    "type" : "long"
  }, {
    "name" : "wrapper1",
    "type" : {
      "type" : "record",
      "name" : "Wrapper1",
      "namespace" : "com.my.Wrappers",
      "doc" : "Some nifty documentation about Wrapper1 things.",
      "fields" : [ {
        "name" : "f",
        "type" : "string"
      }, {
        "name" : "g",
        "type" : "int"
      } ]
    },
    "doc" : "This contains a Wrapper1 record."
  }, {
    "name" : "message",
    "type" : {
      "type" : "record",
      "name" : "Wrapper2",
      "namespace" : "com.my.Wrappers",
      "doc" : "This is a Wrapper2 thing",
      "fields" : [ {
        "name" : "h",
        "type" : "string"
      }, {
        "name" : "i",
        "type" : "int"
      }, {
        "name" : "j",
        "type" : [ "null", "string" ],
        "doc" : "This is nullable",
        "default" : null
      }, {
        "name" : "k",
        "type" : [ "null", "string" ],
        "doc" : "This is nullable and nice",
        "default" : null
      } ]
    },
    "doc" : "Don't touch this"
  } ]
}
    """

    def test_parse(self):
        schema = avro_parser.parse_schema_string(self.avsc)
        print avro.get_schema_string(schema)
