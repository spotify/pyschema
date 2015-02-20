# coding: utf-8
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
            self.assertTrue(gen_type.is_similar_to(ref_type), "Types for field {0!r} don't match".format(ref_name))

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


# the schema below was taken from the old avro_to_pyschema tests
supported_avro_schema = """{
  "name": "Supported",
  "type": "record",
  "namespace": "com.spotify.pyschema.test",
  "doc": "We have doc",
  "fields": [
    {
      "name": "int_field",
      "type": "int"
    },
    {
      "name": "float1",
      "type": "double"
    },
    {
      "name": "required_string_field",
      "type": "string"
    },
    {
      "name": "long_field",
      "type": [
        "null",
        "long"
      ],
      "doc": "some number",
      "default": null
    },
    {
      "name": "optional_string_field",
      "type": [
        "null",
        "string"
      ],
      "doc": "",
      "default": null
    },
    {
      "name": "undocumented_string_field",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "string_list",
      "type": [
        "null",
        {
          "type": "array",
          "items": "string"
        }
      ],
      "default": null
    },
    {
      "name": "string_map",
      "type": [
        "null",
        {
          "type": "map",
          "values": "string"
        }
      ],
      "doc": "map of foo",
      "default": null
    },
    {
      "name": "bytes1",
      "type": [
        "null",
        "bytes"
      ],
      "doc": "bytes field 1",
      "default": null
    },
    {
      "name": "boolean1",
      "type": [
        "null",
        "boolean"
      ],
      "doc": "boolean field 1",
      "default": null
    },
    {
      "name": "another_string_field",
      "type": [
        "null",
        "string"
      ],
      "doc": "What",
      "default": null
    },
    {
      "name": "boolean2",
      "type": [
        "null",
        "boolean"
      ],
      "doc": "boolean field 2",
      "default": null
    },
    {
      "name": "bytes2",
      "type": [
        "null",
        "bytes"
      ],
      "doc": "bytes field 2",
      "default": null
    },
    {
      "name": "weird_characters",
      "type": [
        "null",
        "long"
      ],
      "doc": "';drop table schemas;--Āā\u0000\u0000\\nhttp://uncyclopedia.wikia.com/wiki/AAAAAAAAA! \\\\ многабукаф <script>alert(\\"eh\\")</script>(:,%)'",
      "default": null
    },
    {
      "name": "float2",
      "type": [
        "null",
        "double"
      ],
      "doc": "float field 2",
      "default": null
    }
  ]
}
"""


class Supported(pyschema.Record):
    """We have doc"""
    _namespace = "com.spotify.pyschema.test"
    int_field = pyschema.Integer(nullable=False, size=4)
    float1 = pyschema.Float(nullable=False)
    required_string_field = pyschema.Text(nullable=False)
    long_field = pyschema.Integer(description="some number")
    optional_string_field = pyschema.Text(description="")
    undocumented_string_field = pyschema.Text()
    string_list = pyschema.List(pyschema.Text(nullable=False), nullable=True)
    string_map = pyschema.Map(pyschema.Text(nullable=False), description="map of foo", nullable=True)
    bytes1 = pyschema.Bytes(description="bytes field 1")
    boolean1 = pyschema.Boolean(description="boolean field 1")
    another_string_field = pyschema.Text(description="What")
    boolean2 = pyschema.Boolean(description="boolean field 2")
    bytes2 = pyschema.Bytes(description="bytes field 2")
    weird_characters = pyschema.Integer(
        description=u"\';drop table schemas;--\u0100\u0101\x00\x00\n"
                    u"http://uncyclopedia.wikia.com/wiki/AAAAAAAAA! "
                    u"\\ \u043c\u043d\u043e\u0433\u0430\u0431\u0443"
                    u"\u043a\u0430\u0444 <script>alert(\"eh\")</script>(:,%)\'"
    )
    float2 = pyschema.Float(description="float field 2")

unsupported_avro_schema = """{
  "name": "Unsupported",
  "type": "record",
  "namespace": "com.spotify.pyschema.test",
  "fields": [
    {
      "type": "int",
      "name": "version"
    },
    {
      "doc": "Not an union with null",
      "default": 5135123,
      "type": [
        "int",
        "string"
      ],
      "name": "onion"
    },
    {
      "doc": "City of Stockholm",
      "default": null,
      "type": [
        "null",
        "string"
      ],
      "name": "city"
    }
  ]
}
"""


class TestAvroToPySchema(NoAutoRegister):
    def schemas_match(self, a, b):
        self.assertEquals(pyschema.core.get_full_name(a), pyschema.core.get_full_name(b))
        self.assertEquals(len(a._fields), len(b._fields))
        for (a_field_name, a_field), (b_field_name, b_field) in zip(a._fields.items(), b._fields.items()):
            self.assertEquals(a_field_name, b_field_name, msg="field names don't match")
            self.assertTrue(
                a_field.is_similar_to(b_field),
                "field {a_field_name!r} definitions don't match".format(**locals())
            )

    def test_supported_avro_schema_succeeds(self):
        parsed = avro_parser.parse_schema_string(supported_avro_schema)
        self.schemas_match(parsed, Supported)

    def test_unsupported_avro_schema_fails(self):
        self.assertRaises(
            avro_parser.AVSCParseException,
            avro_parser.parse_schema_string,
            unsupported_avro_schema
        )