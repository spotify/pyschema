from unittest import TestCase
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import pyschema
from pyschema.types import Text, Integer
import pyschema.contrib.avro
from cStringIO import StringIO


@pyschema.no_auto_store()
class Foo(pyschema.Record):
    txt = Text()
    n = Integer()


class RealAvroTest(TestCase):
    def test_avrofile_roundtrip(self):
        py_schema_avro = pyschema.contrib.avro.get_schema_string(Foo)
        avro_schema = avro.schema.parse(py_schema_avro)
        avro_file = StringIO()
        writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)

        input_records = (
            Foo(txt=u"Foo", n=5),
            Foo(txt=u"Bar"),
        )
        for ir in input_records:
            writer.append(pyschema.core.to_json_compatible(ir))
        writer.flush()

        avro_file.seek(0)
        reader = DataFileReader(avro_file, DatumReader())
        for i, datum in enumerate(reader):
            record = pyschema.core.from_json_compatible(Foo, datum)
            self.assertEqual(record, input_records[i])
        reader.close()
