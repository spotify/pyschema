from unittest import TestCase
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import pyschema
from pyschema.types import Text, Integer, List, Enum
import pyschema.contrib.avro
from cStringIO import StringIO


class RealAvroTest(TestCase):
    def test_avrofile_roundtrip(self):
        """
        Validates created schemas using official avro python library and serializes some data
        using the python API together with pyschema functions.

        TODO: test using the official avro json serialization which isn't supported by the
        python avro library. Create a simple Java tool to pipe json into to create avro files
        and the other way around
        """
        @pyschema.no_auto_store()
        class Foo(pyschema.Record):
            txt = Text()
            n = Integer()

        pychema_avro = pyschema.contrib.avro.get_schema_string(Foo)
        avro_schema = avro.schema.parse(pychema_avro)
        print avro_schema
        avro_file = StringIO()
        writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)

        input_records = (
            Foo(txt=u"Foo", n=5),
            Foo(txt=u"Bar"),
        )
        for ir in input_records:
            print pyschema.core.to_json_compatible(ir)
            writer.append(pyschema.core.to_json_compatible(ir))
        writer.flush()

        avro_file.seek(0)
        reader = DataFileReader(avro_file, DatumReader())
        for i, datum in enumerate(reader):
            record = pyschema.core.from_json_compatible(Foo, datum)
            self.assertEqual(record, input_records[i])
        reader.close()


class ComplexTypeTests(TestCase):
    def _readback(self, record):
        avro_file = StringIO()
        pychema_avro = pyschema.contrib.avro.get_schema_string(record)
        avro_schema = avro.schema.parse(pychema_avro)
        writer = DataFileWriter(avro_file, DatumWriter(), avro_schema)
        datum = pyschema.core.to_json_compatible(record)
        writer.append(datum)
        writer.flush()

        avro_file.seek(0)
        reader = DataFileReader(avro_file, DatumReader())
        for i, datum in enumerate(reader):
            record = pyschema.core.from_json_compatible(record.__class__, datum)
        reader.close()
        return record

    def test_list(self):
        @pyschema.no_auto_store()
        class Foo(pyschema.Record):
            l = List(Integer())

        loaded = self._readback(Foo(l=[1, 2, 3]))

        self.assertEqual(
            tuple(loaded.l),
            (1, 2, 3)
        )

        self.assertEqual(
            [],
            self._readback(Foo(l=[])).l
        )


    def test_enum(self):
        @pyschema.no_auto_store()
        class Foo(pyschema.Record):
            e = Enum(["FOO", "BAR"])

        self.assertEqual(
            self._readback(Foo(e="FOO")),
            Foo(e="FOO")
        )

        self.assertEqual(
            None,
            self._readback(Foo()).e
        )

        self.assertRaises(ValueError,
            lambda: self._readback(Foo(e="Foo"))
        )
