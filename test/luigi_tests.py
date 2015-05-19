from common import BaseTest
import pyschema
from pyschema.types import Text, Integer
import pyschema_extensions.luigi
from cStringIO import StringIO
try:
    import simplejson as json
except ImportError:
    import json


class TestMRWriter(BaseTest):
    def setUp(self):
        class FooRecord(pyschema.Record):
            foo = Text()
            bar = Integer()
        self.FooRecord = FooRecord

    def seq(self):
        yield self.FooRecord(foo="Hej", bar=10)
        yield self.FooRecord(foo="Moo", bar=None)
        yield self.FooRecord(foo="Tjaba tjena", bar=3)

    def _generic_writer_tests(self, writer, hard_coded_type=None):
        output = StringIO()
        writer(None, self.seq(), output)
        output.seek(0)
        # remove last \n before splitting
        output_lines = output.read()[:-1].split('\n')
        output_records = tuple(
            pyschema.loads(l, schema=hard_coded_type) for l in output_lines)
        foos = tuple(rec.foo for rec in output_records)
        bars = tuple(rec.bar for rec in output_records)
        self.assertEquals(
            foos,
            ("Hej", "Moo", "Tjaba tjena")
        )
        self.assertEquals(
            bars,
            (10, None, 3)
        )
        return output_lines, output_records

    def test_mr_writer(self):
        writer = pyschema_extensions.luigi.mr_writer
        output_lines, output_records = self._generic_writer_tests(writer)
        record_names = tuple(rec._schema_name for rec in output_records)
        self.assertEquals(
            record_names,
            ("FooRecord",) * 3
        )
        for r in output_records:
            self.assertTrue(isinstance(r, self.FooRecord))

        obj = json.loads(output_lines[0])
        self.recursive_compare(
            obj,
            {"foo": "Hej", "bar": 10, "$schema": "FooRecord"}
        )
