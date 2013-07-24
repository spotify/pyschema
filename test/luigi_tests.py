from common import BaseTest
import pyschema
from pyschema.types import Text, Integer
import pyschema.contrib.luigi
from cStringIO import StringIO
import simplejson as json


class FooRecord(pyschema.Record):
    foo = Text()
    bar = Integer()


class TestMRWriter(BaseTest):
    def setUp(self):
        pass

    def seq(self):
        yield FooRecord(foo="Hej", bar=10)
        yield FooRecord(foo="Moo", bar=None)
        yield FooRecord(foo="Tjaba tjena", bar=3)

    def _generic_writer_tests(self, writer, hard_coded_type=None):
        output = StringIO()
        writer(None, self.seq(), output)
        output.seek(0)
        output_lines = output.read()[:-1].split('\n')  # remove last \n before splitting
        output_records = tuple(pyschema.loads(l, record_class=hard_coded_type) for l in output_lines)
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
        writer = pyschema.contrib.luigi.mr_writer
        output_lines, output_records = self._generic_writer_tests(writer)
        record_names = tuple(rec._record_name for rec in output_records)
        self.assertEquals(
            record_names,
            ("FooRecord",) * 3
        )
        for r in output_records:
            self.assertTrue(isinstance(r, FooRecord))

        obj = json.loads(output_lines[0])
        self.recursive_compare(
            obj,
            {"foo": "Hej", "bar": 10, "$record_name": "FooRecord"}
        )

    def test_typeless_mr_writer(self):
        writer = pyschema.contrib.luigi.typeless_mr_writer
        self.assertRaises(pyschema.ParseException, lambda: self._generic_writer_tests(writer))
        output_lines, output_records = self._generic_writer_tests(writer, FooRecord)
        obj = json.loads(output_lines[0])
        self.recursive_compare(
            obj,
            {"foo": "Hej", "bar": 10}
        )

try:
    import luigi
except:
    pass
else:
    # nose will execute these luigi-dependent tests
    # only if luigi module present
    from _luigi_mr_tests import *
