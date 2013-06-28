from unittest import TestCase
from pyschema import Record, dumps, loads
from pyschema.types import *
import pyschema.core


class RevertDefinitionsTest(TestCase):
    def setUp(self):
        self._original_schemas = pyschema.core.auto_store  # original
        pyschema.core.auto_store = self._original_schemas.clone()

    def tearDown(self):
        pyschema.core.auto_store = self._original_schemas


class TestSubRecord(RevertDefinitionsTest):
    def test_foo(self):
        class Foo(Record):
            bin = Blob()

        class MyRecord(Record):
            a_string = String()
            a_float = Float()
            record = List(SubRecord(Foo))

        rec = MyRecord(a_string=u"hej")
        rec.record = [Foo(bin="bar")]

        s = dumps(rec)
        reloaded_obj = loads(s)

        self.assertEquals(reloaded_obj.a_string, u"hej")
        self.assertTrue(reloaded_obj.a_float is None)
        self.assertTrue(reloaded_obj.record[0].bin, "bar")
