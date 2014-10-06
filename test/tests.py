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

from unittest import TestCase
from pyschema import Record, dumps, loads, ispyschema, no_auto_store
from pyschema.types import *
import pyschema.core


class RevertDefinitionsTest(TestCase):
    def setUp(self):
        self._original_schemas = pyschema.core.auto_store  # original
        pyschema.core.auto_store = self._original_schemas.clone()

    def tearDown(self):
        pyschema.core.auto_store = self._original_schemas


class TestNestedRecord(RevertDefinitionsTest):
    def test_full_circle(self):
        class Foo(Record):
            bin = Bytes()

        class MyRecord(Record):
            a_string = Text()
            a_float = Float()
            record = List(SubRecord(Foo))

        rec = MyRecord(a_string=u"hej")
        rec.record = [Foo(bin="bar")]

        s = dumps(rec)
        reloaded_obj = loads(s)

        self.assertEquals(reloaded_obj.a_string, u"hej")
        self.assertTrue(reloaded_obj.a_float is None)
        self.assertTrue(reloaded_obj.record[0].bin, "bar")


class TestBaseRecordNotInStore(TestCase):
    def test(self):
        self.assertTrue(Record not in pyschema.core.auto_store)


class TestBasicUsage(TestCase):
    def setUp(self):
        @no_auto_store()
        class Foo(Record):
            t = Text()
            i = Integer()
            b = Boolean()

            def calculated(self):
                return self.t * 2

        self.Foo = Foo

    def test_class_field(self):
        record = self.Foo(t=u"foo")
        self.assertEquals(record.t, u"foo")

    def test_post_declaration_field(self):
        record = self.Foo(i=10)
        self.assertEquals(record.i, 10)

    def test_setattr_field(self):
        record = self.Foo()
        self.assertTrue(record.b is None)
        record.b = False
        self.assertFalse(record.b)

    def test_forbidden_assignment(self):
        record = self.Foo()

        def forbidden_assignment():
            record.c = "Something"

        self.assertRaises(
            AttributeError,
            forbidden_assignment
        )

    def test_record_name(self):
        self.assertEquals(
            self.Foo._schema_name,
            "Foo"
        )

    def test_type_adherence(self):
        self.assertTrue(ispyschema(self.Foo))
        self.assertTrue(issubclass(self.Foo, Record))
        self.assertTrue(isinstance(self.Foo, pyschema.core.PySchema))

    def test_method(self):
        record = self.Foo(t=u"a")
        calc = record.calculated()
        self.assertEquals(calc, u"aa")


class TestRuntimeRecord(TestBasicUsage):
    def setUp(self):
        class Foo(object):
            t = Text()

            def calculated(self):
                return self.t * 2

        Foo.i = Integer()
        setattr(Foo, "b", Boolean())
        self.Foo = pyschema.core.PySchema.from_class(Foo, auto_store=False)
