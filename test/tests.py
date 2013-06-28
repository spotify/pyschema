# Copyright (c) 2012 Spotify AB
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
    def test_full_circle(self):
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


class TestBaseRecordNotInStore(TestCase):
    def test(self):
        self.assertNotIn(Record, pyschema.core.auto_store)
