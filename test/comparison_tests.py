from unittest import TestCase
from pyschema import Record, SubRecord


class MyRecord1(Record):
    pass


class MyRecord2(Record):
    prop = SubRecord(MyRecord1)


class SomeObject:
    pass


class TestRecordComparison(TestCase):
    def test_none(self):
        cmp_obj = None

        obj1 = MyRecord1()
        self.assertFalse(obj1 == cmp_obj)

        obj2 = MyRecord2(prop=MyRecord1())
        self.assertFalse(obj2 == cmp_obj)

    def test_some_object(self):
        cmp_obj = SomeObject()

        obj1 = MyRecord1()
        self.assertFalse(obj1 == cmp_obj)

        obj2 = MyRecord2(prop=MyRecord1())
        self.assertFalse(obj2 == cmp_obj)

    def test_empty_property(self):
        obj1 = MyRecord2()
        obj2 = MyRecord2(prop=MyRecord1())
        self.assertFalse(obj2 == obj1)

    def test_different_records(self):
        obj1 = MyRecord1()
        obj2 = MyRecord2(prop=MyRecord1())
        self.assertFalse(obj2 == obj1)
