import unittest
import warnings
from pyschema import Record, RecordStore, no_auto_store
from pyschema.types import *
import namespaced_schemas


@no_auto_store()
class TestRecord(Record):
    a = Text()


class NamespaceTest(unittest.TestCase):

    def test_get_record(self):
        store = RecordStore()
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord').__module__, 'test.namespaced_schemas')
        self.assertEquals(store.get('my.namespace.TestRecord').__module__, 'test.namespaced_schemas')

    def test_records_with_same_name(self):
        """
        Add two records to the store. Both have the same name but different namespace. The one without namespace
        should have priority over the one without when doing lookup without namespace
        """
        store = RecordStore()

        # Record with namespace will take 2 keys in the dict
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord'), namespaced_schemas.TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

        # Non namespace will replace the non-namespace key in dict
        store.add_record(TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

    def test_records_with_same_name_reversed_order(self):
        """
        Add two records to the store. Both have the same name but different namespace. The one without namespace
        should have priority over the one without when doing lookup without namespace
        """
        store = RecordStore()

        # Record without namespace will take 2 keys in dict
        store.add_record(TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), TestRecord)

        # Record with namespace will not replace the base level record
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

    def test_get_without_namespace(self):
        store = RecordStore()
        store.add_record(TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('any.namespace.should.work.TestRecord'), TestRecord)
        self.assertRaises(KeyError, store.get, 'OtherRecord')

    def test_get_without_namespace_namespaced_record(self):
        store = RecordStore()
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord'), namespaced_schemas.TestRecord)
        self.assertEquals(store.get('any.namespace.should.work.TestRecord'), namespaced_schemas.TestRecord)
        self.assertRaises(KeyError, store.get, 'OtherRecord')

    def test_deprecated_namespace(self):
        @no_auto_store()
        class TestRecord(Record):
            _avro_namespace_ = 'legacy'
            a = Text()

        store = RecordStore()

        # Verify that we get a deprecation warning when we add the record
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            store.add_record(TestRecord)
            self.assertEquals(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))
            self.assertEquals("_avro_namespace is deprecated, use _namespace instead", str(w[-1].message))

        self.assertEquals(store.get('legacy.TestRecord'), TestRecord)


if __name__ == '__main__':
    unittest.main()
