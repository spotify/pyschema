import unittest
import warnings
import pyschema
from pyschema import Record, SchemaStore, no_auto_store
from pyschema.types import *
import namespaced_schemas


@no_auto_store()
class TestRecord(Record):
    a = Text()


class NamespaceTest(unittest.TestCase):

    def test_get_record(self):
        store = SchemaStore()
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord').__module__, 'test.namespaced_schemas')
        self.assertEquals(store.get('my.namespace.TestRecord').__module__, 'test.namespaced_schemas')

    def test_records_with_same_name(self):
        """
        Add two records to the store. Both have the same name but different namespace. The one without namespace
        should have priority when doing lookup without namespace
        """
        store = SchemaStore()

        # Record with namespace will take 2 keys in the dict
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord'), namespaced_schemas.TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

        with warnings.catch_warnings(record=True) as warninglist:
            # Non namespace will replace the non-namespace key in dict
            store.add_record(TestRecord)
        self.assertEquals(len(warninglist), 1)  # warning when one definition gets replaced
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

    def test_records_with_same_name_reversed_order(self):
        """
        Add two records to the store. Both have the same name but different namespace. The one without namespace
        should have priority when doing lookup without namespace
        """
        store = SchemaStore()

        # Record without namespace will take 2 keys in dict
        store.add_record(TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), TestRecord)

        with warnings.catch_warnings(record=True) as warninglist:
            # Record with namespace will not replace the base level record
            store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(len(warninglist), 0, msg="there shouldn't be a 'replacement' warning")
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('my.namespace.TestRecord'), namespaced_schemas.TestRecord)

    def test_get_without_namespace(self):
        store = SchemaStore()
        store.add_record(TestRecord)
        self.assertEquals(store.get('TestRecord'), TestRecord)
        self.assertEquals(store.get('any.namespace.should.work.TestRecord'), TestRecord)
        self.assertRaises(KeyError, store.get, 'OtherRecord')

    def test_get_without_namespace_namespaced_record(self):
        store = SchemaStore()
        store.add_record(namespaced_schemas.TestRecord)
        self.assertEquals(store.get('TestRecord'), namespaced_schemas.TestRecord)
        self.assertEquals(store.get('any.namespace.should.work.TestRecord'), namespaced_schemas.TestRecord)
        self.assertRaises(KeyError, store.get, 'OtherRecord')

    def test_json_roundtrip(self):
        store = SchemaStore()
        store.add_record(namespaced_schemas.TestRecord)
        with warnings.catch_warnings(record=True) as warninglist:
            store.add_record(TestRecord)
        self.assertEquals(len(warninglist), 1)

        namespace_instance = namespaced_schemas.TestRecord(a='test')
        namespace_roundtrip = pyschema.loads(pyschema.dumps(namespace_instance), record_store=store)
        self.assertTrue(isinstance(namespace_roundtrip, namespaced_schemas.TestRecord))

        instance = TestRecord(a='test')
        roundtrip = pyschema.loads(pyschema.dumps(instance), record_store=store)
        self.assertTrue(isinstance(roundtrip, TestRecord))

    def test_deprecated_namespace(self):
        @no_auto_store()
        class TestRecord(Record):
            _avro_namespace_ = 'legacy'
            a = Text()

        store = SchemaStore()

        # Verify that we get a deprecation warning when we add the record
        with warnings.catch_warnings(record=True) as w:
            # Cause all warnings to always be triggered.
            warnings.simplefilter("always")
            store.add_record(TestRecord)
            self.assertEquals(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))
            self.assertEquals("_avro_namespace is deprecated, use _namespace instead", str(w[-1].message))

        self.assertEquals(store.get('legacy.TestRecord'), TestRecord)

    def test_duplicated_schema(self):
        # This is a duplication of test.namespaced_schemas.TestRecord
        # This should fail since we want to ensure that namespaced data is consistent in what classes we use
        @no_auto_store()
        class TestRecord(Record):
            _namespace = "my.namespace"
            a = Text()

        store = SchemaStore()

        store.add_record(TestRecord)
        store.add_record(namespaced_schemas.TestRecord)
        data = pyschema.core.dumps(TestRecord(a='testing'))
        self.assertRaises(ValueError,  pyschema.core.loads, data, store)

if __name__ == '__main__':
    unittest.main()
