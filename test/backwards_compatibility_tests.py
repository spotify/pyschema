from unittest import TestCase
import warnings
from pyschema_extensions import avro
from pyschema import core, types


class ContribIsImportableAndRaisesWarnings(TestCase):
    """Tests backwards compatiblity with pre-2.0 versions' pyschema.contrib package

    The pyschema.contrib module and this test class should be removed as soon as possible
    and shouldn't be used. Use pyschema_extensions instead
    """

    def assert_import_warns(self, module_name, warning_type):
        with warnings.catch_warnings(record=True) as warninglist:
            warnings.simplefilter("always")
            __import__(module_name)
            self.assertTrue(any(isinstance(w.message, DeprecationWarning) for w in warninglist))

    def avro_test(self):
        self.assert_import_warns("pyschema.contrib.avro", DeprecationWarning)

    def postgres_test(self):
        self.assert_import_warns("pyschema.contrib.postgres", DeprecationWarning)

    def jsonschema_test(self):
        self.assert_import_warns("pyschema.contrib.jsonschema", DeprecationWarning)

    def luigi_test(self):
        self.assert_import_warns("pyschema.contrib.luigi", DeprecationWarning)

    def avro_to_pyschema_test(self):
        self.assert_import_warns("pyschema.contrib.avro_to_pyschema", DeprecationWarning)


@core.no_auto_store()
class Foo(core.Record):
    i = types.Text()


class LoadsTakesRecordClassArgument(TestCase):
    """Ensure that the loads methods accept `record_class`

    As a (deprecated) alternative to `schema`
    """

    def core_test_loads(self):
        s = '''{"i": "value"}'''
        with warnings.catch_warnings(record=True) as warninglist:
            warnings.simplefilter("always")
            obj = core.loads(s, record_class=Foo)
        self.assertTrue(isinstance(warninglist[0].message, DeprecationWarning))
        self.assertEqual(
            obj,
            core.loads(s, schema=Foo)
        )

    def avro_test_loads(self):
        s = '''{"i": {"string": "value"}}'''
        with warnings.catch_warnings(record=True) as warninglist:
            warnings.simplefilter("always")
            obj = avro.loads(s, record_class=Foo)
        self.assertTrue(isinstance(warninglist[0].message, DeprecationWarning))
        self.assertEqual(
            obj,
            avro.loads(s, schema=Foo)
        )
