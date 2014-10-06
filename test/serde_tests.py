from unittest import TestCase
import pyschema
from pyschema.types import Bytes, Integer
from pyschema.core import ParseError


@pyschema.no_auto_store()
class CustomEncodedBytes(pyschema.Record):
    _ = Bytes(custom_encoding=True)  # stored b64-encoded


@pyschema.no_auto_store()
class AvroStandardBytes(pyschema.Record):
    _ = Bytes(custom_encoding=False)  # stored as utf8 encoded unicode code points < 256


class TestBytes(TestCase):
    def _roundtrip(self, schema, input_bytes, expected=None):
        record = schema()
        record._ = input_bytes
        serialized = pyschema.dumps(record, attach_schema_name=False)
        reborn = pyschema.loads(serialized, schema=schema)
        return serialized, reborn._

    def _all_bytes(self):
        return ''.join(chr(x) for x in xrange(256))  # all-byte blob

    def test_ascii_readable(self):
        ascii = "human readable string"
        serialized, reborn = self._roundtrip(AvroStandardBytes, ascii)
        self.assertTrue("human readable string" in serialized)
        self.assertEqual(reborn, ascii)

    def test_ascii_binary(self):
        ascii = "human readable string"
        serialized, reborn = self._roundtrip(CustomEncodedBytes, ascii)
        self.assertFalse("human readable string" in serialized)
        self.assertEqual(reborn, ascii)

    def test_utf8_readable(self):
        nihongo = u"\u65e5\u672c\u8a9e".encode('utf8')
        serialized, reborn = self._roundtrip(AvroStandardBytes, nihongo)
        self.assertFalse(nihongo in serialized)  # should NOT be represented like the contained utf8
        self.assertEqual(reborn, nihongo)

    def test_utf8_binary(self):
        nihongo = u"\u65e5\u672c\u8a9e".encode('utf8')
        serialized, reborn = self._roundtrip(CustomEncodedBytes, nihongo)
        self.assertFalse(nihongo in serialized)  # should NOT be represented like the contained utf8
        self.assertEqual(reborn, nihongo)

    def test_bytes_readable(self):
        # it's not readable but should still be roundtrippable
        bytes = self._all_bytes()
        serialized, reborn = self._roundtrip(AvroStandardBytes, bytes)
        self.assertEqual(reborn, bytes)

    def test_bytes_binary(self):
        bytes = self._all_bytes()
        serialized, reborn = self._roundtrip(CustomEncodedBytes, bytes)
        self.assertEqual(reborn, bytes)

    def test_invalid_unicode(self):
        # unicode values not allowed
        self.assertRaises(
            ValueError,
            lambda: self._roundtrip(CustomEncodedBytes, u'\u65e5\u672c\u8a9e')
        )
        self.assertRaises(
            ValueError,
            lambda: self._roundtrip(AvroStandardBytes, u'\u65e5\u672c\u8a9e')
        )


class TestExtraFields(TestCase):

    def test_fields(self):
        @pyschema.no_auto_store()
        class ValidRecord(pyschema.Record):
            field = Integer()

        line = '{"field": 8, "invalid_field": 0}'

        self.assertRaises(ParseError, lambda: pyschema.loads(line, schema=ValidRecord))
