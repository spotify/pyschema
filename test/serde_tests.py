from unittest import TestCase
import pyschema
from pyschema.types import Bytes, Integer
from pyschema.core import ParseError


@pyschema.no_auto_store()
class BinaryData(pyschema.Record):
    _ = Bytes(human_readable=False)  # stored b64-encoded


@pyschema.no_auto_store()
class ReadableData(pyschema.Record):
    _ = Bytes(human_readable=True)  # stored as utf8 encoded unicode code points < 256


class TestBytes(TestCase):
    def _roundtrip(self, schema, input_bytes, expected=None):
        record = schema()
        record._ = input_bytes
        serialized = pyschema.dumps(record, attach_record_name=False)
        reborn = pyschema.loads(serialized, record_class=schema)
        return serialized, reborn._

    def _all_bytes(self):
        return ''.join(chr(x) for x in xrange(256))  # all-byte blob

    def test_ascii_readable(self):
        ascii = "human readable string"
        serialized, reborn = self._roundtrip(ReadableData, ascii)
        self.assertTrue("human readable string" in serialized)
        self.assertEqual(reborn, ascii)

    def test_ascii_binary(self):
        ascii = "human readable string"
        serialized, reborn = self._roundtrip(BinaryData, ascii)
        self.assertFalse("human readable string" in serialized)
        self.assertEqual(reborn, ascii)

    def test_utf8_readable(self):
        nihongo = u"\u65e5\u672c\u8a9e".encode('utf8')
        serialized, reborn = self._roundtrip(ReadableData, nihongo)
        self.assertFalse(nihongo in serialized)  # should NOT be represented like the contained utf8
        self.assertEqual(reborn, nihongo)

    def test_utf8_binary(self):
        nihongo = u"\u65e5\u672c\u8a9e".encode('utf8')
        serialized, reborn = self._roundtrip(BinaryData, nihongo)
        self.assertFalse(nihongo in serialized)  # should NOT be represented like the contained utf8
        self.assertEqual(reborn, nihongo)

    def test_bytes_readable(self):
        # it's not readable but should still be roundtrippable
        bytes = self._all_bytes()
        serialized, reborn = self._roundtrip(ReadableData, bytes)
        self.assertEqual(reborn, bytes)

    def test_bytes_binary(self):
        bytes = self._all_bytes()
        serialized, reborn = self._roundtrip(BinaryData, bytes)
        self.assertEqual(reborn, bytes)

    def test_invalid_unicode(self):
        # unicode values not allowed
        self.assertRaises(
            ValueError,
            lambda: self._roundtrip(BinaryData, u'\u65e5\u672c\u8a9e')
        )
        self.assertRaises(
            ValueError,
            lambda: self._roundtrip(ReadableData, u'\u65e5\u672c\u8a9e')
        )


class TestExtraFields(TestCase):

    def test_fields(self):
        @pyschema.no_auto_store()
        class ValidRecord(pyschema.Record):
            field = Integer()

        line = '{"field": 8, "invalid_field": 0}'

        self.assertRaises(ParseError, lambda: pyschema.loads(line, record_class=ValidRecord))
