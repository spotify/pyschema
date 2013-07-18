from unittest import TestCase

from pyschema import Record, no_auto_store
from pyschema.types import Integer, Text
from pyschema.contrib import postgres


@no_auto_store()
class Item(Record):
    name = Text()
    value = Integer()


class TestPostgres(TestCase):
    def test_create_statement(self):
        statement = postgres.create_statement(Item)
