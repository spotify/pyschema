from unittest import TestCase

from pyschema import Record, no_auto_store
from pyschema.types import Integer, Text, Float, Boolean, Date, DateTime
from pyschema_extensions import postgres


@no_auto_store()
class MyItem(Record):
    name = Text()
    value = Integer()
    dec = Float()
    flag = Boolean()
    date = Date()
    datehour = DateTime()



class TestPostgres(TestCase):
    def test_create_statement(self):
        statement = postgres.create_statement(MyItem, 'my_table')
        self.assertEquals("CREATE TABLE my_table (name TEXT, value BIGINT, dec FLOAT, flag BOOLEAN, date DATE, datehour TIMESTAMP WITHOUT TIME ZONE)", statement)

        statement = postgres.create_statement(MyItem)
        self.assertEquals("CREATE TABLE my_item (name TEXT, value BIGINT, dec FLOAT, flag BOOLEAN, date DATE, datehour TIMESTAMP WITHOUT TIME ZONE)", statement)
