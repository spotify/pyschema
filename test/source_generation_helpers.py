from pyschema import Record, List, SubRecord, Map, no_auto_store


@no_auto_store()
class B(Record):
    pass


@no_auto_store()
class A(Record):
    b = List(Map(SubRecord(B)))
