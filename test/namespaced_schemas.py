from pyschema import Record, no_auto_store
from pyschema.types import Text


@no_auto_store()
class TestRecord(Record):
    _namespace = "my.namespace"
    a = Text()
