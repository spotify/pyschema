
PySchema
========

PySchema is a library for Python class declaration with typed fields
that can be introspected and have data contracts associated with them.
This allows for better data integrity checks when
serializing/deserializing data and safe interaction with external tools
that require typed data.

The foremost design principle when creating the library was to keep the
definitions very concise and easy to read. Inspiration was taken from
Django's ORM and the main use cases in mind has been database
interaction (Postgres) and Apache Avro schema/datum generation.

It has been tested on Python 2.6 and Python 2.7

API Reference
~~~~~~~~~~~~~

                
.. toctree::
   _api/pyschema

.. toctree::
   _api_extensions/pyschema_extensions

                
Usage
-----

The ``Record`` base class is the easiest way to define schemas. Typed
fields in the schema are defined using subclasses of ``Field``. Standard
field types are defined in ``pyschema.types`` and are also aliased in
the pyschema package.

.. code:: python

    from pyschema import Record, dumps, loads
    from pyschema.types import *
Declaration
^^^^^^^^^^^

To define a schema, use a Python class declaration inheriting from
``pyschema.Record``.

.. code:: python

    class MyRecord(Record):
        foo = Text()
        bar = Integer()
Class instantiation
^^^^^^^^^^^^^^^^^^^

Like a typical Python class instantiation. All fields are keyword
arguments to the constructor of the record.

.. code:: python

    r = MyRecord(foo="hej", bar=3)
Member access
^^^^^^^^^^^^^

.. code:: python

    r.foo



.. parsed-literal::

    'hej'



Default string representation/repr
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    print r

.. parsed-literal::

    MyRecord(foo='hej', bar=3)


Simple json serialization
^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a json compatible string representing the object. A special
``$schema`` field is added to the json to allow parsing of the record
without prior knowledge of the which schema to use. The name of this
special field can be set to something else using the
``pyschema.core.set_schema_name_field``

.. code:: python

    s = dumps(r)
    print s

.. parsed-literal::

    {"$schema": "MyRecord", "foo": "hej", "bar": 3}


####... and deserialization

.. code:: python

    o = loads(s)
    print o.bar

.. parsed-literal::

    3


Built-in types
--------------

PySchema comes with a standard set of field types that can be used to
represent the most commonly used data types

-  ``Text``
-  ``Integer``
-  ``Float``
-  ``Bytes`` - for binary data, the equivalent of Python < 3 ``str`` or
   Python 3 ``bytes``
-  ``Boolean`` - True or False
-  ``Date`` - ``datetime.date`` objects
-  ``DateTime`` - ``datetime.datetime`` objects
-  ``Enum`` - only allows a preset of text values (specified as an
   arguemnt to the constructor)
-  ``List``
-  ``Map``
-  ``SubRecord``

Complex types
~~~~~~~~~~~~~

Some types function as containers for other types.

List
^^^^

Lists allow storage of ordered sequences of a single type of data,
specified as an argument to the field constructor

.. code:: python

    class RecordWithList(Record):
        foo = List(Integer())
    
    RecordWithList(foo=[1, 2, 3])



.. parsed-literal::

    RecordWithList(foo=[1, 2, 3])



Map
^^^

Storage for dictionaries mapping from strings to values of a single
type, specified as an argument to the field constructor

.. code:: python

    class RecordWithMap(Record):
        foo = Map(Boolean())
    
    RecordWithMap(foo={u"word": True})



.. parsed-literal::

    RecordWithMap(foo={u'word': True})



SubRecord
^^^^^^^^^

SubRecords allow for nesting of records, i.e. storing records of some
sort as fields in other records. SubRecord takes an argument being the
schema (i.e. Record class) of the intended stored object. Recursive
nesting can also be used by supplying ``pyschema.SELF`` as the schema
type to SubRecord, in which case the field accepts records of the parent
record type.

.. code:: python

    class NestedRecord(Record):
        foo = SubRecord(MyRecord)  # MyRecord is defined above...
        
    NestedRecord(foo=MyRecord(foo="foo", bar=5))



.. parsed-literal::

    NestedRecord(foo=MyRecord(foo='foo', bar=5))



.. code:: python

    class NestedSelfRecord(Record):
        foo = SubRecord(SELF)
        bar = Text()
    
    NestedSelfRecord(foo=NestedSelfRecord(foo=None, bar="Second"), bar="First")



.. parsed-literal::

    NestedSelfRecord(foo=NestedSelfRecord(foo=None, bar='Second'), bar='First')



Complex types are field types just like any other, so they can be
combined to create complex data structures

.. code:: python

    class Part(Record):
        value = Integer()
        good = Boolean()
        attributes = List(Text())
    
    class AdvancedRecord(Record):
        name = Text()
        parts = Map(SubRecord(Part))
        
    AdvancedRecord(
        name=u"tool_1",
        parts={
            u"moo": Part(
                value=u"buzz",
                good=False,
                attributes=["something", "other"]
            )
        }
    )



.. parsed-literal::

    AdvancedRecord(name=u'tool_1', parts={u'moo': Part(value=u'buzz', good=False, attributes=['something', 'other'])})



Defaults
~~~~~~~~

All fields are optional in the constructor, left-out fields are ``None``
by default, except for the ``Map`` and ``List`` types where they default
to their respective empty containers ``{}`` and ``[]``.

MyRecord(bar=10)

.. code:: python

    class OtherRecord(Record):
        bar = Map(Float())
        baz = List(Integer())
.. code:: python

    OtherRecord()



.. parsed-literal::

    OtherRecord(bar={}, baz=[])



Fails at serialization time when types don't match

.. code:: python

    broken_record = MyRecord(foo=5) # object creation works with any types (to allow for temporary unallowed values)
.. code:: python

    print broken_record  # repr format also still works

.. parsed-literal::

    MyRecord(foo=5, bar=None)


.. code:: python

    print dumps(broken_record)  # raises an Exception because 5 isn't a text format

::


    ---------------------------------------------------------------------------
    ValueError                                Traceback (most recent call last)

    <ipython-input-17-92114c8b8749> in <module>()
    ----> 1 print dumps(broken_record)  # raises an Exception because 5 isn't a text format
    

    /Users/freider/Code/spotify/pyschema/pyschema/core.py in dumps(obj, attach_schema_name)
        494 
        495 def dumps(obj, attach_schema_name=True):
    --> 496     json_dct = to_json_compatible(obj)
        497     if attach_schema_name:
        498         json_dct[SCHEMA_FIELD_NAME] = obj._schema_name


    /Users/freider/Code/spotify/pyschema/pyschema/core.py in to_json_compatible(record)
        384         val = getattr(record, fname)
        385         if val is not None:
    --> 386             d[fname] = f.dump(val)
        387     return d
        388 


    /Users/freider/Code/spotify/pyschema/pyschema/types.pyc in dump(self, obj)
         34             except:
         35                 raise ValueError(
    ---> 36                     "%r is not a valid UTF-8 string" % obj
         37                 )
         38 


    ValueError: 5 is not a valid UTF-8 string


Extending PySchema
------------------

Create new custom ``Field`` types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    import datetime
    
    class Date(Field):
        def dump(self, obj):
            return obj.strftime("%Y-%m-%d")
        
        def load(self, text):
            return datetime.date(*(int(part) for part in text.split('-')))
.. code:: python

    class MyOtherRecord(Record):
        date = Date()
.. code:: python

    s = dumps(MyOtherRecord(date=datetime.date(2013, 10, 7)))
    print "Serialized:", s
    print "Reloaded:", repr(loads(s).date)

.. parsed-literal::

    Serialized: {"date": "2013-10-07", "$schema": "MyOtherRecord"}
    Reloaded: datetime.date(2013, 10, 7)


Add mixins on existing field types to simplify adding functionality while maintaining OO structure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    Text.postgres_type = "TEXT"
    Integer.postgres_type = "INTEGER"
    
    @List.mixin
    class ListPostgresMixin:
        @property
        def postgres_type(self):
            return self.field_type.postgres_type + " ARRAY"
.. code:: python

    def create_table_from_record(schema):
        parts = []
        for name, field_type in schema._fields.iteritems():
            parts.append("%s %s" % (name, field_type.postgres_type))
        return "CREATE TABLE %s (" % (schema._schema_name,) + ", ".join(parts) + ")"
.. code:: python

    class MyTable(Record):
        list_name = Text()
        numbers = List(Integer())
    
    create_table_from_record(MyTable)



.. parsed-literal::

    'CREATE TABLE MyTable (list_name TEXT, numbers INTEGER ARRAY)'



The following will trigger an error since we haven't mixed in the
``postgres_type`` field for the ``Map`` field type in this example.

.. code:: python

    class Impossibru(Record):
        numbers = Map(Integer())
        
    create_table_from_record(Impossibru)

::


    ---------------------------------------------------------------------------
    AttributeError                            Traceback (most recent call last)

    <ipython-input-24-65f7b1968e1a> in <module>()
          2     numbers = Map(Integer())
          3 
    ----> 4 create_table_from_record(Impossibru)
    

    <ipython-input-22-61f11a5f0ce7> in create_table_from_record(schema)
          2     parts = []
          3     for name, field_type in schema._fields.iteritems():
    ----> 4         parts.append("%s %s" % (name, field_type.postgres_type))
          5     return "CREATE TABLE %s (" % (schema._schema_name,) + ", ".join(parts) + ")"


    AttributeError: 'Map' object has no attribute 'postgres_type'


Under the hood
--------------

In this section, a brief explanation of the underlying architecture of
the package is presented.

Declaration
~~~~~~~~~~~

PySchema utilizes a Schema metaclass for the Record class that hooks
into the class declaration logic of the python interpreter.

When a subclass of Record is *declared*, the metaclass will go through
the class properties and create some helper variables needed for schema
introspection and general setup. To be able to keep ordering of fields,
a counter is increased every time a Field is declared and this is used a
the sorting key in the ordered schema.

The metaclass is responsible for setting up the following magic
variables on the schema *class*: \* ``_fields`` - contains an
OrderedDict of (*name*, *field*) mappings, where *name* is the field
name and *field* is the Field instance, i.e. the type definition
instance for the field. E.g. ``("foo", Integer(size=4))`` \*
``_schema_name`` - the name of the schema. Typically the same as the
class name.

Instantiation
~~~~~~~~~~~~~

When a Record is instantiated, a new object is created where each field
is filled with its default value.
