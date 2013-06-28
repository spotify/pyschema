from core import Field, ParseException
import binascii


class String(Field):
    def load(self, obj):
        if not isinstance(obj, unicode):
            raise ParseException("%r not a unicode object" % obj)
        return obj

    def dump(self, obj):
        if isinstance(obj, unicode):
            return obj
        else:
            try:
                return obj.decode('utf8')
            except:
                raise ParseException(
                    "%r is not a valid UTF-8 string" % obj
                )


class Blob(Field):
    """Binary data"""

    def load(self, obj):
        return binascii.a2b_base64(obj.encode('ascii'))

    def dump(self, obj):
        if isinstance(obj, unicode):
            obj = obj.encode('ascii')
        return binascii.b2a_base64(obj)


class List(Field):
    def __init__(self, field_type=String, *args, **kwargs):
        super(List, self).__init__(*args, **kwargs)
        self.field_type = field_type

    def load(self, obj):
        return [self.field_type.load(o) for o in obj]

    def dump(self, obj):
        return [self.field_type.dump(o) for o in obj]


class Enum(Field):
    field_type = String()  # don't change

    def __init__(self, values, *args, **kwargs):
        super(Enum, self).__init__(*args, **kwargs)
        self.values = set(values)

    def dump(self, obj):
        if obj not in self.values:
            raise ValueError(
                "%r is not an allowed value of Enum%r"
                % (obj, tuple(self.values)))
        return self.field_type.dump(obj)

    def load(self, obj):
        parsed = self.field_type.load(obj)
        if parsed not in self.values:
            raise ParseException(
                "Parsed value %r not in allowed value of Enum(%r)"
                % (parsed, tuple(self.values)))
        return parsed


class Integer(Field):
    def dump(self, obj):
        if not isinstance(obj, int):
            raise ValueError("%r is not a valid Integer" % (obj,))
        return obj

    def load(self, obj):
        return self.dump(obj)


class Boolean(Field):
    VALUE_MAP = {True: '1', 1: '1',
                 False: '0', 0: '0'}

    def dump(self, obj):
        if obj not in self.VALUE_MAP:
            raise ParseException(
                "Invalid value for Boolean field: %r" % obj)
        return bool(obj)

    def load(self, obj):
        return self.dump(obj)


class Float(Field):
    def dump(self, obj):
        return float(obj)

    def load(self, obj):
        return self.dump(obj)


class SubRecord(Field):
    "Field for storing :class:`record.Record`s as fields "
    "in other :class:`record.Record`s"

    def __init__(self, record_class, *args, **kwargs):
        super(SubRecord, self).__init__(*args, **kwargs)
        self._record_class = record_class

    def dump(self, obj):
        if not isinstance(obj, self._record_class):
            raise ParseException("%r is not a %r"
                                 % (obj, self._record_class))
        return obj._to_json_compatible()

    def load(self, obj):
        return self._record_class._from_json_compatible(obj)


# End of field type declarations
