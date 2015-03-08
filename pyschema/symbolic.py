import pyschema
from abc import ABCMeta, abstractmethod


class Expression(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def render():
        pass

    def __repr__(self):
        return self.render()


class Symbol(Expression):
    def __init__(self, name, namespace=None):
        self.name = name
        self.namespace = namespace

    def render(self):
        if self.namespace:
            return "{}.{}".format(self.namespace, self.name)
        return self.name


class Multiplication(Expression):
    def __init__(self, left, right):
        super(Multiplication, self).__init__()
        self.left = left
        self.right = right

    def render(self):
        return "({}) * ({})".format(
            self.left.render(),
            self.right.render()
        )


class EqualityCheck(Expression):
    def __init__(self, left, right):
        super(EqualityCheck, self).__init__()
        self.left = left
        self.right = right

    def render(self):
        return "({}) == ({})".format(
            self.left.render(),
            self.right.render()
        )


class BooleanValued(Expression):
    def __init__(self, proxy):
        self.proxy = proxy

    def render(self):
        return self.proxy.render()

    def __repr__(self):
        return "BooleanValued: " + super(BooleanValued, self).__repr__()

    @classmethod
    def mixin(self, cls):
        "Use as decorator for class to get this property"
        class Wrapper(IntegerValued):
            def __new__(wrappercls, *args, **kwargs):
                return BooleanValued(cls(*args, **kwargs))
        return Wrapper


class IntegerValued(Expression):
    def __init__(self, proxy):
        self.proxy = proxy

    def convert_compatible(self, other):
        if isinstance(other, int):
            return IntegerConstant(other)
        elif isinstance(other, IntegerValued):
            return other
        assert False

    def mul(self, other, reverse):
        other = self.convert_compatible(other)
        if reverse:
            left, right = other, self
        else:
            left, right = self, other
        return IntegerValued(Multiplication(left, right))

    def __mul__(self, other):
        return self.mul(other, reverse=False)

    def __rmul__(self, other):
        return self.mul(other, reverse=True)

    def eq(self, other, reverse):
        other = self.convert_compatible(other)
        if reverse:
            left, right = other, self
        else:
            left, right = self, other
        return BooleanValued(EqualityCheck(left, right))

    def __eq__(self, other):
        return self.eq(other, reverse=False)

    def __req__(self, other):
        return self.eq(other, reverse=True)

    def render(self):
        return self.proxy.render()

    def __repr__(self):
        return "IntegerValued: " + super(IntegerValued, self).__repr__()

    @classmethod
    def mixin(self, cls):
        "Use as decorator for class to get this property"
        class Wrapper(IntegerValued):
            def __new__(wrappercls, *args, **kwargs):
                return IntegerValued(cls(*args, **kwargs))
        return Wrapper


@IntegerValued.mixin
class IntegerConstant(Expression):
    def __init__(self, value):
        self.value = value

    def render(self):
        return str(self.value)


# convenience wrappers
@IntegerValued.mixin
class IntegerSymbol(Symbol):
    pass


SYMBOLMAP = {
    pyschema.Integer: IntegerSymbol
}


class Table(dict):
    def __getattr__(self, name):
        return self[name]


def symbols(schema, object_name):
    syms = Table()
    for name, field in schema._fields.items():
        symcls = SYMBOLMAP.get(type(field))
        if not symcls:
            assert False
        else:
            setattr(syms, name, symcls(name, object_name))

    return syms

if __name__ == "__main__":
    class Foo(pyschema.Record):
        i = pyschema.Integer()

    mytable = symbols(Foo, "mytable")
    other = symbols(Foo, "other")
    print 5 * mytable.i == mytable.i
