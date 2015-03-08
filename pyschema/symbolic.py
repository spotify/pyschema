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


class IntegerValued(Expression):
    def __init__(self, proxy):
        self.proxy = proxy

    def mul2(self, other, reverse):
        if isinstance(other, int):
            other = IntegerConstant(other)
        if reverse:
            left, right = other, self
        else:
            left, right = self, other

        if isinstance(other, IntegerValued):
            return IntegerValued(Multiplication(left, right))
        assert False

    def __mul__(self, other):
        return self.mul2(other, reverse=False)

    def __rmul__(self, other):
        return self.mul2(other, reverse=True)

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
    print 5 * mytable.i * 10 * other.i
