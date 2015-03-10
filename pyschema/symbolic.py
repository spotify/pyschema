import pyschema
from abc import ABCMeta, abstractmethod, abstractproperty


class Expression(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def render():
        pass

    def __repr__(self):
        return self.render("default")


class Symbol(Expression):
    def __init__(self, name, namespace=None):
        self.name = name
        self.namespace = namespace

    def render(self, syntax):
        if self.namespace:
            return "{}.{}".format(self.namespace, self.name)
        return self.name


class StandardMathArithmeticBinaryOperation(object):
    @abstractproperty
    def operator(self):
        pass

    def __init__(self, left, right):
        super(StandardMathArithmeticBinaryOperation, self).__init__()
        self.left = left
        self.right = right

    def render(self, syntax):
        return "({left}) {operator} ({right})".format(
            left=self.left.render(syntax),
            right=self.right.render(syntax),
            operator=self.operator
        )


class Multiplication(StandardMathArithmeticBinaryOperation):
    operator = "*"


class Addition(StandardMathArithmeticBinaryOperation):
    operator = "+"


class Subtraction(StandardMathArithmeticBinaryOperation):
    operator = "-"


class Division(StandardMathArithmeticBinaryOperation):
    operator = "/"


class EqualityCheck(Expression):
    def __init__(self, left, right):
        super(EqualityCheck, self).__init__()
        self.left = left
        self.right = right

    def render(self, syntax):
        if syntax == "sql":
            return "({}) = ({})".format(
                self.left.render(syntax),
                self.right.render(syntax)
            )
        return "({}) == ({})".format(
            self.left.render(syntax),
            self.right.render(syntax)
        )


class BooleanValued(Expression):
    def __init__(self, proxy):
        self.proxy = proxy

    def render(self, syntax):
        return self.proxy.render(syntax)

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

    def binary_operation(self, operator_class, other, reverse):
        if reverse:
            left, right = other, self
        else:
            left, right = self, other
        return operator_class(left, right)

    def mul(self, other, reverse):
        other = self.convert_compatible(other)
        return IntegerValued(self.binary_operation(Multiplication, other, reverse))

    def __mul__(self, other):
        return self.mul(other, reverse=False)

    def __rmul__(self, other):
        return self.mul(other, reverse=True)

    def add(self, other, reverse):
        other = self.convert_compatible(other)
        return IntegerValued(self.binary_operation(Addition, other, reverse))

    def __add__(self, other):
        return self.add(other, reverse=False)

    def __radd__(self, other):
        return self.add(other, reverse=True)

    def div(self, other, reverse):
        other = self.convert_compatible(other)
        return IntegerValued(self.binary_operation(Division, other, reverse))

    def __div__(self, other):
        return self.div(other, reverse=False)

    def __rdiv__(self, other):
        return self.div(other, reverse=True)

    def sub(self, other, reverse):
        other = self.convert_compatible(other)
        return IntegerValued(self.binary_operation(Subtraction, other, reverse))

    def __sub__(self, other):
        return self.sub(other, reverse=False)

    def __rsub__(self, other):
        return self.sub(other, reverse=True)

    def eq(self, other, reverse):
        other = self.convert_compatible(other)
        return BooleanValued(self.binary_operation(EqualityCheck, other, reverse))

    def __eq__(self, other):
        return self.eq(other, reverse=False)

    def __req__(self, other):
        return self.eq(other, reverse=True)

    def render(self, syntax):
        return self.proxy.render(syntax)

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

    def render(self, syntax):
        return str(self.value)


# convenience wrappers
@IntegerValued.mixin
class IntegerSymbol(Symbol):
    pass


SYMBOLMAP = {
    pyschema.Integer: IntegerSymbol
}


class View(dict):
    def __init__(self, fields):
        self._fields = fields

    def __getattr__(self, name):
        if name not in self._fields:
            raise AttributeError("View has no attribute {}".format(
                self._name, pyschema.core.get_full_name(self._schema), name))
        return self._fields[name]

    def __getitem__(self, expression):
        if isinstance(expression, Expression):
            return Selection(self, expression)


class Table(View):
    def __init__(self, schema, ref_name):
        self._schema = schema
        self._name = ref_name
        fields = {}

        for field_name, field in schema._fields.items():
            symcls = SYMBOLMAP.get(type(field))
            symcls(ref_name, ref_name)
            fields[field_name] = symcls(ref_name + "." + field_name)

        super(Table, self).__init__(fields)


class Selection(View):
    def __init__(self, table, expression):
        self.table = table
        self.expression = expression

    def sql(self):
        return "SELECT * FROM {table._name} WHERE {expr}".format(
            table=self.table,
            expr=self.expression.render("sql")
        )

    def pig(self):
        return "FILTER {table._name} BY {expr}".format(
            table=self.table,
            expr=self.expression.render("pig")
        )


class InnerJoin(View):
    def __init__(self, left, right, condition):
        self.left = left
        self.right = right
        self.join_condition = condition
        all_fields = dict(self.left._fields.items() + self.right._fields.items())
        super(InnerJoin, self).__init__(all_fields)

    def sql(self):
        selected_fields = ", ".join(f.render("sql") for f in self._fields.values())

        return """SELECT {fields} FROM {left._name}
INNER JOIN {right._name}
ON {join_condition}
""".format(
            fields=selected_fields,
            left=self.left,
            right=self.right,
            join_condition=self.join_condition.render("sql")
        )


if __name__ == "__main__":
    class Foo(pyschema.Record):
        _namespace = "hello"
        i = pyschema.Integer()
        j = pyschema.Integer()

    mytable = Table(Foo, "mytable")
    other = Table(Foo, "other")
    print "Traceable types:"
    print 5 * mytable.i - 1 == (mytable.i + 5 * 2) / 2
    print
    print "SQL select + filter"
    print mytable[mytable.i == other.j * 2].sql()
    print
    print "Pig filter:"
    print mytable[mytable.i == other.j * 2].pig()
    print
    print "SQL join"
    join = InnerJoin(mytable, other, condition=mytable.i == other.j)
    print join.sql()
