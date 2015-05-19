import copy
from pyschema import types
import pyschema
import os
import sys
from collections import defaultdict

DEFAULT_INDENT = " " * 4


class SourceGenerationError(Exception):
    pass


def to_python_source(classes, indent=DEFAULT_INDENT):
    """Convert a set of pyschemas to executable python source code

    Currently supports all built-in types for basic usage.

    Notably not supported:
    * Maintaining class hierarchy
    * Methods, properties and non-field attributes
    * SELF-references
    """
    return header_source() + "\n" + classes_source(classes, indent)


RESERVED_KEYWORDS = [
    "and", "del", "from", "not", "while", "as", "elif",
    "global", "or", "with", "assert", "else", "if",
    "pass", "yield", "break", "except", "import",
    "print", "class", "exec", "in", "raise", "continue",
    "finally", "is", "return", "def", "for", "lambda", "try"
]


def make_safe(package_name):
    parts = package_name.split(".")
    for kw in RESERVED_KEYWORDS:
        while kw in parts:
            i = parts.index(kw)
            parts[i] = kw + "_"
    return ".".join(parts)


class PackageBuilder(object):
    def __init__(self, target_folder, parent_package, indent=DEFAULT_INDENT):
        self.target_folder = target_folder
        self.parent_package = parent_package
        self.indent = indent

    def get_namespace(self, schema):
        try:
            namespace = make_safe(schema._namespace)
        except AttributeError:
            namespace = None
        return namespace

    def get_namespace_clusters(self, all_classes):
        namespace_cluster = defaultdict(set)
        for c in all_classes:
            namespace = self.get_namespace(c)
            namespace_cluster[namespace].add(c)
        return namespace_cluster

    def format_definitions(self, classes):
        return "\n\n".join([_class_source(c, self.indent) for c in classes])

    def write_namespace_file(self, namespace, module_code):
        if not namespace:
            key = ['__init__']
        else:
            key = namespace.split('.')
        output_file = os.path.join(self.target_folder, *key) + '.py'
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_file, 'w') as out_fn:
            out_fn.write(module_code)

    def write_init_files(self):
        def touch_init_file_in_folder(folder):
            path = os.path.join(folder, '__init__.py')
            if not os.path.exists(path):
                open(path, 'w').close()

        touch_init_file_in_folder(self.target_folder)
        for root, dirs, _ in os.walk(self.target_folder):
            for d in dirs:
                touch_init_file_in_folder(os.path.join(root, d))

    def format_imports(self, imported_classes):
        if not imported_classes:
            return "\n"
        imported_namespaces = self.get_namespace_clusters(imported_classes)
        lines = []
        for namespace, schemas in imported_namespaces.iteritems():
            if self.parent_package:
                if namespace:
                    module = "{}.{}".format(self.parent_package, namespace)
                else:
                    module = self.parent_package
            else:
                if not namespace:
                    module = "."
                else:
                    module = namespace
            class_part = ", ".join(s.__name__ for s in schemas)
            lines.append("from {} import {}".format(module, class_part))
        return "\n".join(lines) + "\n\n"

    def _get_namespace_prefixes(self, namespaces):
        prefixes = []
        for n in namespaces:
            if n:
                parts = n.split(".")
                prefixes.append(".".join(parts[:-1]))
        return prefixes

    def from_classes_with_refs(self, classes):
        class_graph = CachedGraphTraverser()

        all_classes = set(classes)
        for c in classes:
            referenced_schemas = class_graph.find_descendants(c)
            all_classes |= set(referenced_schemas)

        namespace_cluster = self.get_namespace_clusters(all_classes)
        parent_namespaces = self._get_namespace_prefixes(namespace_cluster.keys())
        ordered_schemas = class_graph.get_reference_ordered_schemas(all_classes)

        # Since we don't want to use the previous cached results we create a new instance
        # This CachedGraphTraverser will only keep one-child depth for its find_descendants
        child_only_class_graph = CachedGraphTraverser()
        for namespace, classes in namespace_cluster.iteritems():
            inlined_classes = [c for c in ordered_schemas if c in classes]
            imported_classes = set()

            for inlined in inlined_classes:
                direct_references = child_only_class_graph.find_descendants(inlined, max_depth=1)
                imported_classes |= set([c for c in direct_references if c not in inlined_classes])

            module_code = (
                header_source() +
                self.format_imports(imported_classes) +
                self.format_definitions(inlined_classes)
            )
            if namespace not in parent_namespaces:
                filename = namespace
            else:
                filename = "{}.{}".format(namespace, "__init__")

            self.write_namespace_file(filename, module_code)
            self.write_init_files()


def to_python_package(classes, target_folder, parent_package=None, indent=DEFAULT_INDENT):
    '''
    This function can be used to build a python package representation of pyschema classes.
    One module is created per namespace in a package matching the namespace hierarchy.

    Args:
        classes: A collection of classes to build the package from
        target_folder: Root folder of the package
        parent_package: Prepended on all import statements in order to support absolute imports.
            parent_package is not used when building the package file structure
        indent: Indent level. Defaults to 4 spaces
    '''
    PackageBuilder(target_folder, parent_package, indent).from_classes_with_refs(classes)


def classes_source(classes, indent=DEFAULT_INDENT):
    all_classes = set(classes)
    class_graph = CachedGraphTraverser()
    for c in classes:
        referenced_schemas = class_graph.find_descendants(c)
        all_classes |= set(referenced_schemas)

    ordered = class_graph.get_reference_ordered_schemas(all_classes)
    return "\n\n".join([_class_source(c, indent) for c in ordered])


def header_source():
    """Get the required header for generated source"""
    return (
        "import pyschema\n"
        "from pyschema.types import *\n"
        "from pyschema.core import NO_DEFAULT\n"
    )


def _class_source(schema, indent):
    """Generate Python source code for one specific class

    Doesn't include or take into account any dependencies between record types
    """

    def_pattern = (
        "class {class_name}(pyschema.Record):\n"
        "{indent}# WARNING: This class was generated by pyschema.to_python_source\n"
        "{indent}# there is a risk that any modification made to this class will be overwritten\n"
        "{optional_namespace_def}"
        "{field_defs}\n"
    )
    if hasattr(schema, '_namespace'):
        optional_namespace_def = "{indent}_namespace = {namespace!r}\n".format(
            namespace=schema._namespace, indent=indent)
    else:
        optional_namespace_def = ""

    field_defs = [
        "{indent}{field_name} = {field!r}".format(field_name=field_name, field=field, indent=indent)
        for field_name, field in schema._fields.iteritems()
    ]
    if not field_defs:
        field_defs = ["{indent}pass".format(indent=indent)]

    return def_pattern.format(
        class_name=schema._schema_name,
        optional_namespace_def=optional_namespace_def,
        field_defs="\n".join(field_defs),
        indent=indent
    )


class CachedGraphTraverser(object):
    def __init__(self):
        self.descendants = {}
        self.started = set()

    def find_descendants(self, a, max_depth=sys.getrecursionlimit()):
        if a in self.descendants:
            # fetch from cache
            return self.descendants[a]
        self.started.add(a)
        subs = set()
        if max_depth > 0:
            if pyschema.ispyschema(a):
                for _, field in a._fields.iteritems():
                    subs |= self.find_descendants(field, max_depth)
                self.descendants[a] = subs
            elif isinstance(a, types.List):
                subs |= self.find_descendants(a.field_type, max_depth)
            elif isinstance(a, types.Map):
                subs |= self.find_descendants(a.value_type, max_depth)
            elif isinstance(a, types.SubRecord):
                subs.add(a._schema)
                if a._schema not in self.started:  # otherwise there is a circular reference
                    subs |= self.find_descendants(a._schema, max_depth-1)
            self.started.remove(a)
        return subs

    def get_reference_ordered_schemas(self, schema_set):
        for schema in schema_set:
            self.find_descendants(schema)
        descendants = copy.deepcopy(self.descendants)  # a working copy

        ordered_output = []
        while descendants:
            leaves = []
            for root, referenced in descendants.iteritems():
                if len(referenced) == 0:
                    leaves.append(root)

            if not leaves:
                raise SourceGenerationError("Circular reference in input schemas, aborting")
            ordered_output += leaves
            for leaf in leaves:
                # remove all leaves
                descendants.pop(leaf)
                for root, referenced in descendants.iteritems():
                    if leaf in referenced:
                        referenced.remove(leaf)
        return ordered_output
