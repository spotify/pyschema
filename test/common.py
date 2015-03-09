from unittest import TestCase


class BaseTest(TestCase):
    def recursive_compare(self, node1, node2, path=""):
        if isinstance(node1, dict):
            for k, v1 in node1.iteritems():
                try:
                    v2 = node2[k]
                except (KeyError, TypeError):
                    self.fail("{0} not in second's {1} ({2})".format(k, path, node2))

                self.recursive_compare(v1, v2, "{0}.{1}".format(path, k))
        elif isinstance(node1, list):
            for i, v1 in enumerate(node1):
                try:
                    v2 = node2[i]
                except IndexError:
                    self.fail("{0} not in second's {1}".format(i, path))
                self.recursive_compare(v1, v2, "{0}[{1}]".format(path, i))
        else:
            self.assertEquals(node1, node2, u"{0}: {1} != {2}".format(path, node1, node2).encode("utf8"))
