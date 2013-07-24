from unittest import TestCase


class BaseTest(TestCase):
    def recursive_compare(self, node1, node2):
        if isinstance(node1, dict):
            for k, v1 in node1.iteritems():
                v2 = node2[k]
                self.recursive_compare(v1, v2)
        elif isinstance(node1, list):
            for i, v1 in enumerate(node1):
                v2 = node2[i]
                self.recursive_compare(v1, v2)
        else:
            self.assertEquals(node1, node2)