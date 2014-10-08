import luigi
from luigi.hadoop import JobTask
from luigi.mock import MockFile

from ..common import BaseTest
from pyschema_extensions.luigi import mr_reader, mr_writer
import pyschema
from pyschema.types import Text, Integer


class InputRecord(pyschema.Record):
    foo = Text()
    bar = Integer()


class OutputRecord(pyschema.Record):
    foo = Text()
    barsum = Integer()


class MakeInputData(luigi.Task):
    def run(self):
        with self.output().open('w') as f:
            f.write(pyschema.dumps(
                InputRecord(
                    foo="yay",
                    bar=2
                )
            ))
            f.write("\n")
            f.write(pyschema.dumps(
                InputRecord(
                    foo="yay",
                    bar=3
                )
            ))
            f.write("\n")

    def output(self):
        return MockFile("MockInput")


class VanillaTask(JobTask):
    reader = mr_reader
    writer = mr_writer

    def mapper(self, record):
        yield record.foo, record.bar

    def reducer(self, key, values):
        yield OutputRecord(
            foo=key,
            barsum=sum(values)
        )

    def output(self):
        return MockFile("MyOutput1")

    def requires(self):
        return MakeInputData()


class LuigiOfflineMRTests(BaseTest):
    def setUp(self):
        pass

    def tearDown(self):
        MockFile.fs.remove("MyOutput1")

    def test_typed_mr(self):
        task = VanillaTask()
        luigi.build([task], local_scheduler=True)
        for line in task.output().open('r'):
            self.assertTrue("$schema" in line)
            rec = pyschema.loads(line)
            self.assertEquals(rec.foo, u"yay")
            self.assertEquals(rec.barsum, 5)

