import sys
from core import dumps, loads, ParseException


def mr_reader(job, input_stream):
    """ Converts a file object with json serialised pyschema records to a stream of pyschema objects

    Can be used as job.reader in luigi.hadoop.JobTask
    """
    for line in input_stream:
        yield loads(line),


def mr_writer(job, outputs, output_stream, stderr=sys.stderr):
    """ Writes a stream of json serialised pyschema Records to a file object

    Can be used as job.writer in luigi.hadoop.JobTask
    """
    for output in outputs:
        try:
            print >> output_stream, dumps(output)
        except ParseException, e:
            print >> stderr, e
            raise
