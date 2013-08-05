# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import sys
from pyschema.core import dumps, loads, ParseError


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
        except ParseError, e:
            print >> stderr, e
            raise


def typeless_mr_writer(job, outputs, output_stream, stderr=sys.stderr):
    """ Like `mr_writer` but doesn't include the schema identifying $record_name field

    Can be used as job.writer in luigi.hadoop.JobTask
    """
    for output in outputs:
        try:
            print >> output_stream, dumps(output, attach_record_name=False)
        except ParseError, e:
            print >> stderr, e
            raise


def typed_mr_reader(record_class):
    """ Function factory for an mr_reader that enforces the record class to `record_class`. Because of that, serialized input records don't have to contain $record_name entires.
        """
    def mr_reader(job, input_stream):
        for line in input_stream:
            yield loads(line, record_class=record_class),
    return mr_reader

