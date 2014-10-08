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

"""Basic utilities for using PySchema in Luigi's Python MR lib
"""
import sys
from pyschema import core


def mr_reader(job, input_stream, loads=core.loads):
    """ Converts a file object with json serialised pyschema records
        to a stream of pyschema objects

    Can be used as job.reader in luigi.hadoop.JobTask
    """
    for line in input_stream:
        yield loads(line),


def mr_writer(job, outputs, output_stream,
              stderr=sys.stderr, dumps=core.dumps):
    """ Writes a stream of json serialised pyschema Records to a file object

    Can be used as job.writer in luigi.hadoop.JobTask
    """
    for output in outputs:
        try:
            print >> output_stream, dumps(output)
        except core.ParseError, e:
            print >> stderr, e
            raise
