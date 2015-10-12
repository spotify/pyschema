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

from setuptools import setup

setup(
    name="pyschema",
    version="2.4.0",
    description="Schema definition and serialisation library",
    author="Elias Freider",
    author_email="freider@spotify.com",
    url="http://github.com/spotify/pyschema",
    packages=[
        "pyschema",
        "pyschema_extensions",
        "pyschema.contrib",  # deprecated package, replaced by pyschema_extensions
    ],
    keywords=["schema", "avro", "postgres", "json"],
    install_requires=[
        "simplejson"
    ],
    namespace_packages=[
        "pyschema_extensions"
    ],
    test_suite="nose.collector"
)
