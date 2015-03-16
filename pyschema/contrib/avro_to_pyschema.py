import warnings

warnings.warn(
    "pyschema.contrib.avro_to_pyschema is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.avro_to_pyschema package instead.",
    DeprecationWarning,
    stacklevel=2
)

import pyschema_extensions.avro_to_pyschema
from pyschema_extensions.avro_to_pyschema import *
