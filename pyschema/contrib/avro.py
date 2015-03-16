import warnings

warnings.warn(
    "pyschema.contrib.avro is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.avro package instead.",
    DeprecationWarning,
    stacklevel=2
)

import pyschema_extensions.avro
from pyschema_extensions.avro import *
