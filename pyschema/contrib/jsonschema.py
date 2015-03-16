import warnings

warnings.warn(
    "pyschema.contrib.jsonschema is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.jsonschema package instead.",
    DeprecationWarning,
    stacklevel=2
)

import pyschema_extensions.jsonschema
from pyschema_extensions.jsonschema import *
