import warnings

warnings.warn(
    "pyschema.contrib.jsonschema is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.jsonschema package instead.",
    DeprecationWarning,
    stacklevel=2
)

from pyschema_extensions.jsonschema import *
