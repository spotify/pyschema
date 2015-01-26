import warnings

warnings.warn(
    "pyschema.contrib.postgres is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.postgres package instead.",
    DeprecationWarning,
    stacklevel=2
)

from pyschema_extensions.postgres import *
