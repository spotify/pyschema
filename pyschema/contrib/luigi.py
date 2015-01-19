import warnings

warnings.warn(
    "pyschema.contrib.luigi is deprecated and will be removed.\n"
    "Please use the pyschema_extensions.luigi package instead.",
    FutureWarning,
    stacklevel=2
)

from pyschema_extensions.luigi import *
