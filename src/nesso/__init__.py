"""Description placeholder (required by Flit)."""

import importlib.metadata

from .nesso import cli

# This allows specifying the version in a single place (`pyproject.toml`).
# See https://gist.github.com/benkehoe/066a73903e84576a8d6d911cfedc2df6.
try:
    # __package__ allows for the case where __name__ is "__main__"
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.0.0"
