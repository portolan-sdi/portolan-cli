"""The Portolan library without the command line or the geospatial stack."""

from __future__ import annotations

import logging

# A library adds only a NullHandler. The application configures logging.
logging.getLogger(__name__).addHandler(logging.NullHandler())
