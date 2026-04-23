"""Resume logic for interrupted ArcGIS extractions.

This module re-exports resume utilities from the common module
for backwards compatibility.
"""

from portolan_cli.extract.common.resume import (
    ResumeState,
    get_resume_state,
    should_process_layer,
)

__all__ = [
    "ResumeState",
    "get_resume_state",
    "should_process_layer",
]
