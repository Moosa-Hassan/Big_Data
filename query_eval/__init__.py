"""Research-grade evaluation package for compressed-domain LogLite querying.

This package is the authoritative implementation for part 2 of the project. It
replaces notebook-centric evaluation logic with reusable, documented Python
modules that can be executed from scripts, tests, and notebooks.

The package deliberately separates:
- dataset metadata and staging
- artifact generation
- query definition
- mode dispatch
- metrics and profiling
- result persistence and aggregation

Notebooks should import from this package rather than re-implementing logic in
cells. The current codec source of truth remains the C++ implementation under
`loglite/LogLite-B`, while the Python search backends in this package are the
research evaluation surface that mirrors the current notebook implementation.
"""

from .queries import query_common, query_conjunctive, query_phrase, query_selective, run_query
from .registry import ACTIVE_TEXT_DATASET_SLUGS, MODE_NAMES, QUERY_IDS

__all__ = [
    "ACTIVE_TEXT_DATASET_SLUGS",
    "MODE_NAMES",
    "QUERY_IDS",
    "query_common",
    "query_conjunctive",
    "query_phrase",
    "query_selective",
    "run_query",
]
