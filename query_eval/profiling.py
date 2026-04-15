"""Profiling helpers for one query execution.

What this module owns:
    - Wall-clock timing measurement.
    - CPU timing measurement.
    - Peak RSS normalization across Unix variants.

What this module does not own:
    - Query execution semantics.
    - Correctness metrics.
    - Aggregate reporting.

How this relates to the evaluation pipeline:
    Child processes use this module to produce per-run performance measurements
    without mixing profiling code into execution backends.

Source of truth:
    `resource.getrusage` is used for memory and `time` is used for timing.
"""

from __future__ import annotations

import platform
import resource
import time
from collections.abc import Callable
from typing import TypeVar

from .specs import MemoryMeasurement, TimingMeasurement

ReturnValue = TypeVar("ReturnValue")


def _normalize_peak_rss_to_mb(raw_peak_rss: int) -> float:
    """Normalize `ru_maxrss` into MiB across supported Unix platforms.

    Notes:
        Linux reports `ru_maxrss` in kilobytes, while Darwin reports bytes.
        This normalization is mandatory so multi-machine experiment ledgers use
        the same unit.
    """

    if platform.system() == "Darwin":
        return raw_peak_rss / (1024 * 1024)
    return raw_peak_rss / 1024


def measure_callable(
    callable_under_test: Callable[[], ReturnValue],
) -> tuple[ReturnValue, TimingMeasurement, MemoryMeasurement]:
    """Measure one callable and return its output plus timing/memory metrics.

    Purpose:
        Isolate candidate execution inside a minimal profiling wrapper.

    Arguments:
        callable_under_test: Zero-argument callable that performs the candidate
            query execution.

    Returns:
        A tuple `(result, timing, memory)`.

    Side Effects:
        Executes the provided callable.
    """

    start_wall_time = time.perf_counter()
    start_cpu_time = time.process_time()
    result = callable_under_test()
    end_cpu_time = time.process_time()
    end_wall_time = time.perf_counter()

    usage = resource.getrusage(resource.RUSAGE_SELF)
    peak_rss_mb = _normalize_peak_rss_to_mb(usage.ru_maxrss)

    timing = TimingMeasurement(
        wall_time_ms=(end_wall_time - start_wall_time) * 1000,
        cpu_time_ms=(end_cpu_time - start_cpu_time) * 1000,
    )
    memory = MemoryMeasurement(peak_rss_mb=peak_rss_mb)
    return result, timing, memory
