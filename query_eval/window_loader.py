"""Utilities for parsing `xorc-cli` L-window dumps.

What this module owns:
    - Loading `.window.txt` files into a stable in-memory representation.

What this module does not own:
    - Artifact generation.
    - Query dispatch.
    - Metrics.

How this relates to the evaluation pipeline:
    The `minor_optimization` execution path depends on the final L-window dump
    emitted by `LogLite-B/src/tools/xorc-cli.cc`. That dependency is isolated in
    this module so the rest of the pipeline can work with a normal Python
    mapping rather than re-parsing text dumps on demand.

Source of truth:
    The file format parsed here mirrors the project-local `--window-output-path`
    extension in `LogLite-B/src/tools/xorc-cli.cc`.
"""

from __future__ import annotations

from pathlib import Path

ParsedWindow = dict[int, list[str]]


def load_l_window_from_txt(window_path: Path) -> ParsedWindow:
    """Parse a text L-window dump into `{line_length: [templates...]}`.

    Purpose:
        Convert the codec's analysis-oriented text dump into the exact data
        structure expected by the minor-optimization backend.

    Arguments:
        window_path: Path to the `.window.txt` artifact emitted by `xorc-cli`.

    Returns:
        A mapping from line length to the final deque contents dumped by the
        compressor after processing the raw dataset.

    Raises:
        FileNotFoundError: If the window file does not exist.
        ValueError: If a malformed `len=` header is encountered.

    Side Effects:
        Reads the window file from disk.

    Notes:
        The parser intentionally preserves insertion order within each bucket.
        That order matters because `window_id` values in the compressed bitstream
        refer to positions inside the per-length deque.
    """

    if not window_path.exists():
        raise FileNotFoundError(f"Window artifact not found: {window_path}")

    parsed_window: ParsedWindow = {}
    current_length: int | None = None
    current_templates: list[str] = []

    with window_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue

            if line.startswith("len="):
                if current_length is not None:
                    parsed_window[current_length] = current_templates
                try:
                    current_length = int(line.split("=", 1)[1])
                except ValueError as error:
                    raise ValueError(f"Malformed window header in {window_path}: {line}") from error
                current_templates = []
                continue

            if line == "---":
                if current_length is not None:
                    parsed_window[current_length] = current_templates
                    current_length = None
                    current_templates = []
                continue

            if current_length is not None:
                current_templates.append(line)

    if current_length is not None:
        parsed_window[current_length] = current_templates

    return parsed_window
