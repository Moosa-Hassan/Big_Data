"""Python search backends that mirror the current LogLite notebook logic.

What this module owns:
    - Plaintext search over decompressed artifacts.
    - Bit-level search over `.lite.b` files using full decompression.
    - The current length-filtering `minor_optimization` search path.

What this module does not own:
    - Artifact generation.
    - Query registration.
    - Mode dispatch.
    - Metrics and profiling.

How this relates to the evaluation pipeline:
    This module is the codec-facing execution surface used by part 2. It lifts
    the notebook's prototype functions into reusable, documented Python code.

Relation to the paper / codec:
    The full-decompression implementation mirrors the on-disk bitstream layout
    defined by:
    - `LogLite-B/src/tools/xorc-cli.cc`
    - `LogLite-B/src/compress/stream_compress.cc`
    - `LogLite-B/src/common/file.cc`
    - `LogLite-B/src/common/rle.cc`

Source of truth:
    The C++ codec remains the source of truth for compression semantics. This
    Python module is a research evaluation mirror, not an independent codec.
"""

from __future__ import annotations

import shutil
import struct
import subprocess
from collections import deque
from pathlib import Path

from .specs import ModeRunResult
from .static_qgram_index import (
    keyword_search_loglite_static_qgram_index,
    keyword_search_loglite_static_qgram_index_mmap,
)
from .window_loader import ParsedWindow


def _normalize_query_keywords(query_keywords: str | tuple[str, ...] | list[str]) -> list[str]:
    """Normalize query input into a list of required keyword strings."""

    if isinstance(query_keywords, str):
        return [query_keywords]
    return list(query_keywords)


def _decode_bytes_to_text(raw_bytes: bytes) -> str:
    """Decode bytes into text using the notebook's permissive semantics.

    Notes:
        The notebook always decodes with `errors='ignore'`. The evaluation code
        preserves that behavior so result comparisons remain aligned with the
        prototype that established the current function contract.
    """

    return raw_bytes.decode("utf-8", "ignore")


def keyword_search_plaintext_file(
    text_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> list[str]:
    """Run baseline keyword search over a decompressed text artifact.

    Purpose:
        Provide the correctness baseline for part 2. The baseline intentionally
        scans the decompressed output produced from the `.lite.b` artifact rather
        than the original raw input so all three modes are anchored to the same
        codec output.

    Arguments:
        text_path: Path to the fully decompressed text artifact.
        query_keywords: One keyword or multiple conjunctive keywords. A line is
            a match only if it contains every keyword.

    Returns:
        A list of matched log lines in file order.

    Raises:
        FileNotFoundError: If the decompressed artifact is missing.

    Side Effects:
        Reads the decompressed artifact from disk.
    """

    keywords = _normalize_query_keywords(query_keywords)
    matches: list[str] = []

    with text_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if all(keyword in line for keyword in keywords):
                matches.append(line)

    return matches


def keyword_search_grep_plaintext_file(
    text_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Run grep over plaintext, then exact-postfilter conjunctive payloads."""

    return _keyword_search_external_plaintext_file(
        text_path=text_path,
        query_keywords=query_keywords,
        executable="grep",
        command_builder=lambda executable, term, path: [executable, "-F", "--", term, str(path)],
        planner_strategy="grep_fixed_first_term",
    )


def keyword_search_ripgrep_plaintext_file(
    text_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Run ripgrep over plaintext, then exact-postfilter conjunctive payloads."""

    return _keyword_search_external_plaintext_file(
        text_path=text_path,
        query_keywords=query_keywords,
        executable="rg",
        command_builder=lambda executable, term, path: [
            executable,
            "--fixed-strings",
            "--no-heading",
            "--color",
            "never",
            "--",
            term,
            str(path),
        ],
        planner_strategy="ripgrep_fixed_first_term",
    )


def _keyword_search_external_plaintext_file(
    text_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
    executable: str,
    command_builder,
    planner_strategy: str,
) -> ModeRunResult:
    """Run one external fixed-string baseline and preserve exact query semantics."""

    if not text_path.exists():
        raise FileNotFoundError(f"Plaintext artifact not found: {text_path}")
    executable_path = shutil.which(executable)
    if executable_path is None:
        raise RuntimeError(f"The {executable!r} baseline requires {executable} on PATH.")

    keywords = _normalize_query_keywords(query_keywords)
    first_non_empty_term = next((keyword for keyword in keywords if keyword), None)
    if first_non_empty_term is None:
        matches = keyword_search_plaintext_file(text_path, keywords)
        return ModeRunResult(
            matches=matches,
            verified_records=len(matches),
            planner_strategy=f"{planner_strategy}_empty_query",
        )

    command = command_builder(executable_path, first_non_empty_term, text_path)
    completed_process = subprocess.run(command, capture_output=True, text=True)
    if completed_process.returncode not in (0, 1):
        raise RuntimeError(
            f"{executable} baseline failed.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed_process.stdout}\n"
            f"stderr:\n{completed_process.stderr}"
        )

    if completed_process.returncode == 0:
        candidate_lines = completed_process.stdout.split("\n")
        if candidate_lines and candidate_lines[-1] == "":
            candidate_lines.pop()
    else:
        candidate_lines = []
    matches = [line for line in candidate_lines if all(keyword in line for keyword in keywords)]
    return ModeRunResult(
        matches=matches,
        decoded_records=len(candidate_lines),
        decoded_bytes=sum(len(line.encode("utf-8")) for line in candidate_lines),
        skipped_records=None,
        skipped_bytes=None,
        fallback_count=0,
        total_records=None,
        planner_strategy=planner_strategy if len(keywords) == 1 else f"{planner_strategy}_hybrid_postfilter",
        verified_records=len(candidate_lines),
        verified_bytes=sum(len(line.encode("utf-8")) for line in candidate_lines),
    )


def keyword_search_loglite_binary_full_decompression(
    bin_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> list[str]:
    """Search a `.lite.b` file using faithful sequential decompression.

    Purpose:
        Walk the compressed bitstream in the same logical order as the C++
        decompressor, reconstruct each line, maintain the mirrored L-window
        state, and store only lines that satisfy the query.

    Arguments:
        bin_path: Path to the LogLite compressed binary.
        query_keywords: One keyword or multiple conjunctive keywords.

    Returns:
        A list of matched lines in decode order.

    Failure Modes:
        Returns an empty list if the file is missing, too small to be valid, or
        malformed enough that sane parsing cannot continue.

    Notes:
        This is the Python realization of the part-1 function contract assumed
        by part 2. It is expected to match the decompressed-text baseline.
    """

    keywords = _normalize_query_keywords(query_keywords)

    # These constants mirror `LogLite-B/src/common/constants.h` and the decode
    # logic in `xorc-cli.cc`. They are duplicated here intentionally because the
    # Python backend is a codec-semantic mirror, not a generic text searcher.
    each_window_size_count = 3
    stream_encoder_count = 13
    original_length_count = 15
    max_len = 10000
    rle_count = 8
    rle_skim = 8
    each_window_size = 1 << each_window_size_count

    matches: list[str] = []
    if not bin_path.exists():
        return matches

    data = bin_path.read_bytes()
    if len(data) < 16:
        return matches

    # `write_bitset_to_file` stores a packed array of unsigned-long blocks,
    # followed by one trailing `size_t` telling the decoder how many bits in the
    # final block are meaningful. Reconstructing this exact layout is necessary
    # before we can replay the C++ decoder line by line.
    ulong_size = 8
    size_t_size = 8
    file_size = len(data)
    last_block_bits = struct.unpack("<Q", data[file_size - size_t_size : file_size])[0]
    blocks_bytes = data[: file_size - size_t_size]
    if len(blocks_bytes) % ulong_size != 0:
        return matches

    num_blocks = len(blocks_bytes) // ulong_size
    blocks = struct.unpack("<" + "Q" * num_blocks, blocks_bytes)
    bits_per_block = ulong_size * 8
    total_bits = (num_blocks - 1) * bits_per_block + (last_block_bits or bits_per_block)

    position = 0

    def read_bit() -> int | None:
        nonlocal position
        if position >= total_bits:
            return None
        block_index = position // bits_per_block
        block_offset = position % bits_per_block
        bit = (blocks[block_index] >> block_offset) & 1
        position += 1
        return bit

    def read_int(bit_count: int) -> int:
        value = 0
        for bit_index in range(bit_count):
            bit = read_bit()
            if bit is None:
                return value
            if bit:
                value |= 1 << bit_index
        return value

    def read_bytes_from_bits(byte_count: int) -> bytes:
        buffer = bytearray(byte_count)
        for byte_index in range(byte_count):
            buffer[byte_index] = read_int(8) & 0xFF
        return bytes(buffer)

    # This mirrored window is the core correctness requirement. The C++ decoder
    # updates it on every decoded line, and later compressed entries refer back
    # to it through `window_id`. If this state drifts, exactness is lost.
    window: dict[int, deque[str]] = {}

    while True:
        flag = read_bit()
        if flag is None:
            break

        if flag == 0:
            length = read_int(original_length_count)
            if length <= 0:
                continue

            line = _decode_bytes_to_text(read_bytes_from_bits(length))

            # The raw-line path resets the bucket for this length to contain only
            # the current line, matching `Stream_Compress::stream_decompress`.
            if length < max_len:
                window[length] = deque([line], maxlen=each_window_size)

            if all(keyword in line for keyword in keywords):
                matches.append(line)
            continue

        window_id = read_int(each_window_size_count)
        encoded_payload_bit_length = read_int(stream_encoder_count)
        if encoded_payload_bit_length <= 0:
            continue

        bits_consumed = 0
        xor_bytes: list[int] = []
        while bits_consumed < encoded_payload_bit_length:
            tag_bit = read_bit()
            if tag_bit is None:
                break
            bits_consumed += 1

            if tag_bit == 1:
                literal_byte = 0
                for bit_index in range(rle_skim):
                    bit = read_bit()
                    if bit is None:
                        break
                    bits_consumed += 1
                    if bit:
                        literal_byte |= 1 << bit_index
                xor_bytes.append(literal_byte & 0xFF)
            else:
                zero_count = 0
                for bit_index in range(rle_count):
                    bit = read_bit()
                    if bit is None:
                        break
                    bits_consumed += 1
                    if bit:
                        zero_count |= 1 << bit_index
                if zero_count > 0:
                    xor_bytes.extend([0] * zero_count)

        if not xor_bytes:
            continue

        xor_result = bytearray(xor_bytes)
        line_length = len(xor_result)
        current_bucket = window.get(line_length)
        if not current_bucket or window_id >= len(current_bucket):
            # A well-formed stream should not land here. The graceful continue
            # mirrors notebook behavior and avoids crashing measurement runs.
            continue

        pattern_bytes = current_bucket[window_id].encode("utf-8", "ignore")
        if len(pattern_bytes) < line_length:
            pattern_bytes = pattern_bytes.ljust(line_length, b"\0")

        # This mirrors `simdReplaceNullCharacters` from the C++ path. In the XOR
        # payload, zero bytes mean "reuse the template byte at this position".
        for index in range(line_length):
            if xor_result[index] == 0:
                xor_result[index] = pattern_bytes[index]

        line = _decode_bytes_to_text(bytes(xor_result))
        current_bucket.append(line)
        if all(keyword in line for keyword in keywords):
            matches.append(line)

    return matches


def keyword_search_loglite_binary_minor_optimization(
    bin_path: Path,
    parsed_l_window: ParsedWindow,
    query_keywords: str | tuple[str, ...] | list[str],
) -> list[str]:
    """Search a `.lite.b` file using the current minor optimization.

    Purpose:
        Evaluate the notebook's current length-filtering idea exactly as it is,
        including its known correctness weakness. The role of this mode is to be
        measured honestly, not normalized into looking exact.

    Arguments:
        bin_path: Path to the LogLite compressed binary.
        parsed_l_window: Parsed final L-window dump for the dataset.
        query_keywords: One keyword or multiple conjunctive keywords.

    Returns:
        A list of matched lines reconstructed by the optimization.

    Failure Modes:
        Returns an empty list if the input file is missing or malformed.

    Notes:
        This mode filters by lengths observed in the final L-window templates. It
        can skip entries without updating decode state, which means later records
        that depend on skipped templates may be reconstructed incorrectly.
    """

    each_window_size_count = 3
    stream_encoder_count = 13
    original_length_count = 15
    max_len = 10000
    rle_count = 8
    each_window_size = 1 << each_window_size_count

    keywords = _normalize_query_keywords(query_keywords)
    keyword_bytes = [keyword.encode("utf-8") for keyword in keywords]
    matches: list[str] = []

    # The optimization's first filter is deliberately simple: if no final-window
    # template of a given length contains all query keywords, the mode assumes no
    # line of that length is worth reconstructing. This is fast, but not exact.
    allowed_lengths: set[int] = set()
    for length, templates in parsed_l_window.items():
        for template in templates:
            if all(keyword in template for keyword in keywords):
                allowed_lengths.add(length)
                break

    if not bin_path.exists():
        return matches

    data = bin_path.read_bytes()
    if len(data) < 16:
        return matches

    ulong_size = 8
    file_size = len(data)
    last_block_bits = struct.unpack("<Q", data[file_size - 8 :])[0]
    blocks_bytes = data[: file_size - 8]
    num_blocks = len(blocks_bytes) // ulong_size
    blocks = struct.unpack("<" + "Q" * num_blocks, blocks_bytes)
    bits_per_block = 64
    total_bits = (num_blocks - 1) * bits_per_block + (last_block_bits or bits_per_block)

    position = 0

    def read_bit() -> int | None:
        nonlocal position
        if position >= total_bits:
            return None
        bit = (blocks[position // 64] >> (position % 64)) & 1
        position += 1
        return bit

    def read_int(bit_count: int) -> int:
        value = 0
        for bit_index in range(bit_count):
            bit = read_bit()
            if bit is not None and bit:
                value |= 1 << bit_index
        return value

    def skip_bits(bit_count: int) -> None:
        nonlocal position
        position += bit_count

    window: dict[int, deque[bytes]] = {}

    while True:
        flag = read_bit()
        if flag is None:
            break

        if flag == 0:
            line_length = read_int(original_length_count)
            if line_length in allowed_lengths:
                line_bytes = bytearray(line_length)
                for byte_index in range(line_length):
                    line_bytes[byte_index] = read_int(8)
                final_line_bytes = bytes(line_bytes)

                if line_length < max_len:
                    window[line_length] = deque([final_line_bytes], maxlen=each_window_size)

                if all(keyword in final_line_bytes for keyword in keyword_bytes):
                    matches.append(_decode_bytes_to_text(final_line_bytes))
            else:
                skip_bits(line_length * 8)
            continue

        window_id = read_int(each_window_size_count)
        encoded_payload_bit_length = read_int(stream_encoder_count)

        xor_delta = bytearray()
        bits_consumed = 0
        while bits_consumed < encoded_payload_bit_length:
            tag_bit = read_bit()
            if tag_bit is None:
                break
            bits_consumed += 1
            if tag_bit == 1:
                xor_delta.append(read_int(8))
                bits_consumed += 8
            else:
                zero_count = read_int(rle_count)
                bits_consumed += rle_count
                xor_delta.extend(b"\x00" * zero_count)

        reconstructed_length = len(xor_delta)
        if reconstructed_length in allowed_lengths:
            current_bucket = window.get(reconstructed_length)
            if not current_bucket or window_id >= len(current_bucket):
                continue

            template_bytes = current_bucket[window_id]
            reconstructed = bytearray(xor_delta)
            for index in range(reconstructed_length):
                if reconstructed[index] == 0:
                    reconstructed[index] = template_bytes[index]
            final_bytes = bytes(reconstructed)
            current_bucket.append(final_bytes)

            if all(keyword in final_bytes for keyword in keyword_bytes):
                matches.append(_decode_bytes_to_text(final_bytes))
        else:
            # This is the known semantic compromise. The mode skips work, but it
            # also fails to update the length-specific decode window, which can
            # corrupt later reconstructions for the same length bucket.
            pass

    return matches


def keyword_search_loglite_static_bloom(
    bin_path: Path,
    parsed_l_window: ParsedWindow,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Search a static-window LogLite bitstream using bitmap pre-filtering.

    Purpose:
        Validate the static L-window + Bloom-filter direction. Each record has a
        64-bit alphanumeric-token bitmap. A missing query bit lets us skip that
        record; a candidate record is still reconstructed and exact-checked.
    """

    each_window_size_count = 5
    stream_encoder_count = 13
    original_length_count = 15
    word_bitmap_bits = 64
    rle_count = 8

    keywords = _normalize_query_keywords(query_keywords)
    keyword_bytes = [keyword.encode("utf-8") for keyword in keywords]
    matches: list[str] = []
    decoded_records = 0
    decoded_bytes = 0
    skipped_records = 0
    skipped_bytes = 0
    bloom_rejected_records = 0
    bloom_candidate_records = 0
    total_records = 0

    def build_result() -> ModeRunResult:
        return ModeRunResult(
            matches=matches,
            decoded_records=decoded_records,
            decoded_bytes=decoded_bytes,
            skipped_records=skipped_records,
            skipped_bytes=skipped_bytes,
            fallback_count=0,
            bloom_rejected_records=bloom_rejected_records,
            bloom_candidate_records=bloom_candidate_records,
            total_records=total_records,
        )

    if not bin_path.exists():
        return build_result()

    data = bin_path.read_bytes()
    if len(data) < 16:
        return build_result()

    file_size = len(data)
    last_block_bits = struct.unpack("<Q", data[file_size - 8 :])[0]
    blocks_bytes = data[: file_size - 8]
    if len(blocks_bytes) % 8 != 0:
        return build_result()

    num_blocks = len(blocks_bytes) // 8
    blocks = struct.unpack("<" + "Q" * num_blocks, blocks_bytes)
    total_bits = (num_blocks - 1) * 64 + (last_block_bits or 64)
    position = 0

    def compute_word_bitmap(text: str) -> int:
        """Mirror `computeWordBitmap` in `src_static/compress/stream_compress.cc`."""

        bitmap = 0
        token: list[str] = []

        def flush_token() -> None:
            nonlocal bitmap, token
            if not token:
                return
            token_hash = 0
            for character in token:
                token_hash = ((token_hash << 5) + token_hash) ^ ord(character)
            bitmap |= 1 << (token_hash & 63)
            token = []

        for character in text:
            if character.isalnum():
                token.append(character)
            else:
                flush_token()
        flush_token()
        return bitmap

    query_bitmap = 0
    for keyword in keywords:
        query_bitmap |= compute_word_bitmap(keyword)

    def read_bit() -> int | None:
        nonlocal position
        if position >= total_bits:
            return None
        bit = (blocks[position // 64] >> (position % 64)) & 1
        position += 1
        return bit

    def read_int(bit_count: int) -> int:
        value = 0
        for bit_index in range(bit_count):
            bit = read_bit()
            if bit is not None and bit:
                value |= 1 << bit_index
        return value

    def skip_bits(bit_count: int) -> None:
        nonlocal position
        position = min(position + bit_count, total_bits)

    while True:
        flag = read_bit()
        if flag is None:
            break
        total_records += 1

        # Static streams store a 64-bit token bitmap immediately after the flag.
        # This is the whole compressed-domain filter: reject quickly if any
        # required query token bit is absent, otherwise reconstruct and verify.
        record_bitmap = read_int(word_bitmap_bits)
        if (record_bitmap & query_bitmap) != query_bitmap:
            skipped_records += 1
            bloom_rejected_records += 1
            if flag == 0:
                line_length = read_int(original_length_count)
                skipped_bytes += line_length
                skip_bits(line_length * 8)
            else:
                _window_id = read_int(each_window_size_count)
                encoded_payload_bit_length = read_int(stream_encoder_count)
                skip_bits(encoded_payload_bit_length)
            continue

        bloom_candidate_records += 1
        decoded_records += 1

        if flag == 0:
            line_length = read_int(original_length_count)
            line_bytes = bytearray(line_length)
            for byte_index in range(line_length):
                line_bytes[byte_index] = read_int(8)
            final_bytes = bytes(line_bytes)
            decoded_bytes += len(final_bytes)
            if all(keyword in final_bytes for keyword in keyword_bytes):
                matches.append(_decode_bytes_to_text(final_bytes))
            continue

        window_id = read_int(each_window_size_count)
        encoded_payload_bit_length = read_int(stream_encoder_count)
        xor_delta = bytearray()
        bits_consumed = 0
        while bits_consumed < encoded_payload_bit_length:
            tag_bit = read_bit()
            if tag_bit is None:
                break
            bits_consumed += 1
            if tag_bit == 1:
                xor_delta.append(read_int(8))
                bits_consumed += 8
            else:
                zero_count = read_int(rle_count)
                bits_consumed += rle_count
                xor_delta.extend(b"\x00" * zero_count)

        line_length = len(xor_delta)
        templates = parsed_l_window.get(line_length)
        if not templates or window_id >= len(templates):
            continue

        template_bytes = templates[window_id].encode("utf-8", "ignore")
        if len(template_bytes) < line_length:
            template_bytes = template_bytes.ljust(line_length, b"\0")

        reconstructed = bytearray(xor_delta)
        for index in range(line_length):
            if reconstructed[index] == 0:
                reconstructed[index] = template_bytes[index]

        final_bytes = bytes(reconstructed)
        decoded_bytes += len(final_bytes)
        if all(keyword in final_bytes for keyword in keyword_bytes):
            matches.append(_decode_bytes_to_text(final_bytes))

    return build_result()
