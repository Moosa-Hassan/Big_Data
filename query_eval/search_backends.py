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

import struct
from collections import deque
from pathlib import Path

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
