"""Exact q-gram index over static-window LogLite artifacts.

This module implements the `static_qgram_index` execution mode. The index is a
sidecar JSON artifact built after static LogLite compression. Query execution
uses q-gram postings to identify candidate record IDs, reconstructs only those
records by bit offset, and then applies the same exact substring predicate as
the decompressed-text baseline.

Correctness sketch:
    - Q-gram necessity: if pattern p occurs in text s, every q-gram of p occurs
      in s.
    - Filter soundness: intersecting q-gram postings can only remove records
      missing a necessary gram, so no true substring match is removed.
    - Exactness: every candidate is reconstructed and checked with the baseline
      all-terms substring predicate, eliminating all q-gram false positives.
    - Query work: after index load, the search performs postings intersections
      plus candidate reconstruction, avoiding the record-by-record bitmap scan
      used by `static_bloom`.
"""

from __future__ import annotations

import json
import io
import mmap
import platform
import shutil
import struct
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .registry import get_project_root, get_runtime_root
from .specs import ArtifactSpec, ModeRunResult
from .window_loader import ParsedWindow, load_l_window_from_txt

STATIC_QGRAM_INDEX_VERSION = "static_qgram_index.v1"
STATIC_QGRAM_MMAP_INDEX_VERSION = 2
STATIC_QGRAM_MMAP_MAGIC = b"QIDX2MM\0"
STATIC_QGRAM_COMPACT_INDEX_VERSION = 5
STATIC_QGRAM_COMPACT_MAGIC = b"QIDX3MM\0"

_MMAP_HEADER_STRUCT = struct.Struct("<8sII19Q")
_COMPACT_HEADER_STRUCT = struct.Struct("<8sII17Q")
_POSTING_TABLE_ENTRY_STRUCT = struct.Struct("<IH")
_Q3_DICTIONARY_ENTRY_STRUCT = struct.Struct("<IIH")
_COMPACT_DICTIONARY_ENTRY_STRUCT = struct.Struct("<IIII")
_RECORD_DIRECTORY_STRUCT = struct.Struct("<QQIIIHBBQ")
_LINE_DIRECTORY_STRUCT = struct.Struct("<QI")

_HEADER_SOURCE_SIZE = 0
_HEADER_SOURCE_MTIME_NS = 1
_HEADER_WINDOW_SIZE = 2
_HEADER_WINDOW_MTIME_NS = 3
_HEADER_RECORD_COUNT = 4
_HEADER_Q1_OFFSET = 5
_HEADER_Q1_COUNT = 6
_HEADER_Q2_OFFSET = 7
_HEADER_Q2_COUNT = 8
_HEADER_Q3_OFFSET = 9
_HEADER_Q3_COUNT = 10
_HEADER_POSTINGS_OFFSET = 11
_HEADER_POSTINGS_SIZE = 12
_HEADER_RECORD_DIRECTORY_OFFSET = 13
_HEADER_RECORD_DIRECTORY_SIZE = 14
_HEADER_LINE_DIRECTORY_OFFSET = 15
_HEADER_LINE_DIRECTORY_SIZE = 16
_HEADER_LINE_SLAB_OFFSET = 17
_HEADER_LINE_SLAB_SIZE = 18

_COMPACT_HEADER_SOURCE_SIZE = 0
_COMPACT_HEADER_SOURCE_MTIME_NS = 1
_COMPACT_HEADER_WINDOW_SIZE = 2
_COMPACT_HEADER_WINDOW_MTIME_NS = 3
_COMPACT_HEADER_RECORD_COUNT = 4
_COMPACT_HEADER_Q1_OFFSET = 5
_COMPACT_HEADER_Q1_COUNT = 6
_COMPACT_HEADER_Q2_OFFSET = 7
_COMPACT_HEADER_Q2_COUNT = 8
_COMPACT_HEADER_Q3_OFFSET = 9
_COMPACT_HEADER_Q3_COUNT = 10
_COMPACT_HEADER_POSTINGS_OFFSET = 11
_COMPACT_HEADER_POSTINGS_SIZE = 12
_COMPACT_HEADER_LINE_DIRECTORY_OFFSET = 13
_COMPACT_HEADER_LINE_DIRECTORY_SIZE = 14
_COMPACT_HEADER_LINE_SLAB_OFFSET = 15
_COMPACT_HEADER_LINE_SLAB_SIZE = 16

EACH_WINDOW_SIZE_COUNT = 5
STREAM_ENCODER_COUNT = 13
ORIGINAL_LENGTH_COUNT = 15
WORD_BITMAP_BITS = 64
RLE_COUNT = 8


@dataclass(frozen=True)
class RecordDirectoryEntry:
    """Random-access metadata for one record in a static LogLite bitstream."""

    record_id: int
    bit_offset: int
    flag: int
    payload_bit_offset: int
    payload_bit_length: int
    line_length: int
    window_id: int | None
    raw_length: int | None
    decoded_length: int
    token_bitmap: int


@dataclass(frozen=True)
class StaticQGramIndex:
    """Loaded q-gram index and record directory for a static artifact."""

    index_path: Path
    compressed_binary_path: Path
    static_window_path: Path
    record_directory: list[RecordDirectoryEntry]
    postings: dict[str, dict[str, list[int]]]
    record_count: int
    source_size: int
    source_mtime_ns: int
    window_size: int
    window_mtime_ns: int


@dataclass(frozen=True)
class StaticQGramMMapHeader:
    """Parsed fixed header for a binary mmap q-gram sidecar."""

    version: int
    header_size: int
    source_size: int
    source_mtime_ns: int
    window_size: int
    window_mtime_ns: int
    record_count: int
    q1_offset: int
    q1_count: int
    q2_offset: int
    q2_count: int
    q3_offset: int
    q3_count: int
    postings_offset: int
    postings_size: int
    record_directory_offset: int
    record_directory_size: int
    line_directory_offset: int
    line_directory_size: int
    line_slab_offset: int
    line_slab_size: int


@dataclass(frozen=True)
class StaticQGramCompactHeader:
    """Parsed fixed header for a compact binary q-gram sidecar."""

    version: int
    header_size: int
    source_size: int
    source_mtime_ns: int
    window_size: int
    window_mtime_ns: int
    record_count: int
    q1_offset: int
    q1_count: int
    q2_offset: int
    q2_count: int
    q3_offset: int
    q3_count: int
    postings_offset: int
    postings_size: int
    line_directory_offset: int
    line_directory_size: int
    line_slab_offset: int
    line_slab_size: int


class StaticQGramMMapView:
    """Lightweight mmap view over a binary q-gram sidecar."""

    def __init__(self, mmap_buffer: mmap.mmap) -> None:
        self._mmap = mmap_buffer
        self.header = _parse_mmap_header(mmap_buffer)

    @property
    def record_count(self) -> int:
        """Return the number of records represented in the sidecar."""

        return self.header.record_count

    def get_postings(self, gram_size: int, gram_value: int) -> list[int]:
        """Return sorted record IDs for one encoded q-gram."""

        if gram_size == 1:
            if gram_value < 0 or gram_value >= self.header.q1_count:
                return []
            entry_offset = self.header.q1_offset + gram_value * _POSTING_TABLE_ENTRY_STRUCT.size
            return self._read_postings_entry(entry_offset)
        if gram_size == 2:
            if gram_value < 0 or gram_value >= self.header.q2_count:
                return []
            entry_offset = self.header.q2_offset + gram_value * _POSTING_TABLE_ENTRY_STRUCT.size
            return self._read_postings_entry(entry_offset)
        if gram_size != 3:
            return []

        low = 0
        high = self.header.q3_count
        while low < high:
            middle = (low + high) // 2
            entry_offset = self.header.q3_offset + middle * _Q3_DICTIONARY_ENTRY_STRUCT.size
            current_gram, _postings_offset, _count = _Q3_DICTIONARY_ENTRY_STRUCT.unpack_from(
                self._mmap,
                entry_offset,
            )
            if current_gram == gram_value:
                return self._read_q3_postings_entry(entry_offset)
            if current_gram < gram_value:
                low = middle + 1
            else:
                high = middle
        return []

    def line_bytes(self, record_id: int) -> bytes:
        """Return normalized decoded bytes for one record."""

        offset, length = self.line_location(record_id)
        start = self.header.line_slab_offset + offset
        return bytes(self._mmap[start : start + length])

    def line_location(self, record_id: int) -> tuple[int, int]:
        """Return the line-slab offset and byte length for one record."""

        if record_id < 0 or record_id >= self.header.record_count:
            raise IndexError(f"Record id out of range: {record_id}")
        entry_offset = self.header.line_directory_offset + record_id * _LINE_DIRECTORY_STRUCT.size
        return _LINE_DIRECTORY_STRUCT.unpack_from(self._mmap, entry_offset)

    def line_contains_all(self, record_id: int, keyword_bytes: list[bytes]) -> bool:
        """Return whether a record line contains all query byte terms."""

        offset, length = self.line_location(record_id)
        start = self.header.line_slab_offset + offset
        line_view = self._mmap[start : start + length]
        return all(keyword in line_view for keyword in keyword_bytes)

    def _read_postings_entry(self, entry_offset: int) -> list[int]:
        postings_relative_offset, count = _POSTING_TABLE_ENTRY_STRUCT.unpack_from(self._mmap, entry_offset)
        if count == 0:
            return []
        postings_start = self.header.postings_offset + postings_relative_offset
        postings_end = postings_start + count * 2
        postings_bytes = self._mmap[postings_start:postings_end]
        return list(struct.unpack("<" + "H" * count, postings_bytes))

    def _read_q3_postings_entry(self, entry_offset: int) -> list[int]:
        _gram, postings_relative_offset, count = _Q3_DICTIONARY_ENTRY_STRUCT.unpack_from(self._mmap, entry_offset)
        if count == 0:
            return []
        postings_start = self.header.postings_offset + postings_relative_offset
        postings_end = postings_start + count * 2
        postings_bytes = self._mmap[postings_start:postings_end]
        return list(struct.unpack("<" + "H" * count, postings_bytes))


class StaticQGramCompactMMapView:
    """Lightweight mmap view over a compact qidx3 sidecar."""

    def __init__(self, mmap_buffer: mmap.mmap) -> None:
        self._mmap = mmap_buffer
        self.header = _parse_compact_header(mmap_buffer)

    @property
    def record_count(self) -> int:
        """Return the number of records represented in the sidecar."""

        return self.header.record_count

    def get_postings_meta(self, gram_size: int, gram_value: int) -> tuple[int, int, int] | None:
        """Return postings blob offset, byte length, and count for one q-gram."""

        section = self._section_for_gram_size(gram_size)
        if section is None:
            return None
        section_offset, section_count = section
        low = 0
        high = section_count
        while low < high:
            middle = (low + high) // 2
            entry_offset = section_offset + middle * _COMPACT_DICTIONARY_ENTRY_STRUCT.size
            current_gram, postings_offset, postings_byte_length, count = _COMPACT_DICTIONARY_ENTRY_STRUCT.unpack_from(
                self._mmap,
                entry_offset,
            )
            if current_gram == gram_value:
                return postings_offset, postings_byte_length, count
            if current_gram < gram_value:
                low = middle + 1
            else:
                high = middle
        return None

    def get_postings_count(self, gram_size: int, gram_value: int) -> int:
        """Return the posting-list cardinality for one q-gram without decoding it."""

        meta = self.get_postings_meta(gram_size, gram_value)
        return 0 if meta is None else meta[2]

    def get_postings(self, gram_size: int, gram_value: int) -> list[int]:
        """Return sorted record IDs for one q-gram."""

        meta = self.get_postings_meta(gram_size, gram_value)
        if meta is None:
            return []
        postings_relative_offset, postings_byte_length, count = meta
        if count == 0:
            return []
        start = self.header.postings_offset + postings_relative_offset
        return decode_delta_varint_postings(self._mmap, start, postings_byte_length, expected_count=count)

    def line_bytes(self, record_id: int) -> bytes:
        """Return normalized decoded bytes for one record."""

        offset, length = self.line_location(record_id)
        start = self.header.line_slab_offset + offset
        return bytes(self._mmap[start : start + length])

    def line_location(self, record_id: int) -> tuple[int, int]:
        """Return the line-slab offset and byte length for one record."""

        if record_id < 0 or record_id >= self.header.record_count:
            raise IndexError(f"Record id out of range: {record_id}")
        entry_offset = self.header.line_directory_offset + record_id * _LINE_DIRECTORY_STRUCT.size
        return _LINE_DIRECTORY_STRUCT.unpack_from(self._mmap, entry_offset)

    def line_contains_all(self, record_id: int, keyword_bytes: list[bytes]) -> bool:
        """Return whether one normalized line contains all query byte terms."""

        offset, length = self.line_location(record_id)
        start = self.header.line_slab_offset + offset
        line_view = self._mmap[start : start + length]
        return all(keyword in line_view for keyword in keyword_bytes)

    def _section_for_gram_size(self, gram_size: int) -> tuple[int, int] | None:
        if gram_size == 1:
            return self.header.q1_offset, self.header.q1_count
        if gram_size == 2:
            return self.header.q2_offset, self.header.q2_count
        if gram_size == 3:
            return self.header.q3_offset, self.header.q3_count
        return None


class _StaticBitReader:
    """LSB-first bit reader for LogLite dynamic-bitset files."""

    def __init__(self, blocks: tuple[int, ...], total_bits: int, position: int = 0) -> None:
        self.blocks = blocks
        self.total_bits = total_bits
        self.position = position

    def read_bit(self) -> int | None:
        """Read one bit or return None at end-of-stream."""

        if self.position >= self.total_bits:
            return None
        bit = (self.blocks[self.position // 64] >> (self.position % 64)) & 1
        self.position += 1
        return bit

    def read_int(self, bit_count: int) -> int:
        """Read a little-endian fixed-width integer."""

        value = 0
        for bit_index in range(bit_count):
            bit = self.read_bit()
            if bit is not None and bit:
                value |= 1 << bit_index
        return value

    def read_bytes_from_bits(self, byte_count: int) -> bytes:
        """Read byte_count raw bytes from LSB-first bit encoding."""

        buffer = bytearray(byte_count)
        for byte_index in range(byte_count):
            buffer[byte_index] = self.read_int(8) & 0xFF
        return bytes(buffer)

    def skip_bits(self, bit_count: int) -> None:
        """Skip bits without crossing the physical stream boundary."""

        self.position = min(self.position + bit_count, self.total_bits)


def qgrams_for_bytes(payload: bytes, gram_size: int) -> set[str]:
    """Return unique q-grams as hex strings for a byte payload."""

    if gram_size <= 0:
        raise ValueError("gram_size must be positive.")
    if len(payload) < gram_size:
        return set()
    return {payload[offset : offset + gram_size].hex() for offset in range(len(payload) - gram_size + 1)}


def qgram_values_for_bytes(payload: bytes, gram_size: int) -> set[int]:
    """Return unique q-grams as big-endian integer values."""

    if gram_size <= 0:
        raise ValueError("gram_size must be positive.")
    if len(payload) < gram_size:
        return set()
    return {
        int.from_bytes(payload[offset : offset + gram_size], "big")
        for offset in range(len(payload) - gram_size + 1)
    }


def qgrams_for_query_term(term: str) -> tuple[int, set[str]]:
    """Return the chosen q and q-grams for one query term."""

    term_bytes = term.encode("utf-8")
    if not term_bytes:
        return 0, set()
    gram_size = min(3, len(term_bytes))
    return gram_size, qgrams_for_bytes(term_bytes, gram_size)


def qgram_values_for_query_term(term: str) -> tuple[int, set[int]]:
    """Return the chosen q and integer q-grams for one query term."""

    term_bytes = term.encode("utf-8")
    if not term_bytes:
        return 0, set()
    gram_size = min(3, len(term_bytes))
    return gram_size, qgram_values_for_bytes(term_bytes, gram_size)


def encode_unsigned_varint(value: int) -> bytes:
    """Encode one non-negative integer using unsigned LEB128 varints."""

    if value < 0:
        raise ValueError("Varint values must be non-negative.")
    buffer = bytearray()
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value & 0x7F)
    return bytes(buffer)


def encode_delta_varint_postings(record_ids: list[int] | tuple[int, ...]) -> bytes:
    """Encode sorted record IDs as delta-varint postings."""

    encoded = bytearray()
    previous_record_id = 0
    for index, record_id in enumerate(record_ids):
        if record_id < 0:
            raise ValueError("Record IDs must be non-negative.")
        if index > 0 and record_id <= previous_record_id:
            raise ValueError("Record IDs must be strictly increasing.")
        delta = record_id if index == 0 else record_id - previous_record_id
        encoded.extend(encode_unsigned_varint(delta))
        previous_record_id = record_id
    return bytes(encoded)


def decode_delta_varint_postings(
    buffer: bytes | bytearray | mmap.mmap,
    offset: int,
    byte_length: int,
    expected_count: int | None = None,
) -> list[int]:
    """Decode delta-varint postings from a byte buffer or mmap."""

    postings: list[int] = []
    end_offset = offset + byte_length
    cursor = offset
    previous_record_id = 0
    while cursor < end_offset:
        shift = 0
        value = 0
        while True:
            if cursor >= end_offset:
                raise ValueError("Truncated varint posting list.")
            byte_value = buffer[cursor]
            cursor += 1
            value |= (byte_value & 0x7F) << shift
            if (byte_value & 0x80) == 0:
                break
            shift += 7
            if shift > 63:
                raise ValueError("Varint posting value is too large.")
        record_id = value if not postings else previous_record_id + value
        postings.append(record_id)
        previous_record_id = record_id
    if expected_count is not None and len(postings) != expected_count:
        raise ValueError(f"Decoded {len(postings)} postings; expected {expected_count}.")
    return postings


def intersect_sorted_postings(posting_lists: list[list[int]]) -> list[int]:
    """Intersect sorted record-ID postings lists."""

    if not posting_lists:
        return []
    ordered_lists = sorted(posting_lists, key=len)
    intersection = ordered_lists[0]
    for postings in ordered_lists[1:]:
        intersection = _intersect_two_sorted_lists(intersection, postings)
        if not intersection:
            return []
    return intersection


def ensure_static_qgram_index(artifact_spec: ArtifactSpec, force_rebuild: bool = False) -> Path:
    """Ensure the sidecar q-gram index exists and is current."""

    if (
        artifact_spec.static_compressed_binary_path is None
        or artifact_spec.static_window_path is None
        or artifact_spec.static_qgram_index_path is None
    ):
        raise RuntimeError("ArtifactSpec does not include static q-gram artifact paths.")

    if force_rebuild or _index_needs_rebuild(
        artifact_spec.static_qgram_index_path,
        artifact_spec.static_compressed_binary_path,
        artifact_spec.static_window_path,
    ):
        build_static_qgram_index(
            artifact_spec.static_compressed_binary_path,
            artifact_spec.static_window_path,
            artifact_spec.static_qgram_index_path,
        )
    return artifact_spec.static_qgram_index_path


def ensure_static_qgram_mmap_index(artifact_spec: ArtifactSpec, force_rebuild: bool = False) -> Path:
    """Ensure the binary mmap q-gram sidecar exists and is current."""

    if (
        artifact_spec.static_compressed_binary_path is None
        or artifact_spec.static_window_path is None
        or artifact_spec.static_qgram_mmap_index_path is None
    ):
        raise RuntimeError("ArtifactSpec does not include static q-gram mmap artifact paths.")

    if force_rebuild or _mmap_index_needs_rebuild(
        artifact_spec.static_qgram_mmap_index_path,
        artifact_spec.static_compressed_binary_path,
        artifact_spec.static_window_path,
    ):
        build_static_qgram_mmap_index(
            artifact_spec.static_compressed_binary_path,
            artifact_spec.static_window_path,
            artifact_spec.static_qgram_mmap_index_path,
        )
    return artifact_spec.static_qgram_mmap_index_path


def ensure_static_qgram_compact_index(artifact_spec: ArtifactSpec, force_rebuild: bool = False) -> Path:
    """Ensure the compact qidx3 sidecar exists and is current."""

    if (
        artifact_spec.static_compressed_binary_path is None
        or artifact_spec.static_window_path is None
        or artifact_spec.static_qgram_compact_index_path is None
        or artifact_spec.decompressed_text_path is None
    ):
        raise RuntimeError("ArtifactSpec does not include static compact q-gram artifact paths.")

    if force_rebuild or _compact_index_needs_rebuild(
        artifact_spec.static_qgram_compact_index_path,
        artifact_spec.static_compressed_binary_path,
        artifact_spec.static_window_path,
        artifact_spec.decompressed_text_path,
    ):
        build_static_qgram_compact_index(
            artifact_spec.static_compressed_binary_path,
            artifact_spec.static_window_path,
            artifact_spec.static_qgram_compact_index_path,
            artifact_spec.decompressed_text_path,
        )
    return artifact_spec.static_qgram_compact_index_path


def build_static_qgram_index(
    compressed_binary_path: Path,
    static_window_path: Path,
    index_path: Path,
) -> StaticQGramIndex:
    """Build and persist a sidecar q-gram index for a static LogLite artifact."""

    parsed_window = load_l_window_from_txt(static_window_path)
    blocks, total_bits = _load_static_blocks(compressed_binary_path)
    record_directory = parse_static_record_directory(blocks, total_bits)

    postings_sets: dict[str, dict[str, set[int]]] = {"1": {}, "2": {}, "3": {}}
    for entry in record_directory:
        line = decode_static_record_from_blocks(blocks, total_bits, parsed_window, entry)
        normalized_bytes = line.encode("utf-8")
        for gram_size in (1, 2, 3):
            for gram in qgrams_for_bytes(normalized_bytes, gram_size):
                postings_sets[str(gram_size)].setdefault(gram, set()).add(entry.record_id)

    postings: dict[str, dict[str, list[int]]] = {
        gram_size: {gram: sorted(record_ids) for gram, record_ids in sorted(grams.items())}
        for gram_size, grams in postings_sets.items()
    }

    source_stat = compressed_binary_path.stat()
    window_stat = static_window_path.stat()
    payload = {
        "version": STATIC_QGRAM_INDEX_VERSION,
        "compressed_binary_path": str(compressed_binary_path),
        "static_window_path": str(static_window_path),
        "source_size": source_stat.st_size,
        "source_mtime_ns": source_stat.st_mtime_ns,
        "window_size": window_stat.st_size,
        "window_mtime_ns": window_stat.st_mtime_ns,
        "record_count": len(record_directory),
        "record_directory": [asdict(entry) for entry in record_directory],
        "postings": postings,
    }

    index_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = index_path.with_suffix(index_path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    temporary_path.replace(index_path)

    return StaticQGramIndex(
        index_path=index_path,
        compressed_binary_path=compressed_binary_path,
        static_window_path=static_window_path,
        record_directory=record_directory,
        postings=postings,
        record_count=len(record_directory),
        source_size=source_stat.st_size,
        source_mtime_ns=source_stat.st_mtime_ns,
        window_size=window_stat.st_size,
        window_mtime_ns=window_stat.st_mtime_ns,
    )


def build_static_qgram_mmap_index(
    compressed_binary_path: Path,
    static_window_path: Path,
    index_path: Path,
) -> StaticQGramMMapHeader:
    """Build and persist a binary mmap q-gram index for a static artifact."""

    parsed_window = load_l_window_from_txt(static_window_path)
    blocks, total_bits = _load_static_blocks(compressed_binary_path)
    record_directory = parse_static_record_directory(blocks, total_bits)
    if len(record_directory) > 0xFFFF:
        raise ValueError("The qidx2 uint16 postings format supports at most 65535 records.")

    postings_sets: dict[int, dict[int, set[int]]] = {1: {}, 2: {}, 3: {}}
    record_directory_buffer = bytearray()
    line_directory_buffer = bytearray()
    line_slab_buffer = bytearray()

    for entry in record_directory:
        line = decode_static_record_from_blocks(blocks, total_bits, parsed_window, entry)
        line_bytes = line.encode("utf-8")
        line_offset = len(line_slab_buffer)
        line_slab_buffer.extend(line_bytes)
        line_directory_buffer.extend(_LINE_DIRECTORY_STRUCT.pack(line_offset, len(line_bytes)))
        record_directory_buffer.extend(
            _RECORD_DIRECTORY_STRUCT.pack(
                entry.payload_bit_offset,
                entry.payload_bit_length,
                entry.line_length,
                entry.raw_length or 0,
                entry.decoded_length,
                entry.window_id if entry.window_id is not None else 0xFFFF,
                entry.flag,
                0,
                entry.token_bitmap,
            )
        )
        for gram_size in (1, 2, 3):
            for gram in qgram_values_for_bytes(line_bytes, gram_size):
                postings_sets[gram_size].setdefault(gram, set()).add(entry.record_id)

    postings_buffer = bytearray()

    def append_posting(record_ids: set[int] | None) -> tuple[int, int]:
        if not record_ids:
            return 0, 0
        sorted_record_ids = sorted(record_ids)
        offset = len(postings_buffer)
        if offset > 0xFFFFFFFF:
            raise ValueError("The qidx2 postings blob exceeded the uint32 offset limit.")
        if len(sorted_record_ids) > 0xFFFF:
            raise ValueError("The qidx2 posting-list count exceeded the uint16 count limit.")
        postings_buffer.extend(struct.pack("<" + "H" * len(sorted_record_ids), *sorted_record_ids))
        return offset, len(sorted_record_ids)

    q1_buffer = bytearray()
    for gram in range(256):
        offset, count = append_posting(postings_sets[1].get(gram))
        q1_buffer.extend(_POSTING_TABLE_ENTRY_STRUCT.pack(offset, count))

    q2_buffer = bytearray()
    for gram in range(65536):
        offset, count = append_posting(postings_sets[2].get(gram))
        q2_buffer.extend(_POSTING_TABLE_ENTRY_STRUCT.pack(offset, count))

    q3_buffer = bytearray()
    for gram, record_ids in sorted(postings_sets[3].items()):
        offset, count = append_posting(record_ids)
        q3_buffer.extend(_Q3_DICTIONARY_ENTRY_STRUCT.pack(gram, offset, count))

    source_stat = compressed_binary_path.stat()
    window_stat = static_window_path.stat()

    record_directory_offset = _MMAP_HEADER_STRUCT.size
    line_directory_offset = record_directory_offset + len(record_directory_buffer)
    q1_offset = line_directory_offset + len(line_directory_buffer)
    q2_offset = q1_offset + len(q1_buffer)
    q3_offset = q2_offset + len(q2_buffer)
    postings_offset = q3_offset + len(q3_buffer)
    line_slab_offset = postings_offset + len(postings_buffer)
    header_values = [
        source_stat.st_size,
        source_stat.st_mtime_ns,
        window_stat.st_size,
        window_stat.st_mtime_ns,
        len(record_directory),
        q1_offset,
        256,
        q2_offset,
        65536,
        q3_offset,
        len(postings_sets[3]),
        postings_offset,
        len(postings_buffer),
        record_directory_offset,
        len(record_directory_buffer),
        line_directory_offset,
        len(line_directory_buffer),
        line_slab_offset,
        len(line_slab_buffer),
    ]
    header = _MMAP_HEADER_STRUCT.pack(
        STATIC_QGRAM_MMAP_MAGIC,
        STATIC_QGRAM_MMAP_INDEX_VERSION,
        _MMAP_HEADER_STRUCT.size,
        *header_values,
    )

    index_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = index_path.with_suffix(index_path.suffix + ".tmp")
    with temporary_path.open("wb") as handle:
        handle.write(header)
        handle.write(record_directory_buffer)
        handle.write(line_directory_buffer)
        handle.write(q1_buffer)
        handle.write(q2_buffer)
        handle.write(q3_buffer)
        handle.write(postings_buffer)
        handle.write(line_slab_buffer)
    temporary_path.replace(index_path)
    return load_static_qgram_mmap_index_header(index_path)


def build_static_qgram_compact_index(
    compressed_binary_path: Path,
    static_window_path: Path,
    index_path: Path,
    baseline_text_path: Path | None = None,
) -> StaticQGramCompactHeader:
    """Build and persist a compact qidx3 mmap index for a static artifact."""

    postings_sets: dict[int, dict[int, set[int]]] = {1: {}, 2: {}, 3: {}}
    line_directory_buffer = bytearray()
    line_slab_buffer = bytearray()

    logical_line_id = 0
    if baseline_text_path is None:
        parsed_window = load_l_window_from_txt(static_window_path)
        blocks, total_bits = _load_static_blocks(compressed_binary_path)
        record_directory = parse_static_record_directory(blocks, total_bits)
        logical_lines = (
            logical_line
            for entry in record_directory
            for logical_line in _baseline_logical_lines_from_record_text(
                decode_static_record_from_blocks(blocks, total_bits, parsed_window, entry)
            )
        )
    else:
        logical_lines = _baseline_logical_lines_from_text_path(baseline_text_path)

    for logical_line in logical_lines:
        line_bytes = logical_line.encode("utf-8")
        if len(line_bytes) > 0xFFFFFFFF:
            raise ValueError("qidx3 line directory supports line byte lengths up to uint32.")
        line_offset = len(line_slab_buffer)
        line_slab_buffer.extend(line_bytes)
        line_directory_buffer.extend(_LINE_DIRECTORY_STRUCT.pack(line_offset, len(line_bytes)))
        for gram_size in (1, 2, 3):
            for gram in qgram_values_for_bytes(line_bytes, gram_size):
                postings_sets[gram_size].setdefault(gram, set()).add(logical_line_id)
        logical_line_id += 1

    postings_buffer = bytearray()

    def append_posting(record_ids: set[int]) -> tuple[int, int, int]:
        sorted_record_ids = sorted(record_ids)
        encoded_postings = encode_delta_varint_postings(sorted_record_ids)
        offset = len(postings_buffer)
        if offset > 0xFFFFFFFF or len(encoded_postings) > 0xFFFFFFFF:
            raise ValueError("qidx3 postings dictionary exceeded the uint32 offset/length limit.")
        postings_buffer.extend(encoded_postings)
        return offset, len(encoded_postings), len(sorted_record_ids)

    def build_dictionary_buffer(postings_by_gram: dict[int, set[int]]) -> bytearray:
        dictionary_buffer = bytearray()
        for gram, record_ids in sorted(postings_by_gram.items()):
            postings_offset, postings_byte_length, count = append_posting(record_ids)
            dictionary_buffer.extend(
                _COMPACT_DICTIONARY_ENTRY_STRUCT.pack(gram, postings_offset, postings_byte_length, count)
            )
        return dictionary_buffer

    q1_buffer = build_dictionary_buffer(postings_sets[1])
    q2_buffer = build_dictionary_buffer(postings_sets[2])
    q3_buffer = build_dictionary_buffer(postings_sets[3])

    source_stat = (baseline_text_path or compressed_binary_path).stat()
    window_stat = static_window_path.stat()

    line_directory_offset = _COMPACT_HEADER_STRUCT.size
    q1_offset = line_directory_offset + len(line_directory_buffer)
    q2_offset = q1_offset + len(q1_buffer)
    q3_offset = q2_offset + len(q2_buffer)
    postings_offset = q3_offset + len(q3_buffer)
    line_slab_offset = postings_offset + len(postings_buffer)
    header_values = [
        source_stat.st_size,
        source_stat.st_mtime_ns,
        window_stat.st_size,
        window_stat.st_mtime_ns,
        logical_line_id,
        q1_offset,
        len(postings_sets[1]),
        q2_offset,
        len(postings_sets[2]),
        q3_offset,
        len(postings_sets[3]),
        postings_offset,
        len(postings_buffer),
        line_directory_offset,
        len(line_directory_buffer),
        line_slab_offset,
        len(line_slab_buffer),
    ]
    header = _COMPACT_HEADER_STRUCT.pack(
        STATIC_QGRAM_COMPACT_MAGIC,
        STATIC_QGRAM_COMPACT_INDEX_VERSION,
        _COMPACT_HEADER_STRUCT.size,
        *header_values,
    )

    index_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = index_path.with_suffix(index_path.suffix + ".tmp")
    with temporary_path.open("wb") as handle:
        handle.write(header)
        handle.write(line_directory_buffer)
        handle.write(q1_buffer)
        handle.write(q2_buffer)
        handle.write(q3_buffer)
        handle.write(postings_buffer)
        handle.write(line_slab_buffer)
    temporary_path.replace(index_path)
    return load_static_qgram_compact_index_header(index_path)


def load_static_qgram_index(index_path: Path) -> StaticQGramIndex:
    """Load a q-gram index and validate its schema version."""

    raw_payload = json.loads(index_path.read_text(encoding="utf-8"))
    if raw_payload.get("version") != STATIC_QGRAM_INDEX_VERSION:
        raise ValueError(
            f"Unsupported static q-gram index version {raw_payload.get('version')!r}; "
            f"expected {STATIC_QGRAM_INDEX_VERSION!r}."
        )

    record_directory = [
        RecordDirectoryEntry(
            record_id=int(entry["record_id"]),
            bit_offset=int(entry["bit_offset"]),
            flag=int(entry["flag"]),
            payload_bit_offset=int(entry["payload_bit_offset"]),
            payload_bit_length=int(entry["payload_bit_length"]),
            line_length=int(entry["line_length"]),
            window_id=_optional_int(entry.get("window_id")),
            raw_length=_optional_int(entry.get("raw_length")),
            decoded_length=int(entry["decoded_length"]),
            token_bitmap=int(entry["token_bitmap"]),
        )
        for entry in raw_payload["record_directory"]
    ]

    postings: dict[str, dict[str, list[int]]] = {
        str(gram_size): {
            str(gram): [int(record_id) for record_id in record_ids]
            for gram, record_ids in grams.items()
        }
        for gram_size, grams in raw_payload["postings"].items()
    }

    return StaticQGramIndex(
        index_path=index_path,
        compressed_binary_path=Path(raw_payload["compressed_binary_path"]),
        static_window_path=Path(raw_payload["static_window_path"]),
        record_directory=record_directory,
        postings=postings,
        record_count=int(raw_payload["record_count"]),
        source_size=int(raw_payload["source_size"]),
        source_mtime_ns=int(raw_payload["source_mtime_ns"]),
        window_size=int(raw_payload["window_size"]),
        window_mtime_ns=int(raw_payload["window_mtime_ns"]),
    )


def load_static_qgram_mmap_index_header(index_path: Path) -> StaticQGramMMapHeader:
    """Load only the fixed header from a binary mmap q-gram sidecar."""

    with index_path.open("rb") as handle:
        header_bytes = handle.read(_MMAP_HEADER_STRUCT.size)
    if len(header_bytes) != _MMAP_HEADER_STRUCT.size:
        raise ValueError(f"Static q-gram mmap index is too small: {index_path}")
    return _parse_mmap_header(header_bytes)


def load_static_qgram_compact_index_header(index_path: Path) -> StaticQGramCompactHeader:
    """Load only the fixed header from a compact qidx3 sidecar."""

    with index_path.open("rb") as handle:
        header_bytes = handle.read(_COMPACT_HEADER_STRUCT.size)
    if len(header_bytes) != _COMPACT_HEADER_STRUCT.size:
        raise ValueError(f"Static q-gram compact index is too small: {index_path}")
    return _parse_compact_header(header_bytes)


def open_static_qgram_mmap_index(index_path: Path) -> tuple[object, mmap.mmap, StaticQGramMMapView]:
    """Open a qidx2 file and return its handle, mmap, and parsed view."""

    handle = index_path.open("rb")
    try:
        mmap_buffer = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
    except Exception:
        handle.close()
        raise
    try:
        view = StaticQGramMMapView(mmap_buffer)
    except Exception:
        mmap_buffer.close()
        handle.close()
        raise
    return handle, mmap_buffer, view


def open_static_qgram_compact_index(index_path: Path) -> tuple[object, mmap.mmap, StaticQGramCompactMMapView]:
    """Open a qidx3 file and return its handle, mmap, and parsed view."""

    handle = index_path.open("rb")
    try:
        mmap_buffer = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
    except Exception:
        handle.close()
        raise
    try:
        view = StaticQGramCompactMMapView(mmap_buffer)
    except Exception:
        mmap_buffer.close()
        handle.close()
        raise
    return handle, mmap_buffer, view


def parse_static_record_directory(
    blocks: tuple[int, ...],
    total_bits: int,
) -> list[RecordDirectoryEntry]:
    """Parse a static bitstream into random-access record entries."""

    reader = _StaticBitReader(blocks, total_bits)
    record_directory: list[RecordDirectoryEntry] = []

    while reader.position < total_bits:
        bit_offset = reader.position
        flag = reader.read_bit()
        if flag is None:
            break
        token_bitmap = reader.read_int(WORD_BITMAP_BITS)

        if flag == 0:
            line_length = reader.read_int(ORIGINAL_LENGTH_COUNT)
            payload_bit_offset = reader.position
            payload_bit_length = line_length * 8
            reader.skip_bits(payload_bit_length)
            record_directory.append(
                RecordDirectoryEntry(
                    record_id=len(record_directory),
                    bit_offset=bit_offset,
                    flag=flag,
                    payload_bit_offset=payload_bit_offset,
                    payload_bit_length=payload_bit_length,
                    line_length=line_length,
                    window_id=None,
                    raw_length=line_length,
                    decoded_length=line_length,
                    token_bitmap=token_bitmap,
                )
            )
            continue

        window_id = reader.read_int(EACH_WINDOW_SIZE_COUNT)
        payload_bit_length = reader.read_int(STREAM_ENCODER_COUNT)
        payload_bit_offset = reader.position
        decoded_length = _decode_rle_payload_length(reader, payload_bit_length)
        record_directory.append(
            RecordDirectoryEntry(
                record_id=len(record_directory),
                bit_offset=bit_offset,
                flag=flag,
                payload_bit_offset=payload_bit_offset,
                payload_bit_length=payload_bit_length,
                line_length=decoded_length,
                window_id=window_id,
                raw_length=None,
                decoded_length=decoded_length,
                token_bitmap=token_bitmap,
            )
        )

    return record_directory


def decode_static_record_from_paths(
    compressed_binary_path: Path,
    parsed_window: ParsedWindow,
    entry: RecordDirectoryEntry,
) -> str:
    """Decode one static record by path and directory entry."""

    blocks, total_bits = _load_static_blocks(compressed_binary_path)
    return decode_static_record_from_blocks(blocks, total_bits, parsed_window, entry)


def decode_static_record_from_blocks(
    blocks: tuple[int, ...],
    total_bits: int,
    parsed_window: ParsedWindow,
    entry: RecordDirectoryEntry,
) -> str:
    """Decode one static record from a previously loaded bitstream."""

    reader = _StaticBitReader(blocks, total_bits, position=entry.payload_bit_offset)
    if entry.flag == 0:
        raw_length = entry.raw_length or 0
        return _decode_bytes_to_text(reader.read_bytes_from_bits(raw_length))

    xor_delta = _decode_rle_payload(reader, entry.payload_bit_length)
    line_length = len(xor_delta)
    templates = parsed_window.get(line_length)
    if not templates or entry.window_id is None or entry.window_id >= len(templates):
        return ""

    template_bytes = templates[entry.window_id].encode("utf-8", "ignore")
    if len(template_bytes) < line_length:
        template_bytes = template_bytes.ljust(line_length, b"\0")

    reconstructed = bytearray(xor_delta)
    for index in range(line_length):
        if reconstructed[index] == 0:
            reconstructed[index] = template_bytes[index]

    return _decode_bytes_to_text(bytes(reconstructed))


def keyword_search_loglite_static_qgram_index(
    bin_path: Path,
    static_window_path: Path,
    index_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Search static LogLite using exact q-gram postings plus verification."""

    keywords = _normalize_query_keywords(query_keywords)
    loaded_index = load_static_qgram_index(index_path)
    parsed_window = load_l_window_from_txt(static_window_path)
    blocks, total_bits = _load_static_blocks(bin_path)

    all_record_ids = set(range(loaded_index.record_count))
    candidate_ids = set(all_record_ids)
    for keyword in keywords:
        if keyword == "":
            continue

        gram_size, grams = qgrams_for_query_term(keyword)
        if gram_size == 0:
            continue

        postings_for_size = loaded_index.postings.get(str(gram_size), {})
        posting_lists: list[list[int]] = []
        missing_gram = False
        for gram in grams:
            postings = postings_for_size.get(gram)
            if postings is None:
                missing_gram = True
                break
            posting_lists.append(postings)

        if missing_gram:
            candidate_ids.clear()
            break

        term_candidates = set(intersect_sorted_postings(posting_lists))
        candidate_ids.intersection_update(term_candidates)
        if not candidate_ids:
            break

    record_by_id = {entry.record_id: entry for entry in loaded_index.record_directory}
    matches: list[str] = []
    decoded_bytes = 0
    sorted_candidate_ids = sorted(candidate_ids)
    for record_id in sorted_candidate_ids:
        entry = record_by_id[record_id]
        line = decode_static_record_from_blocks(blocks, total_bits, parsed_window, entry)
        decoded_bytes += len(line.encode("utf-8"))
        if all(keyword in line for keyword in keywords):
            matches.append(line)

    return ModeRunResult(
        matches=matches,
        decoded_records=len(sorted_candidate_ids),
        decoded_bytes=decoded_bytes,
        skipped_records=loaded_index.record_count - len(sorted_candidate_ids),
        skipped_bytes=None,
        fallback_count=0,
        total_records=loaded_index.record_count,
    )


def keyword_search_loglite_static_qgram_index_mmap(
    index_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Search static LogLite using a binary mmap q-gram sidecar."""

    keywords = _normalize_query_keywords(query_keywords)
    keyword_bytes = [keyword.encode("utf-8") for keyword in keywords]
    handle, mmap_buffer, view = open_static_qgram_mmap_index(index_path)
    try:
        candidate_ids = set(range(view.record_count))
        for keyword in keywords:
            if keyword == "":
                continue

            gram_size, gram_values = qgram_values_for_query_term(keyword)
            if gram_size == 0:
                continue

            posting_lists: list[list[int]] = []
            missing_gram = False
            for gram_value in gram_values:
                postings = view.get_postings(gram_size, gram_value)
                if not postings:
                    missing_gram = True
                    break
                posting_lists.append(postings)

            if missing_gram:
                candidate_ids.clear()
                break

            candidate_ids.intersection_update(intersect_sorted_postings(posting_lists))
            if not candidate_ids:
                break

        matches: list[str] = []
        decoded_bytes = 0
        sorted_candidate_ids = sorted(candidate_ids)
        for record_id in sorted_candidate_ids:
            line_offset, line_length = view.line_location(record_id)
            decoded_bytes += line_length
            line_start = view.header.line_slab_offset + line_offset
            line_bytes = mmap_buffer[line_start : line_start + line_length]
            if all(keyword in line_bytes for keyword in keyword_bytes):
                matches.append(_decode_bytes_to_text(line_bytes))

        return ModeRunResult(
            matches=matches,
            decoded_records=len(sorted_candidate_ids),
            decoded_bytes=decoded_bytes,
            skipped_records=view.record_count - len(sorted_candidate_ids),
            skipped_bytes=None,
            fallback_count=0,
            total_records=view.record_count,
        )
    finally:
        mmap_buffer.close()
        handle.close()


def keyword_search_loglite_static_qgram_index_mmap_compact(
    index_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Search static LogLite using compact qidx3 plus planned exact verification."""

    keywords = _normalize_query_keywords(query_keywords)
    keyword_bytes = [keyword.encode("utf-8") for keyword in keywords]
    handle, mmap_buffer, view = open_static_qgram_compact_index(index_path)
    try:
        gram_requests, missing_required_gram, estimated_postings_ids = _collect_compact_gram_requests(
            view,
            keywords,
        )
        use_slab_scan = _choose_compact_planner_strategy(view.record_count, gram_requests, estimated_postings_ids)
        planner_strategy = "line_slab_scan" if use_slab_scan else "postings_intersection"

        postings_lists_touched = 0
        postings_ids_read = 0
        if missing_required_gram:
            candidate_ids: list[int] = []
            planner_strategy = "missing_gram"
        elif use_slab_scan or not gram_requests:
            candidate_ids = list(range(view.record_count))
        else:
            posting_lists: list[list[int]] = []
            for gram_size, gram_value, count in sorted(gram_requests, key=lambda item: item[2]):
                postings = view.get_postings(gram_size, gram_value)
                postings_lists_touched += 1
                postings_ids_read += len(postings)
                if not postings:
                    posting_lists = []
                    break
                posting_lists.append(postings)
            candidate_ids = intersect_sorted_postings(posting_lists) if posting_lists else []

        matches: list[str] = []
        verified_bytes = 0
        for record_id in candidate_ids:
            line_offset, line_length = view.line_location(record_id)
            verified_bytes += line_length
            line_start = view.header.line_slab_offset + line_offset
            line_bytes = mmap_buffer[line_start : line_start + line_length]
            if all(keyword in line_bytes for keyword in keyword_bytes):
                matches.append(_decode_bytes_to_text(line_bytes))

        verified_records = len(candidate_ids)
        return ModeRunResult(
            matches=matches,
            decoded_records=verified_records,
            decoded_bytes=verified_bytes,
            skipped_records=view.record_count - verified_records,
            skipped_bytes=None,
            fallback_count=0,
            total_records=view.record_count,
            planner_strategy=planner_strategy,
            postings_lists_touched=postings_lists_touched,
            postings_ids_read=postings_ids_read,
            verified_records=verified_records,
            verified_bytes=verified_bytes,
        )
    finally:
        mmap_buffer.close()
        handle.close()


def keyword_search_loglite_static_qgram_index_mmap_cpp(
    index_path: Path,
    query_keywords: str | tuple[str, ...] | list[str],
) -> ModeRunResult:
    """Search qidx3 with the native mmap helper and return exact matched lines."""

    keywords = _normalize_query_keywords(query_keywords)
    binary_path = ensure_qidx_search_binary()
    completed_process = subprocess.run(
        [str(binary_path), str(index_path), *keywords],
        cwd=get_project_root(),
        capture_output=True,
        text=False,
    )
    if completed_process.returncode != 0:
        raise RuntimeError(
            "Native qidx-search failed.\n"
            f"command: {binary_path} {index_path}\n"
            f"stderr:\n{completed_process.stderr.decode('utf-8', 'ignore')}"
        )
    output = completed_process.stdout.decode("utf-8", "ignore")
    matches = output.split("\n")
    if matches and matches[-1] == "":
        matches.pop()
    native_stats = _parse_native_qidx_stats(completed_process.stderr)
    try:
        header = load_static_qgram_compact_index_header(index_path)
        total_records = native_stats.get("total_records", header.record_count)
    except (OSError, ValueError, struct.error):
        total_records = native_stats.get("total_records")
    verified_records = native_stats.get("verified_records")
    skipped_records = (
        None if total_records is None or verified_records is None else max(0, total_records - verified_records)
    )
    planner_strategy = native_stats.get("planner_strategy")
    return ModeRunResult(
        matches=matches,
        decoded_records=verified_records,
        decoded_bytes=native_stats.get("verified_bytes"),
        skipped_records=skipped_records,
        skipped_bytes=None,
        fallback_count=0,
        total_records=total_records,
        planner_strategy=f"cpp_{planner_strategy}" if isinstance(planner_strategy, str) else "cpp_qidx3_planned_search",
        postings_lists_touched=native_stats.get("postings_lists_touched"),
        postings_ids_read=native_stats.get("postings_ids_read"),
        verified_records=verified_records,
        verified_bytes=native_stats.get("verified_bytes"),
    )


def _decode_rle_payload_length(reader: _StaticBitReader, payload_bit_length: int) -> int:
    """Decode only the output byte length of an RLE payload."""

    start_position = reader.position
    bits_consumed = 0
    decoded_length = 0
    while bits_consumed < payload_bit_length and reader.position < reader.total_bits:
        tag_bit = reader.read_bit()
        if tag_bit is None:
            break
        bits_consumed += 1
        if tag_bit == 1:
            reader.skip_bits(8)
            bits_consumed += 8
            decoded_length += 1
        else:
            zero_count = reader.read_int(RLE_COUNT)
            bits_consumed += RLE_COUNT
            decoded_length += zero_count

    reader.position = min(start_position + payload_bit_length, reader.total_bits)
    return decoded_length


def _parse_native_qidx_stats(stderr_bytes: bytes) -> dict[str, Any]:
    """Parse the native qidx-search instrumentation line from stderr."""

    stderr_text = stderr_bytes.decode("utf-8", "ignore")
    for raw_line in stderr_text.splitlines():
        if not raw_line.startswith("QIDX3_STATS "):
            continue
        stats: dict[str, Any] = {}
        for token in raw_line.removeprefix("QIDX3_STATS ").split():
            key, separator, value = token.partition("=")
            if not separator:
                continue
            if key == "planner_strategy":
                stats[key] = value
                continue
            try:
                stats[key] = int(value)
            except ValueError:
                continue
        return stats
    return {}


def _intersect_two_sorted_lists(left: list[int], right: list[int]) -> list[int]:
    """Intersect two sorted integer lists with a linear two-pointer scan."""

    left_index = 0
    right_index = 0
    result: list[int] = []
    while left_index < len(left) and right_index < len(right):
        left_value = left[left_index]
        right_value = right[right_index]
        if left_value == right_value:
            result.append(left_value)
            left_index += 1
            right_index += 1
        elif left_value < right_value:
            left_index += 1
        else:
            right_index += 1
    return result


def _decode_rle_payload(reader: _StaticBitReader, payload_bit_length: int) -> bytearray:
    """Decode an RLE payload into its XOR-delta bytes."""

    bits_consumed = 0
    xor_delta = bytearray()
    while bits_consumed < payload_bit_length and reader.position < reader.total_bits:
        tag_bit = reader.read_bit()
        if tag_bit is None:
            break
        bits_consumed += 1
        if tag_bit == 1:
            xor_delta.append(reader.read_int(8) & 0xFF)
            bits_consumed += 8
        else:
            zero_count = reader.read_int(RLE_COUNT)
            bits_consumed += RLE_COUNT
            xor_delta.extend(b"\x00" * zero_count)

    reader.position = min(reader.position, reader.total_bits)
    return xor_delta


def _load_static_blocks(bin_path: Path) -> tuple[tuple[int, ...], int]:
    """Load LogLite dynamic-bitset blocks and logical bit length."""

    if not bin_path.exists():
        raise FileNotFoundError(f"Static compressed artifact not found: {bin_path}")

    data = bin_path.read_bytes()
    if len(data) < 16:
        raise ValueError(f"Static compressed artifact is too small to parse: {bin_path}")

    file_size = len(data)
    last_block_bits = struct.unpack("<Q", data[file_size - 8 :])[0]
    blocks_bytes = data[: file_size - 8]
    if len(blocks_bytes) % 8 != 0:
        raise ValueError(f"Malformed static compressed artifact block layout: {bin_path}")

    num_blocks = len(blocks_bytes) // 8
    blocks = struct.unpack("<" + "Q" * num_blocks, blocks_bytes)
    total_bits = (num_blocks - 1) * 64 + (last_block_bits or 64)
    return blocks, total_bits


def _collect_compact_gram_requests(
    view: StaticQGramCompactMMapView,
    keywords: list[str],
) -> tuple[list[tuple[int, int, int]], bool, int]:
    """Collect required q-grams with cheap posting-cardinality estimates."""

    required_grams: dict[tuple[int, int], int] = {}
    missing_required_gram = False
    for keyword in keywords:
        if keyword == "":
            continue
        gram_size, gram_values = qgram_values_for_query_term(keyword)
        if gram_size == 0:
            continue
        for gram_value in gram_values:
            key = (gram_size, gram_value)
            if key in required_grams:
                continue
            count = view.get_postings_count(gram_size, gram_value)
            if count == 0:
                missing_required_gram = True
                required_grams.clear()
                break
            required_grams[key] = count
        if missing_required_gram:
            break
    gram_requests = [
        (gram_size, gram_value, count)
        for (gram_size, gram_value), count in required_grams.items()
    ]
    estimated_postings_ids = sum(count for _gram_size, _gram_value, count in gram_requests)
    return gram_requests, missing_required_gram, estimated_postings_ids


def _choose_compact_planner_strategy(
    record_count: int,
    gram_requests: list[tuple[int, int, int]],
    estimated_postings_ids: int,
) -> bool:
    """Return True when direct slab verification is cheaper than postings planning."""

    if record_count <= 0:
        return False
    if not gram_requests:
        return True
    broad_short_gram = any(gram_size <= 2 and count >= record_count * 0.75 for gram_size, _gram, count in gram_requests)
    if broad_short_gram:
        return True
    return estimated_postings_ids >= record_count * 2


def _parse_mmap_header(buffer: bytes | mmap.mmap) -> StaticQGramMMapHeader:
    """Parse and validate a qidx2 fixed header from bytes or mmap."""

    unpacked = _MMAP_HEADER_STRUCT.unpack_from(buffer, 0)
    magic = unpacked[0]
    version = int(unpacked[1])
    header_size = int(unpacked[2])
    values = [int(value) for value in unpacked[3:]]
    if magic != STATIC_QGRAM_MMAP_MAGIC:
        raise ValueError(f"Unsupported static q-gram mmap magic: {magic!r}")
    if version != STATIC_QGRAM_MMAP_INDEX_VERSION:
        raise ValueError(
            f"Unsupported static q-gram mmap version {version}; expected {STATIC_QGRAM_MMAP_INDEX_VERSION}."
        )
    if header_size != _MMAP_HEADER_STRUCT.size:
        raise ValueError(f"Unsupported static q-gram mmap header size: {header_size}")
    return StaticQGramMMapHeader(
        version=version,
        header_size=header_size,
        source_size=values[_HEADER_SOURCE_SIZE],
        source_mtime_ns=values[_HEADER_SOURCE_MTIME_NS],
        window_size=values[_HEADER_WINDOW_SIZE],
        window_mtime_ns=values[_HEADER_WINDOW_MTIME_NS],
        record_count=values[_HEADER_RECORD_COUNT],
        q1_offset=values[_HEADER_Q1_OFFSET],
        q1_count=values[_HEADER_Q1_COUNT],
        q2_offset=values[_HEADER_Q2_OFFSET],
        q2_count=values[_HEADER_Q2_COUNT],
        q3_offset=values[_HEADER_Q3_OFFSET],
        q3_count=values[_HEADER_Q3_COUNT],
        postings_offset=values[_HEADER_POSTINGS_OFFSET],
        postings_size=values[_HEADER_POSTINGS_SIZE],
        record_directory_offset=values[_HEADER_RECORD_DIRECTORY_OFFSET],
        record_directory_size=values[_HEADER_RECORD_DIRECTORY_SIZE],
        line_directory_offset=values[_HEADER_LINE_DIRECTORY_OFFSET],
        line_directory_size=values[_HEADER_LINE_DIRECTORY_SIZE],
        line_slab_offset=values[_HEADER_LINE_SLAB_OFFSET],
        line_slab_size=values[_HEADER_LINE_SLAB_SIZE],
    )


def _parse_compact_header(buffer: bytes | mmap.mmap) -> StaticQGramCompactHeader:
    """Parse and validate a qidx3 fixed header from bytes or mmap."""

    unpacked = _COMPACT_HEADER_STRUCT.unpack_from(buffer, 0)
    magic = unpacked[0]
    version = int(unpacked[1])
    header_size = int(unpacked[2])
    values = [int(value) for value in unpacked[3:]]
    if magic != STATIC_QGRAM_COMPACT_MAGIC:
        raise ValueError(f"Unsupported static q-gram compact magic: {magic!r}")
    if version != STATIC_QGRAM_COMPACT_INDEX_VERSION:
        raise ValueError(
            f"Unsupported static q-gram compact version {version}; expected {STATIC_QGRAM_COMPACT_INDEX_VERSION}."
        )
    if header_size != _COMPACT_HEADER_STRUCT.size:
        raise ValueError(f"Unsupported static q-gram compact header size: {header_size}")
    return StaticQGramCompactHeader(
        version=version,
        header_size=header_size,
        source_size=values[_COMPACT_HEADER_SOURCE_SIZE],
        source_mtime_ns=values[_COMPACT_HEADER_SOURCE_MTIME_NS],
        window_size=values[_COMPACT_HEADER_WINDOW_SIZE],
        window_mtime_ns=values[_COMPACT_HEADER_WINDOW_MTIME_NS],
        record_count=values[_COMPACT_HEADER_RECORD_COUNT],
        q1_offset=values[_COMPACT_HEADER_Q1_OFFSET],
        q1_count=values[_COMPACT_HEADER_Q1_COUNT],
        q2_offset=values[_COMPACT_HEADER_Q2_OFFSET],
        q2_count=values[_COMPACT_HEADER_Q2_COUNT],
        q3_offset=values[_COMPACT_HEADER_Q3_OFFSET],
        q3_count=values[_COMPACT_HEADER_Q3_COUNT],
        postings_offset=values[_COMPACT_HEADER_POSTINGS_OFFSET],
        postings_size=values[_COMPACT_HEADER_POSTINGS_SIZE],
        line_directory_offset=values[_COMPACT_HEADER_LINE_DIRECTORY_OFFSET],
        line_directory_size=values[_COMPACT_HEADER_LINE_DIRECTORY_SIZE],
        line_slab_offset=values[_COMPACT_HEADER_LINE_SLAB_OFFSET],
        line_slab_size=values[_COMPACT_HEADER_LINE_SLAB_SIZE],
    )


def _index_needs_rebuild(index_path: Path, compressed_binary_path: Path, static_window_path: Path) -> bool:
    """Return whether a q-gram index is missing, stale, or incompatible."""

    if not index_path.exists():
        return True
    try:
        payload: dict[str, Any] = json.loads(index_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return True
    if payload.get("version") != STATIC_QGRAM_INDEX_VERSION:
        return True

    source_stat = compressed_binary_path.stat()
    window_stat = static_window_path.stat()
    return (
        int(payload.get("source_size", -1)) != source_stat.st_size
        or int(payload.get("source_mtime_ns", -1)) != source_stat.st_mtime_ns
        or int(payload.get("window_size", -1)) != window_stat.st_size
        or int(payload.get("window_mtime_ns", -1)) != window_stat.st_mtime_ns
    )


def _mmap_index_needs_rebuild(index_path: Path, compressed_binary_path: Path, static_window_path: Path) -> bool:
    """Return whether a binary q-gram mmap index is missing, stale, or incompatible."""

    if not index_path.exists():
        return True
    try:
        header = load_static_qgram_mmap_index_header(index_path)
    except (OSError, ValueError, struct.error):
        return True

    source_stat = compressed_binary_path.stat()
    window_stat = static_window_path.stat()
    return (
        header.source_size != source_stat.st_size
        or header.source_mtime_ns != source_stat.st_mtime_ns
        or header.window_size != window_stat.st_size
        or header.window_mtime_ns != window_stat.st_mtime_ns
        or header.q2_offset != header.q1_offset + header.q1_count * _POSTING_TABLE_ENTRY_STRUCT.size
        or header.q3_offset != header.q2_offset + header.q2_count * _POSTING_TABLE_ENTRY_STRUCT.size
        or header.postings_offset != header.q3_offset + header.q3_count * _Q3_DICTIONARY_ENTRY_STRUCT.size
    )


def _compact_index_needs_rebuild(
    index_path: Path,
    compressed_binary_path: Path,
    static_window_path: Path,
    baseline_text_path: Path | None = None,
) -> bool:
    """Return whether a compact qidx3 index is missing, stale, or incompatible."""

    if not index_path.exists():
        return True
    try:
        header = load_static_qgram_compact_index_header(index_path)
    except (OSError, ValueError, struct.error):
        return True

    source_stat = (baseline_text_path or compressed_binary_path).stat()
    window_stat = static_window_path.stat()
    return (
        header.source_size != source_stat.st_size
        or header.source_mtime_ns != source_stat.st_mtime_ns
        or header.window_size != window_stat.st_size
        or header.window_mtime_ns != window_stat.st_mtime_ns
        or header.line_directory_offset != _COMPACT_HEADER_STRUCT.size
        or header.line_directory_size != header.record_count * _LINE_DIRECTORY_STRUCT.size
        or header.q1_offset != header.line_directory_offset + header.line_directory_size
        or header.q2_offset != header.q1_offset + header.q1_count * _COMPACT_DICTIONARY_ENTRY_STRUCT.size
        or header.q3_offset != header.q2_offset + header.q2_count * _COMPACT_DICTIONARY_ENTRY_STRUCT.size
        or header.postings_offset != header.q3_offset + header.q3_count * _COMPACT_DICTIONARY_ENTRY_STRUCT.size
        or header.line_slab_offset != header.postings_offset + header.postings_size
    )


def ensure_qidx_search_binary(force_rebuild: bool = False) -> Path:
    """Compile the native qidx-search helper if needed and return its path."""

    source_path = get_project_root() / "query_eval" / "native" / "qidx_search.cc"
    if not source_path.exists():
        raise FileNotFoundError(f"Native qidx-search source not found: {source_path}")

    runtime_root = get_runtime_root()
    runtime_root.mkdir(parents=True, exist_ok=True)
    binary_name = "qidx-search.exe" if platform.system().lower().startswith("win") else "qidx-search"
    binary_path = runtime_root / binary_name
    if (
        binary_path.exists()
        and not force_rebuild
        and binary_path.stat().st_mtime_ns >= source_path.stat().st_mtime_ns
    ):
        return binary_path

    compiler = (
        shutil.which("clang++")
        or shutil.which("g++")
        or shutil.which("c++")
    )
    if compiler is None:
        raise RuntimeError("static_qgram_index_mmap_cpp requires clang++, g++, or c++ on PATH.")

    command = [
        compiler,
        "-std=c++17",
        "-O3",
        "-DNDEBUG",
        str(source_path),
        "-o",
        str(binary_path),
    ]
    completed_process = subprocess.run(command, cwd=get_project_root(), capture_output=True, text=True)
    if completed_process.returncode != 0:
        raise RuntimeError(
            "Failed to compile qidx-search.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed_process.stdout}\n"
            f"stderr:\n{completed_process.stderr}"
        )
    return binary_path


def _normalize_query_keywords(query_keywords: str | tuple[str, ...] | list[str]) -> list[str]:
    """Normalize query input into required substring terms."""

    if isinstance(query_keywords, str):
        return [query_keywords]
    return list(query_keywords)


def _decode_bytes_to_text(raw_bytes: bytes) -> str:
    """Decode bytes with the permissive baseline semantics."""

    return raw_bytes.decode("utf-8", "ignore")


def _baseline_logical_lines_from_record_text(record_text: str) -> list[str]:
    """Return logical text lines produced by the decompressed-text baseline."""

    return [raw_line.rstrip("\n") for raw_line in io.StringIO(record_text + "\n", newline=None)]


def _baseline_logical_lines_from_text_path(text_path: Path):
    """Yield logical lines exactly as the plaintext baseline scans them."""

    with text_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for raw_line in handle:
            yield raw_line.rstrip("\n")


def _optional_int(value: Any) -> int | None:
    """Convert a JSON optional integer field."""

    if value is None:
        return None
    return int(value)
