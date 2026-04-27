
from collections import deque
from pathlib import Path
import struct

def keyword_search_loglite_static_bloom(bin_path, parsed_l_window, query_keywords):
    """
    Search keyword(s) over a LogLite-B static-window compressed binary
    that was produced with the per-record 64-bit word bitmap extension.

    Filtering strategy:
        For each query keyword, hash every alphanumeric token in the keyword
        to a bit position in a 64-bit vector (same hash logic as computeWordBitmap
        in stream_compress.cc). The resulting query_bitmap is the OR of all
        per-keyword bit positions.

        For each entry in the bitstream:
            1. Read the flag bit (0 = raw, 1 = compressed).
            2. Read the 64-bit per-record word bitmap written by the compressor.
            3. If (record_bitmap & query_bitmap) != query_bitmap, the record
               cannot contain all keywords — skip it.
            4. Otherwise decompress the record and perform the exact keyword check.


    Args:
        bin_path:        Path to the .lite.static.b compressed binary file.
        parsed_l_window: Dict[int, List[str]] from load_l_window_from_txt().
                         Used only to populate allowed_lengths pre-filter.
        query_keywords:  Single keyword string or list of keyword strings.

    Returns:
        List[str]: Reconstructed log lines that contain all query keyword(s).
    """
    # Constants (must exactly match constants.h + static build)
    EACH_WINDOW_SIZE_COUNT = 5
    STREAM_ENCODER_COUNT   = 13
    ORIGINAL_LENGTH_COUNT  = 15
    WORD_BITMAP_BITS       = 64
    MAX_LEN                = 10000
    RLE_COUNT              = 8

    keywords = [query_keywords] if isinstance(query_keywords, str) else list(query_keywords)
    keywords_bytes = [k.encode("utf-8") for k in keywords]
    results = []

    # 
    def compute_word_bitmap(text: str) -> int:
        """
        Deterministic 6-bit hash per alphanumeric token.
        Exact same logic as the C++ version below.
        """
        bitmap = 0
        token = []
        for ch in text:
            if ch.isalnum():
                token.append(ch)
            else:
                if token:
                    h = 0
                    for c in token:
                        h = ((h << 5) + h) ^ ord(c)   # h = h*33 ^ ord(c)
                    idx = h & 63
                    bitmap |= (1 << idx)
                    token = []
        if token:
            h = 0
            for c in token:
                h = ((h << 5) + h) ^ ord(c)
            idx = h & 63
            bitmap |= (1 << idx)
        return bitmap

    query_bitmap = 0
    for kw in keywords:
        query_bitmap |= compute_word_bitmap(kw)

    # --- Load static L-window (parsed_l_window is authoritative) ---
    # parsed_l_window: Dict[int, List[str]]  -- length -> list of templates (up to 32)

    # --- Load compressed bitstream ---
    data = Path(bin_path).read_bytes()
    if len(data) < 16:
        return results

    ulong_size = 8
    file_size  = len(data)
    last_block_bits = struct.unpack("<Q", data[file_size - 8:])[0]
    blocks_bytes    = data[: file_size - 8]
    num_blocks      = len(blocks_bytes) // ulong_size
    blocks          = struct.unpack("<" + "Q" * num_blocks, blocks_bytes)
    bits_per_block  = 64
    total_bits      = (num_blocks - 1) * bits_per_block + (last_block_bits or bits_per_block)

    pos = 0

    def read_bit():
        nonlocal pos
        if pos >= total_bits:
            return None
        bit = (blocks[pos // 64] >> (pos % 64)) & 1
        pos += 1
        return bit

    def read_int(bit_count):
        v = 0
        for i in range(bit_count):
            b = read_bit()
            if b is not None and b:
                v |= (1 << i)
        return v

    def skip_bits(n):
        nonlocal pos
        pos = min(pos + n, total_bits)

    # --- Main bitstream walk with bloom pre-filter ---
    while True:
        flag = read_bit()
        if flag is None:
            break

        # Every record has the 64-bit word bitmap right after flag
        record_bitmap = read_int(WORD_BITMAP_BITS)

        # Bloom filter fast reject: guaranteed no match if any required bit is missing
        if (record_bitmap & query_bitmap) != query_bitmap:
            # Skip entire payload without any decoding
            if flag == 0:  # RAW
                length = read_int(ORIGINAL_LENGTH_COUNT)
                skip_bits(length * 8)
            else:  # COMPRESSED
                window_id   = read_int(EACH_WINDOW_SIZE_COUNT)
                rle_bit_len = read_int(STREAM_ENCODER_COUNT)
                skip_bits(rle_bit_len)
            continue

        # Bloom passed → candidate, must decode and exact-check
        if flag == 0:  # RAW LINE
            length = read_int(ORIGINAL_LENGTH_COUNT)
            line_bytes = bytearray(length)
            for i in range(length):
                line_bytes[i] = read_int(8)
            line_bytes = bytes(line_bytes)

            if all(k in line_bytes for k in keywords_bytes):
                results.append(line_bytes.decode("utf-8", "ignore"))

        else:  # COMPRESSED LINE (static window lookup)
            window_id   = read_int(EACH_WINDOW_SIZE_COUNT)
            rle_bit_len = read_int(STREAM_ENCODER_COUNT)

            # Decode RLE XOR delta
            xor_delta = bytearray()
            bits_consumed = 0
            while bits_consumed < rle_bit_len:
                rle_type = read_bit()
                if rle_type is None:
                    break
                bits_consumed += 1

                if rle_type == 1:  # Literal byte
                    xor_delta.append(read_int(8))
                    bits_consumed += 8
                else:  # Zero run
                    count = read_int(RLE_COUNT)
                    bits_consumed += RLE_COUNT
                    xor_delta.extend(b"\x00" * count)

            length = len(xor_delta)

            # Static lookup from parsed_l_window
            templates = parsed_l_window.get(length)
            if not templates or window_id >= len(templates):
                continue  # malformed or out-of-range window_id

            template_bytes = templates[window_id].encode("utf-8", "ignore")
            if len(template_bytes) < length:
                template_bytes = template_bytes.ljust(length, b'\0')

            # Reconstruct: replace 0-bytes with template bytes
            reconstructed = bytearray(xor_delta)
            for i in range(length):
                if reconstructed[i] == 0:
                    reconstructed[i] = template_bytes[i]

            final_bytes = bytes(reconstructed)

            if all(k in final_bytes for k in keywords_bytes):
                results.append(final_bytes.decode("utf-8", "ignore"))

    return results


def keyword_search_loglite_binary_minor_optimization(bin_path, parsed_l_window, query_keywords):
    """
    Search for keyword(s) in a LogLite compressed binary file (.lite.b)
    using a two-step filtering strategy.

    Step 1 - L-Window Pre-filter:
        Search the parsed L-window templates for entries that contain all
        query keyword(s). Collect the lengths of matching templates into a
        set of 'allowed lengths'. Only log lines whose length matches one
        of these allowed lengths can possibly contain the keyword(s).

    Step 2 - Selective Binary Pass:
        Walk the compressed bitstream entry by entry:

        - Flag 0 (RAW line):
            Read the next 15 bits to get the original string length.
            If this length is in allowed_lengths, read and decompress
            the raw bytes, then perform the keyword check.
            If the length is NOT in allowed_lengths, skip the raw bytes
            entirely (length * 8 bits) without any decoding.

        - Flag 1 (COMPRESSED line):
            Read window_id (3 bits) and rle_bit_len (13 bits).
            Decode the full RLE payload to determine the reconstructed
            string length (sum of all zero-run counts and literal bytes).
            If this length is in allowed_lengths, perform reconstruction
            against the window template and keyword check.
            If NOT in allowed_lengths, skip without reconstruction.
            NOTE: skipped compressed lines do NOT update the window state,
            which may cause incorrect decompression for subsequent lines
            that reference a template built from a skipped line.
            
    Limitation is that decompression would probably be wrong as we are not updating state (l_window) 
    for every log which is required for correctness. The purpose of this is to see how many results we get back 
    with the optimization and to measure performance benefits. Correctness of logs being decompressed correctly would 
    be ensured in later versions.
    """    
    # Constants from constants.h
    EACH_WINDOW_SIZE_COUNT = 3
    STREAM_ENCODER_COUNT = 13
    ORIGINAL_LENGTH_COUNT = 15
    MAX_LEN = 10000
    RLE_COUNT = 8
    EACH_WINDOW_SIZE = 1 << EACH_WINDOW_SIZE_COUNT

    # Normalize keywords
    keywords = [query_keywords] if isinstance(query_keywords, str) else list(query_keywords)
    keywords_bytes = [k.encode('utf-8') for k in keywords]
    results = []

    # --- STEP 2: Search l_window templates for keyword matches ---
    # Store lengths of templates that contain the keyword(s)
    allowed_lengths = set()
    for length, templates in parsed_l_window.items():
        for template in templates:
            if all(k in template for k in keywords):
                allowed_lengths.add(length)
                break  # one match in this length bucket is enough

    # print(f"Allowed lengths from l_window: {sorted(allowed_lengths)}")

    # --- Bitstream Loading ---
    data = Path(bin_path).read_bytes()
    if len(data) < 16:
        return results

    ulong_size = 8
    file_size = len(data)
    last_block_bits = struct.unpack('<Q', data[file_size - 8:])[0]
    blocks_bytes = data[:file_size - 8]
    num_blocks = len(blocks_bytes) // ulong_size
    blocks = struct.unpack('<' + 'Q' * num_blocks, blocks_bytes)
    bits_per_block = 64
    total_bits = (num_blocks - 1) * bits_per_block + (last_block_bits or bits_per_block)

    pos = 0

    def read_bit():
        nonlocal pos
        if pos >= total_bits: return None
        bit = (blocks[pos // 64] >> (pos % 64)) & 1
        pos += 1
        return bit

    def read_int(bit_count):
        v = 0
        for i in range(bit_count):
            b = read_bit()
            if b is not None and b: v |= (1 << i)
        return v

    def skip_bits(n):
        nonlocal pos
        pos += n

    # --- STEP 3: Pass over compressed binary ---
    window = {}  # dict[int, deque[bytes]]

    while True:
        flag = read_bit()
        if flag is None:
            break

        if flag == 0:  # RAW LINE
            length = read_int(ORIGINAL_LENGTH_COUNT)

            if length in allowed_lengths:
                # Decompress and check
                line_bytes = bytearray(length)
                for i in range(length):
                    line_bytes[i] = read_int(8)
                line_bytes = bytes(line_bytes)

                if length < MAX_LEN:
                    window[length] = deque([line_bytes], maxlen=EACH_WINDOW_SIZE)

                if all(k in line_bytes for k in keywords_bytes):
                    results.append(line_bytes.decode('utf-8', 'ignore'))
            else:
                # Skip raw bytes entirely
                skip_bits(length * 8)

        else:  # COMPRESSED LINE
            window_id = read_int(EACH_WINDOW_SIZE_COUNT)
            rle_bit_len = read_int(STREAM_ENCODER_COUNT)

            # Decode RLE payload to find original length
            xor_delta = bytearray()
            bits_consumed = 0
            while bits_consumed < rle_bit_len:
                rle_type = read_bit()
                bits_consumed += 1
                if rle_type == 1:  # Literal
                    xor_delta.append(read_int(8))
                    bits_consumed += 8
                else:  # Zero Run
                    count = read_int(RLE_COUNT)
                    bits_consumed += RLE_COUNT
                    xor_delta.extend(b'\x00' * count)

            length = len(xor_delta)

            if length in allowed_lengths:
                # Decompress and check
                dq = window.get(length)
                if not dq or window_id >= len(dq):
                    continue

                template_bytes = dq[window_id]
                reconstructed = bytearray(xor_delta)
                for i in range(length):
                    if reconstructed[i] == 0:
                        reconstructed[i] = template_bytes[i]
                final_bytes = bytes(reconstructed)

                dq.append(final_bytes)

                if all(k in final_bytes for k in keywords_bytes):
                    results.append(final_bytes.decode('utf-8', 'ignore'))
            else:
                # Skip — but window state is NOT updated
                # This will cause wrong decompression for later lines
                # of this length if they reference a skipped template
                pass

    return results

def keyword_search_loglite_binary_full_decompression(bin_path, query_keywords):
    """
    Keyword search over a LogLite .lite.b file, using the *true*
    bit-level format implemented in LogLite-B (see stream_compress.cc
    and xorc-cli.cc).

    `query_keywords` can be either a single string or an iterable of strings.
    A line is a match only if it contains *all* keywords.

    This mirrors the C++ decompression logic:
    - Read the global boost::dynamic_bitset written by write_bitset_to_file.
    - Walk it entry-by-entry, exactly as xorc-cli's decompression loop does.
    - Reconstruct each original log line using the same sliding window rules.
    - Check each reconstructed line for the query keywords and collect matches.

   
    """

    # Normalize keywords to a list of strings
    if isinstance(query_keywords, str):
        keywords = [query_keywords]
    else:
        keywords = list(query_keywords)

    # Constants from constants.h
    EACH_WINDOW_SIZE_COUNT = 3
    STREAM_ENCODER_COUNT = 13
    ORIGINAL_LENGTH_COUNT = 15
    MAX_LEN = 10000
    RLE_COUNT = 8
    RLE_SKIM = 8
    EACH_WINDOW_SIZE = 1 << EACH_WINDOW_SIZE_COUNT

    results = []

    bin_path = Path(bin_path)
    if not bin_path.exists():
        print(f"Error: File {bin_path} not found.")
        return results

    data = bin_path.read_bytes()
    if len(data) < 16:
        return results

    # Rebuild the boost::dynamic_bitset bitstream written by write_bitset_to_file
    ulong_size = 8  # sizeof(unsigned long) on 64-bit Linux
    size_t_size = 8 # sizeof(size_t)
    file_size = len(data)

    last_block_bits = struct.unpack('<Q', data[file_size - size_t_size : file_size])[0]
    blocks_bytes = data[: file_size - size_t_size]
    if len(blocks_bytes) % ulong_size != 0:
        return results

    num_blocks = len(blocks_bytes) // ulong_size
    blocks = struct.unpack('<' + 'Q' * num_blocks, blocks_bytes)
    bits_per_block = ulong_size * 8
    total_bits = (num_blocks - 1) * bits_per_block + (last_block_bits or bits_per_block)

    pos = 0

    def read_bit():
        nonlocal pos
        if pos >= total_bits:
            return None
        block_idx = pos // bits_per_block
        offset = pos % bits_per_block
        bit = (blocks[block_idx] >> offset) & 1
        pos += 1
        return bit

    def read_int(bit_count: int) -> int:
        v = 0
        for j in range(bit_count):
            b = read_bit()
            if b is None:
                return v
            if b:
                v |= (1 << j)
        return v

    def read_bytes_from_bits(num_bytes: int) -> bytes:
        buf = bytearray(num_bytes)
        for i in range(num_bytes):
            val = read_int(8)
            buf[i] = val & 0xFF
        return bytes(buf)

    # Sliding window used during decompression: length -> deque of recent lines
    window = {}  # type: dict[int, deque[str]]

    while True:
        flag = read_bit()
        if flag is None:
            break

        if flag == 0:
            # Raw log line
            length = read_int(ORIGINAL_LENGTH_COUNT)
            if length <= 0:
                continue
            line_bytes = read_bytes_from_bits(length)
            try:
                line = line_bytes.decode('utf-8', 'ignore')
            except UnicodeDecodeError:
                line = line_bytes.decode('latin1', 'ignore')

            # Update window for this length (reset, as in C++ stream_decompress)
            if length < MAX_LEN:
                dq = deque()
                dq.append(line)
                window[length] = dq

            if all(k in line for k in keywords):
                results.append(line)

        else:
            # Compressed (RLE-encoded XOR against a template in the window)
            window_id = read_int(EACH_WINDOW_SIZE_COUNT)
            len_single_data_bits = read_int(STREAM_ENCODER_COUNT)
            if len_single_data_bits <= 0:
                continue

            # Decode the RLE bitstream into xor_result bytes
            consumed = 0
            xor_bytes = []
            while consumed < len_single_data_bits:
                bit = read_bit()
                if bit is None:
                    break
                consumed += 1

                if bit == 1:
                    # Literal byte: next 8 bits are the byte value
                    val = 0
                    for j in range(8):
                        b = read_bit()
                        if b is None:
                            break
                        consumed += 1
                        if b:
                            val |= (1 << j)
                    xor_bytes.append(val & 0xFF)
                else:
                    # Run of zeros: next RLE_COUNT bits encode the run length
                    zero_count = 0
                    for j in range(RLE_COUNT):
                        b = read_bit()
                        if b is None:
                            break
                        consumed += 1
                        if b:
                            zero_count |= (1 << j)
                    if zero_count > 0:
                        xor_bytes.extend([0] * zero_count)

            if not xor_bytes:
                continue

            xor_result = bytearray(xor_bytes)
            length = len(xor_result)

            dq = window.get(length)
            if not dq or window_id >= len(dq):
                # In a valid stream this should not happen; bail out gracefully.
                continue

            pattern = dq[window_id]
            pattern_bytes = pattern.encode('utf-8', 'ignore')
            if len(pattern_bytes) < length:
                pattern_bytes = pattern_bytes.ljust(length, b'\0')

            # simdReplaceNullCharacters: replace '\0' with pattern character
            for i in range(length):
                if xor_result[i] == 0:
                    xor_result[i] = pattern_bytes[i]

            line_bytes = bytes(xor_result)
            try:
                line = line_bytes.decode('utf-8', 'ignore')
            except UnicodeDecodeError:
                line = line_bytes.decode('latin1', 'ignore')

            # Update window with this newly reconstructed line
            dq = window.setdefault(length, deque())
            if len(dq) < EACH_WINDOW_SIZE:
                dq.append(line)
            else:
                dq.popleft()
                dq.append(line)

            if all(k in line for k in keywords):
                results.append(line)

    return results