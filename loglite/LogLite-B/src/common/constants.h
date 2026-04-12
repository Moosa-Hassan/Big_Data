// constants.h
// -----------
// Global configuration knobs and fixed constants that define the
// **bit-level format** and behavior of LogLite-B.
//
// These values control things like:
//   - How many bits are used to encode window IDs and RLE lengths.
//   - How many bits are used for raw line lengths.
//   - How big the L-window can be for each line length.
//   - How the run-length encoding of zero bytes is parameterized.
//
// The compressor (`Stream_Compress::stream_compress`) and the
// decompressor (`Stream_Compress::stream_decompress` and the parsing
// loop in `xorc-cli.cc`) must **agree** on these constants; changing
// them alters the on-disk format and breaks compatibility with
// previously compressed files.

#include <new>
#ifndef CONSTANTS_H_
#define CONSTANTS_H_

// Parameters that can be adjusted

// LogLite's L-window size is 2^EACH_WINDOW_SIZE_COUNT entries per
// length bucket. This is the number of templates kept per distinct
// line length.
constexpr int EACH_WINDOW_SIZE_COUNT = 3;  // k

// Similarity threshold used when deciding whether to compress a line
// against an existing template or fall back to raw storage.
// Default θ is 0.85, so we encode XOR+RLE when at least 85% of
// characters match.
constexpr double Similarity_Threshold = 1 - 0.85;  //default Sitha θ is 0.85

// STREAM_ENCODER_COUNT and ORIGINAL_LENGTH_COUNT define how many bits
// we dedicate to:
//   - length of the XOR+RLE payload (in bits)
//   - original line length (in bytes)
// Making these smaller can improve compression ratio, but if too
// small the decoder cannot reconstruct long lines.
constexpr int STREAM_ENCODER_COUNT = 13;
constexpr int ORIGINAL_LENGTH_COUNT = 15;

// // for large dataset, have to use the following.
// constexpr int STREAM_ENCODER_COUNT = 13 + 8;
// constexpr int ORIGINAL_LENGTH_COUNT = 15 + 8;

// If the log length is larger than this value, the log is not
// compressed with XOR+RLE. This can influence compression rate on
// datasets with very long lines.
constexpr int MAX_LEN = 10000;

// Reserved_Memory is the amount of memory (in GB) that some parts of
// the original implementation assume is available for buffers.
// Adjusting this changes how aggressively memory is reserved but does
// not affect the on-disk format.
constexpr double Reserved_Memory = 33;



//====================================//
// Parameters that should not be adjusted
// These are baked into the RLE and SIMD logic. Modifying them requires
// auditing and updating both compression and decompression code.
constexpr int RLE_COUNT = 8;                 // number of bits used to encode a zero-run length
constexpr int RLE_POW_COUNT = 1 << RLE_COUNT; // max zero-run length representable
constexpr int RLE_SKIM = 8;                  // number of bits per literal byte in RLE stream
constexpr int EACH_WINDOW_SIZE = 1 << EACH_WINDOW_SIZE_COUNT; // L-window size per length
constexpr size_t simd_width32 = 32;          // AVX2 vector width in bytes
constexpr size_t simd_width16 = 16;          // SSE vector width in bytes

#endif // CONSTANTS_H_