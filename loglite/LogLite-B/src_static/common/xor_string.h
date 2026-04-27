#ifndef XOR_STRING_H_
#define XOR_STRING_H_

// SIMD-accelerated helpers for XOR-ing two equal-length strings.
// In LogLite, we XOR the current log line with a template (from the L-window)
// to get a difference mask. Long runs of zeros in this mask mean the lines
// are very similar, and that mask is then compressed further with RLE.

#include <iostream>
#include <string>
#include <immintrin.h>
#include <stdexcept>
#include <chrono>

#include "common/constants.h"

namespace XORC
{
    // Return a new string whose bytes are the XOR of a and b.
    // a and b must be the same length; we use AVX2/SSE first and
    // then a scalar tail loop.
    std::string bitwiseXor(const std::string &a, const std::string &b);

    // XOR a and b into an already-allocated result buffer.
    // This avoids reallocations when we call it repeatedly in the
    // compressor while scanning many templates.
    void bitwiseXor(const std::string &a, const std::string &b, std::string &result);
}

#endif