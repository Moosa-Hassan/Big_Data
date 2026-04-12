#ifndef RLE_H_
#define RLE_H_

// Run-length encoder for the XOR mask produced by comparing a line
// with a template. Long runs of zero bytes in the XOR result mean the
// two strings are identical over that region, which we encode as a
// (flag + run length) instead of storing each zero.

#include <iostream>
#include <string>
#include <boost/dynamic_bitset.hpp>
#include <vector>
#include <immintrin.h>

#include "common/constants.h"

namespace XORC
{
    // Encode the XOR result "input" into bits and append to output_data.
    // The format is:
    //   1 bit  : tag (1 = literal byte, 0 = zero run)
    //   if tag = 1: next 8 bits  are the literal byte value from original_data
    //   if tag = 0: next RLE_COUNT bits store the run length of '\0' bytes
    // It returns the number of bits written for this encoded XOR mask.
    size_t runLengthEncodeString(const std::string &input,
                                 boost::dynamic_bitset<> &output_data,
                                 uint64_t &len_output_data,
                                 const std::string &single_data);

}

#endif