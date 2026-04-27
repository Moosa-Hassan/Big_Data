#ifndef XORC_STREAM_COMPRESS_COMPRESS_H_
#define XORC_STREAM_COMPRESS_COMPRESS_H_

// Core LogLite-B stream compressor / decompressor.
//
// For each log line, Stream_Compress decides whether to:
//   - store the line as-is (flag bit 0 + original length + raw bytes), or
//   - reference an existing template in the L-window (flag bit 1 + window
//     index + RLE-encoded XOR mask between the line and the template).
//
// The L-window (kept per line length) holds recent lines that are good
// candidates to reuse as templates. stream_compress updates this window
// on every line; stream_decompress consumes the same bit layout and
// rebuilds the plaintext while maintaining a mirrored window.

#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <boost/dynamic_bitset.hpp>
#include <deque>
#include <unordered_map>
#include <chrono>

#include "common/xor_string.h"
#include "common/rle.h"
#include "common/constants.h"

namespace XORC
{

    class Stream_Compress
    {
    private:
        // L-window: for each distinct line length we keep a deque of
        // templates. The static variant is append-only until it reaches
        // EACH_WINDOW_SIZE entries; after that, it does not evict or add
        // new entries. The index inside the deque is what we write into
        // the bitstream when we decide to compress a line relative to a
        // template.
        std::unordered_map<size_t, std::deque<std::string>> window;

    public:
        Stream_Compress();
        ~Stream_Compress();

        // Compress a single log line and append its bits to output_data.
        // This function is called once per line from xorc-cli.
        void stream_compress(const std::string &single_data, boost::dynamic_bitset<> &output_data, uint64_t &len_output_data);
        // Decode one record from the bitstream. single_data contains either
        // the RLE-encoded XOR mask (for isRLE == true) or the raw bytes
        // (for isRLE == false). original_length_or_window_id is interpreted
        // accordingly when updating / reading from the window.
        void stream_decompress(const boost::dynamic_bitset<> &single_data, const bool isRLE, const int window_id, std::string &output_data, std::string &xor_result);

        // getter added to get access to L - window component
        const std::unordered_map<size_t, std::deque<std::string>> &get_window() const { return window; }
    };

}

#endif