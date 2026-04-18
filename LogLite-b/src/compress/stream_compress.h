#ifndef XORC_STREAM_COMPRESS_COMPRESS_H_
#define XORC_STREAM_COMPRESS_COMPRESS_H_

#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <boost/dynamic_bitset.hpp>
#include <deque>
#include <unordered_map>
#include <set>
#include <chrono>

#include "common/xor_string.h"
#include "common/rle.h"
#include "common/constants.h"

namespace XORC
{
    // Global timing accumulators (nanoseconds)
    extern long long time_lwindow;
    extern long long time_xorp;
    extern long long time_rle;
    extern long long time_decomp_rle;
    extern long long time_decomp_xor_reconstruct;
    extern long long time_decomp_window_update;

    // Counters
    extern long long count_compressed;
    extern long long count_raw;
    extern long long count_skipped;
    extern long long count_emitted;
    extern long long count_postfilter_dropped;
    extern long long count_positions_total;   // total char positions across all processed lines
    extern long long count_positions_filled;  // positions actually null-filled (Stage 3)

    void print_timing_report();

    // Field range: 0-indexed, start inclusive, end exclusive
    struct FieldRange {
        size_t start;
        size_t end;
    };

    class Stream_Compress
    {
    private:
        std::unordered_map<size_t, std::deque<std::string>> window;
        std::unordered_map<size_t, std::string> template_map;
        std::unordered_map<size_t, size_t> template_count;

        void update_template(const std::string &log);

    public:
        Stream_Compress();
        ~Stream_Compress();

        void stream_compress(const std::string &single_data,
                             boost::dynamic_bitset<> &output_data,
                             uint64_t &len_output_data);

        // Full decompress
        void stream_decompress(const boost::dynamic_bitset<> &single_data,
                               const bool isRLE,
                               const int window_id,
                               std::string &output_data,
                               std::string &xor_result);

        // Stage 2: skip by length bucket + keyword post-filter
        void stream_decompress_skip(const boost::dynamic_bitset<> &single_data,
                                    const bool isRLE,
                                    const int original_length_or_window_id,
                                    std::string &output_data,
                                    std::string &xor_result,
                                    const std::set<size_t> &target_lengths,
                                    const std::string &keyword);

        // Stage 3: sniper mode — partial field extraction
        void stream_decompress_fields(const boost::dynamic_bitset<> &single_data,
                                      const bool isRLE,
                                      const int original_length_or_window_id,
                                      std::string &output_data,
                                      std::string &xor_result,
                                      const std::set<size_t> &target_lengths,
                                      const std::string &keyword,
                                      const std::vector<FieldRange> &fields);

        void save_template_map(const std::string &path) const;
        void load_template_map(const std::string &path);
        std::set<size_t> lengths_matching_keyword(const std::string &keyword) const;
        const std::unordered_map<size_t, std::string>& get_template_map() const;
    };
}

#endif
