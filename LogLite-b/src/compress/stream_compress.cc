#include "stream_compress.h"

namespace XORC
{
    // ── Timing accumulators ──
    long long time_lwindow               = 0;
    long long time_xorp                  = 0;
    long long time_rle                   = 0;
    long long time_decomp_rle            = 0;
    long long time_decomp_xor_reconstruct= 0;
    long long time_decomp_window_update  = 0;

    long long count_compressed       = 0;
    long long count_raw              = 0;
    long long count_skipped          = 0;
    long long count_emitted          = 0;
    long long count_postfilter_dropped = 0;

    using Clock = std::chrono::high_resolution_clock;

    void print_timing_report()
    {
        long long total_comp   = time_lwindow + time_xorp + time_rle;
        long long total_decomp = time_decomp_rle + time_decomp_xor_reconstruct + time_decomp_window_update;

        std::cout << "\n========== Component Timing Report ==========\n";
        std::cout << "Lines through XOR path : " << count_compressed << "\n";
        std::cout << "Lines stored raw       : " << count_raw        << "\n\n";

        std::cout << "-- Compression --\n";
        std::cout << "  L-Windows lookup : " << time_lwindow / 1000000.0 << " ms ("
                  << (total_comp ? 100.0 * time_lwindow / total_comp : 0) << "%)\n";
        std::cout << "  XOR-P            : " << time_xorp    / 1000000.0 << " ms ("
                  << (total_comp ? 100.0 * time_xorp    / total_comp : 0) << "%)\n";
        std::cout << "  RLE encode       : " << time_rle     / 1000000.0 << " ms ("
                  << (total_comp ? 100.0 * time_rle     / total_comp : 0) << "%)\n";
        std::cout << "  Total            : " << total_comp   / 1000000.0 << " ms\n\n";

        std::cout << "-- Decompression --\n";
        std::cout << "  RLE decode       : " << time_decomp_rle              / 1000000.0 << " ms ("
                  << (total_decomp ? 100.0 * time_decomp_rle              / total_decomp : 0) << "%)\n";
        std::cout << "  XOR reconstruct  : " << time_decomp_xor_reconstruct  / 1000000.0 << " ms ("
                  << (total_decomp ? 100.0 * time_decomp_xor_reconstruct  / total_decomp : 0) << "%)\n";
        std::cout << "  Window update    : " << time_decomp_window_update    / 1000000.0 << " ms ("
                  << (total_decomp ? 100.0 * time_decomp_window_update    / total_decomp : 0) << "%)\n";
        std::cout << "  Total            : " << total_decomp / 1000000.0 << " ms\n";
        if (count_positions_total > 0) {
            std::cout << "\n-- Stage 3 Field Extraction --\n";
            std::cout << "  Positions total  : " << count_positions_total << "\n";
            std::cout << "  Positions filled : " << count_positions_filled << " (" << 100.0 * count_positions_filled / count_positions_total << "%)\n";
            std::cout << "  Work avoided     : " << 100.0 * (1.0 - (double)count_positions_filled / count_positions_total) << "%\n";
        }
        std::cout << "=============================================\n\n";
    }

    static void integerToBitset(size_t value, boost::dynamic_bitset<> &output_data,
                                uint64_t &len_output_data, size_t bit_count = STREAM_ENCODER_COUNT)
    {
        for (size_t i = 0; i < bit_count; ++i)
            output_data[len_output_data++] = (value >> i) & 1;
    }

    Stream_Compress::Stream_Compress() {}
    Stream_Compress::~Stream_Compress() {}

    // ── Consensus template update ──
    void Stream_Compress::update_template(const std::string &log)
    {
        size_t len = log.size();
        auto it = template_map.find(len);

        if (it == template_map.end())
        {
            // First log of this length — initialize template
            template_map[len] = log;
            template_count[len] = 1;
        }
        else
        {
            // Update consensus: zero out positions that differ
            std::string &tmpl = it->second;
            for (size_t i = 0; i < len; ++i)
                if (tmpl[i] != '\0' && tmpl[i] != log[i])
                    tmpl[i] = '\0';
            template_count[len]++;
        }
    }

    // ── Template map save/load ──
    void Stream_Compress::save_template_map(const std::string &path) const
    {
        std::ofstream f(path, std::ios::binary);
        if (!f) { std::cerr << "Cannot write tmap: " << path << "\n"; return; }

        for (const auto &kv : template_map)
        {
            size_t len = kv.first;
            const std::string &tmpl = kv.second;
            size_t count = template_count.at(len);

            // Format per entry:
            // [8 bytes: length][8 bytes: log count][N bytes: template]
            f.write(reinterpret_cast<const char*>(&len),   sizeof(size_t));
            f.write(reinterpret_cast<const char*>(&count), sizeof(size_t));
            f.write(tmpl.data(), len);
        }

        std::cout << "Template map saved: " << path
                  << " (" << template_map.size() << " length buckets)\n";
    }

    void Stream_Compress::load_template_map(const std::string &path)
    {
        std::ifstream f(path, std::ios::binary);
        if (!f) { std::cerr << "Cannot read tmap: " << path << "\n"; return; }

        while (f.peek() != EOF)
        {
            size_t len = 0, count = 0;
            f.read(reinterpret_cast<char*>(&len),   sizeof(size_t));
            f.read(reinterpret_cast<char*>(&count), sizeof(size_t));
            if (!f) break;

            std::string tmpl(len, '\0');
            f.read(&tmpl[0], len);
            if (!f) break;

            template_map[len]   = tmpl;
            template_count[len] = count;
        }

        std::cout << "Template map loaded: " << path
                  << " (" << template_map.size() << " length buckets)\n";
    }

    std::set<size_t> Stream_Compress::lengths_matching_keyword(const std::string &keyword) const
    {
        std::set<size_t> result;
        for (const auto &kv : template_map)
        {
            // Search only in non-null positions of the consensus template
            const std::string &tmpl = kv.second;
            // Build a searchable string of static chars only
            // We do a sliding window search of keyword against static positions
            size_t klen = keyword.size();
            size_t tlen = tmpl.size();

            if (klen > tlen) continue;

            // Check if keyword could appear in the static parts
            // We use a simple approach: check if keyword appears as a substring
            // treating '\0' positions as wildcards (could match anything)
            bool found = false;
            for (size_t i = 0; i <= tlen - klen && !found; ++i)
            {
                bool match = true;
                for (size_t j = 0; j < klen && match; ++j)
                {
                    // '\0' in template = dynamic position, always matches
                    if (tmpl[i + j] != '\0' && tmpl[i + j] != keyword[j])
                        match = false;
                }
                if (match) found = true;
            }

            if (found) result.insert(kv.first);
        }
        return result;
    }

    const std::unordered_map<size_t, std::string>& Stream_Compress::get_template_map() const
    {
        return template_map;
    }

    // ── Compression ──
    void Stream_Compress::stream_compress(const std::string &single_data,
                                          boost::dynamic_bitset<> &output_data,
                                          uint64_t &len_output_data)
    {
        const size_t len_single_data = single_data.size();

        // Always update consensus template for every log
        update_template(single_data);

        if (this->window.find(len_single_data) != this->window.end())
        {
            std::string xor_result;
            xor_result.resize(len_single_data);

            float min_compress_rate = 3.0;
            int   min_index = -1;
            std::string min_xor_result;
            min_xor_result.reserve(len_single_data);

            auto t0 = Clock::now();

            for (int j = this->window[len_single_data].size() - 1; j >= 0; --j)
            {
                auto tx0 = Clock::now();
                XORC::bitwiseXor(single_data, this->window[len_single_data][j], xor_result);
                auto tx1 = Clock::now();
                time_xorp += std::chrono::duration_cast<std::chrono::nanoseconds>(tx1 - tx0).count();

                int count = 0;
                for (size_t i = 0; i < len_single_data; ++i)
                    if (xor_result[i] == '\0') ++count;

                float tem_rate = 1.0f - static_cast<float>(count) / len_single_data;

                if (tem_rate <= min_compress_rate)
                {
                    min_compress_rate = tem_rate;
                    min_index         = j;
                    min_xor_result    = xor_result;
                    if (min_compress_rate <= Similarity_Threshold) break;
                }
            }

            auto t1 = Clock::now();
            time_lwindow += std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

            output_data[len_output_data++] = 1;
            integerToBitset(min_index, output_data, len_output_data, EACH_WINDOW_SIZE_COUNT);

            size_t tem_index = len_output_data;
            len_output_data += STREAM_ENCODER_COUNT;

            auto tr0 = Clock::now();
            size_t len_xor_rle_bitset = XORC::runLengthEncodeString(
                min_xor_result, output_data, len_output_data, single_data);
            auto tr1 = Clock::now();
            time_rle += std::chrono::duration_cast<std::chrono::nanoseconds>(tr1 - tr0).count();

            for (size_t i = 0; i < STREAM_ENCODER_COUNT; ++i)
                output_data[tem_index + i] = (len_xor_rle_bitset >> i) & 1;

            if (this->window[len_single_data].size() < EACH_WINDOW_SIZE)
                this->window[len_single_data].push_back(single_data);
            else
            {
                this->window[len_single_data].pop_front();
                this->window[len_single_data].push_back(single_data);
            }
            ++count_compressed;
        }
        else if (len_single_data >= MAX_LEN || len_single_data == 0)
        {
            output_data[len_output_data++] = 0;
            integerToBitset(len_single_data, output_data, len_output_data, ORIGINAL_LENGTH_COUNT);
            for (size_t i = 0; i < len_single_data; i++)
                for (size_t j = 0; j < 8; ++j)
                    output_data[len_output_data++] = (single_data[i] >> j) & 1;
            ++count_raw;
        }
        else
        {
            std::deque<std::string> newDeque;
            newDeque.push_back(single_data);
            this->window[len_single_data] = newDeque;

            output_data[len_output_data++] = 0;
            integerToBitset(len_single_data, output_data, len_output_data, ORIGINAL_LENGTH_COUNT);
            for (size_t i = 0; i < len_single_data; i++)
                for (size_t j = 0; j < 8; ++j)
                    output_data[len_output_data++] = (single_data[i] >> j) & 1;
            ++count_raw;
        }
    }

    // ── Decompression helpers ──
    static std::string bitsetToString(const boost::dynamic_bitset<> &bitset)
    {
        std::string data;
        data.resize(bitset.size() / 8);
        for (size_t i = 0; i < data.size(); ++i)
        {
            char c = 0;
            for (size_t j = 0; j < 8; ++j)
                if (bitset[i * 8 + j]) c |= (1 << j);
            data[i] = c;
        }
        return data;
    }

    // ── Full decompression ──
    void Stream_Compress::stream_decompress(const boost::dynamic_bitset<> &single_data,
                                             const bool isRLE,
                                             const int original_length_or_window_id,
                                             std::string &output_data,
                                             std::string &xor_result)
    {
        xor_result.clear();
        if (isRLE)
        {
            auto tr0 = Clock::now();
            size_t len_single_data = single_data.size();
            size_t i = 0;
            int zero_count = 0;
            unsigned char byte = 0;

            while (i < len_single_data)
            {
                if (single_data[i])
                {
                    i++;
                    byte = 0;
                    for (size_t j = 0; j < 8; ++j)
                        byte |= (single_data[i + j]) << j;
                    i += RLE_SKIM;
                    xor_result.push_back(byte);
                }
                else
                {
                    i++;
                    zero_count = 0;
                    for (size_t j = 0; j < RLE_COUNT; ++j, ++i)
                        if (single_data[i]) zero_count |= (1 << j);
                    xor_result.append(zero_count, '\0');
                }
            }
            auto tr1 = Clock::now();
            time_decomp_rle += std::chrono::duration_cast<std::chrono::nanoseconds>(tr1 - tr0).count();

            auto tx0 = Clock::now();
            int len_xor_result = xor_result.size();
            std::string &pattern = this->window[len_xor_result][original_length_or_window_id];
            for (size_t i = 0; i < xor_result.size(); ++i)
                if (xor_result[i] == '\0') xor_result[i] = pattern[i];
            output_data += xor_result;
            output_data += "\n";
            auto tx1 = Clock::now();
            time_decomp_xor_reconstruct += std::chrono::duration_cast<std::chrono::nanoseconds>(tx1 - tx0).count();

            auto tw0 = Clock::now();
            if (this->window[len_xor_result].size() < EACH_WINDOW_SIZE)
                this->window[len_xor_result].push_back(xor_result);
            else
            {
                this->window[len_xor_result].pop_front();
                this->window[len_xor_result].push_back(xor_result);
            }
            auto tw1 = Clock::now();
            time_decomp_window_update += std::chrono::duration_cast<std::chrono::nanoseconds>(tw1 - tw0).count();
        }
        else
        {
            std::string tem = bitsetToString(single_data);
            output_data += tem;
            output_data += "\n";
            if (original_length_or_window_id < MAX_LEN)
            {
                std::deque<std::string> newDeque;
                newDeque.push_back(tem);
                this->window[original_length_or_window_id] = newDeque;
            }
        }
    }

    // ── Skip decompression with keyword post-filter ──
    void Stream_Compress::stream_decompress_skip(
        const boost::dynamic_bitset<> &single_data,
        const bool isRLE,
        const int original_length_or_window_id,
        std::string &output_data,
        std::string &xor_result,
        const std::set<size_t> &target_lengths,
        const std::string &keyword)
    {
        xor_result.clear();

        if (isRLE)
        {
            auto tr0 = Clock::now();
            size_t len_single_data = single_data.size();
            size_t i = 0;
            int zero_count = 0;
            unsigned char byte = 0;

            while (i < len_single_data)
            {
                if (single_data[i])
                {
                    i++;
                    byte = 0;
                    for (size_t j = 0; j < 8; ++j)
                        byte |= (single_data[i + j]) << j;
                    i += RLE_SKIM;
                    xor_result.push_back(byte);
                }
                else
                {
                    i++;
                    zero_count = 0;
                    for (size_t j = 0; j < RLE_COUNT; ++j, ++i)
                        if (single_data[i]) zero_count |= (1 << j);
                    xor_result.append(zero_count, '\0');
                }
            }
            auto tr1 = Clock::now();
            time_decomp_rle += std::chrono::duration_cast<std::chrono::nanoseconds>(tr1 - tr0).count();

            int len_xor_result = xor_result.size();
            std::string &pattern = this->window[len_xor_result][original_length_or_window_id];

            bool relevant = target_lengths.empty() ||
                            target_lengths.count((size_t)len_xor_result) > 0;

            if (relevant)
            {
                // Full reconstruct
                auto tx0 = Clock::now();
                for (size_t i = 0; i < xor_result.size(); ++i)
                    if (xor_result[i] == '\0') xor_result[i] = pattern[i];
                auto tx1 = Clock::now();
                time_decomp_xor_reconstruct += std::chrono::duration_cast<std::chrono::nanoseconds>(tx1 - tx0).count();

                // Post-filter: keyword check on fully reconstructed line
                if (keyword.empty() || xor_result.find(keyword) != std::string::npos)
                {
                    output_data += xor_result;
                    output_data += "\n";
                    ++count_emitted;
                }
                else
                {
                    ++count_postfilter_dropped;
                }
            }
            else
            {
                // Reconstruct for window correctness but don't emit
                for (size_t i = 0; i < xor_result.size(); ++i)
                    if (xor_result[i] == '\0') xor_result[i] = pattern[i];
                ++count_skipped;
            }

            auto tw0 = Clock::now();
            if (this->window[len_xor_result].size() < EACH_WINDOW_SIZE)
                this->window[len_xor_result].push_back(xor_result);
            else
            {
                this->window[len_xor_result].pop_front();
                this->window[len_xor_result].push_back(xor_result);
            }
            auto tw1 = Clock::now();
            time_decomp_window_update += std::chrono::duration_cast<std::chrono::nanoseconds>(tw1 - tw0).count();
        }
        else
        {
            size_t raw_length = single_data.size() / 8;
            std::string tem;
            tem.resize(raw_length);
            for (size_t i = 0; i < raw_length; ++i)
            {
                char c = 0;
                for (size_t j = 0; j < 8; ++j)
                    if (single_data[i * 8 + j]) c |= (1 << j);
                tem[i] = c;
            }

            bool relevant = target_lengths.empty() ||
                            target_lengths.count(raw_length) > 0;

            if (relevant)
            {
                if (keyword.empty() || tem.find(keyword) != std::string::npos)
                {
                    output_data += tem;
                    output_data += "\n";
                    ++count_emitted;
                }
                else
                {
                    ++count_postfilter_dropped;
                }
            }
            else
            {
                ++count_skipped;
            }

            if (original_length_or_window_id < MAX_LEN)
            {
                std::deque<std::string> newDeque;
                newDeque.push_back(tem);
                this->window[original_length_or_window_id] = newDeque;
            }
        }
    }
}

namespace XORC
{
    long long count_positions_total  = 0;
    long long count_positions_filled = 0;

    // ── Stage 3: Sniper Mode — partial field extraction ──
    void Stream_Compress::stream_decompress_fields(
        const boost::dynamic_bitset<> &single_data,
        const bool isRLE,
        const int original_length_or_window_id,
        std::string &output_data,
        std::string &xor_result,
        const std::set<size_t> &target_lengths,
        const std::string &keyword,
        const std::vector<FieldRange> &fields)
    {
        xor_result.clear();

        if (isRLE)
        {
            // ── RLE decode (always needed to walk bitstream) ──
            auto tr0 = Clock::now();
            size_t len_single_data = single_data.size();
            size_t i = 0;
            int zero_count = 0;
            unsigned char byte = 0;

            while (i < len_single_data)
            {
                if (single_data[i])
                {
                    i++;
                    byte = 0;
                    for (size_t j = 0; j < 8; ++j)
                        byte |= (single_data[i + j]) << j;
                    i += RLE_SKIM;
                    xor_result.push_back(byte);
                }
                else
                {
                    i++;
                    zero_count = 0;
                    for (size_t j = 0; j < RLE_COUNT; ++j, ++i)
                        if (single_data[i]) zero_count |= (1 << j);
                    xor_result.append(zero_count, '\0');
                }
            }
            auto tr1 = Clock::now();
            time_decomp_rle += std::chrono::duration_cast<std::chrono::nanoseconds>(tr1 - tr0).count();

            int len_xor_result = xor_result.size();
            count_positions_total += len_xor_result;

            // ── Bucket filter ──
            bool relevant = target_lengths.empty() ||
                            target_lengths.count((size_t)len_xor_result) > 0;

            if (!relevant)
            {
                // Still need window update for correctness
                std::string &pattern = this->window[len_xor_result][original_length_or_window_id];
                for (size_t i = 0; i < xor_result.size(); ++i)
                    if (xor_result[i] == '\0') xor_result[i] = pattern[i];

                auto tw0 = Clock::now();
                if (this->window[len_xor_result].size() < EACH_WINDOW_SIZE)
                    this->window[len_xor_result].push_back(xor_result);
                else
                {
                    this->window[len_xor_result].pop_front();
                    this->window[len_xor_result].push_back(xor_result);
                }
                auto tw1 = Clock::now();
                time_decomp_window_update += std::chrono::duration_cast<std::chrono::nanoseconds>(tw1 - tw0).count();
                ++count_skipped;
                return;
            }

            std::string &pattern = this->window[len_xor_result][original_length_or_window_id];

            // ── Partial null-fill: only fill positions within field ranges ──
            auto tx0 = Clock::now();

            // Build a set of positions we need — union of all field ranges
            // clamped to actual line length
            for (const auto &fr : fields)
            {
                size_t s = fr.start < (size_t)len_xor_result ? fr.start : (size_t)len_xor_result;
                size_t e = fr.end   < (size_t)len_xor_result ? fr.end   : (size_t)len_xor_result;
                for (size_t p = s; p < e; ++p)
                {
                    if (xor_result[p] == '\0')
                    {
                        xor_result[p] = pattern[p];
                        ++count_positions_filled;
                    }
                }
            }

            auto tx1 = Clock::now();
            time_decomp_xor_reconstruct += std::chrono::duration_cast<std::chrono::nanoseconds>(tx1 - tx0).count();

            // ── Keyword post-filter ──
            // For keyword check we need the full line — fill remaining nulls
            // but only if keyword is set
            std::string full_line;
            if (!keyword.empty())
            {
                full_line = xor_result;
                for (size_t i = 0; i < full_line.size(); ++i)
                    if (full_line[i] == '\0') full_line[i] = pattern[i];

                if (full_line.find(keyword) == std::string::npos)
                {
                    // Reconstruct for window but don't emit
                    xor_result = full_line;
                    auto tw0 = Clock::now();
                    if (this->window[len_xor_result].size() < EACH_WINDOW_SIZE)
                        this->window[len_xor_result].push_back(xor_result);
                    else
                    {
                        this->window[len_xor_result].pop_front();
                        this->window[len_xor_result].push_back(xor_result);
                    }
                    auto tw1 = Clock::now();
                    time_decomp_window_update += std::chrono::duration_cast<std::chrono::nanoseconds>(tw1 - tw0).count();
                    ++count_postfilter_dropped;
                    return;
                }
                xor_result = full_line;
            }
            else
            {
                // No keyword — fill remaining nulls for window correctness
                for (size_t i = 0; i < xor_result.size(); ++i)
                    if (xor_result[i] == '\0') xor_result[i] = pattern[i];
            }

            // ── Extract and emit only requested fields ──
            bool first_field = true;
            for (const auto &fr : fields)
            {
                size_t s = fr.start < (size_t)len_xor_result ? fr.start : (size_t)len_xor_result;
                size_t e = fr.end   < (size_t)len_xor_result ? fr.end   : (size_t)len_xor_result;
                if (s >= e) continue;
                if (!first_field) output_data += "\t";
                output_data += xor_result.substr(s, e - s);
                first_field = false;
            }
            output_data += "\n";
            ++count_emitted;

            // ── Window update ──
            auto tw0 = Clock::now();
            if (this->window[len_xor_result].size() < EACH_WINDOW_SIZE)
                this->window[len_xor_result].push_back(xor_result);
            else
            {
                this->window[len_xor_result].pop_front();
                this->window[len_xor_result].push_back(xor_result);
            }
            auto tw1 = Clock::now();
            time_decomp_window_update += std::chrono::duration_cast<std::chrono::nanoseconds>(tw1 - tw0).count();
        }
        else
        {
            // Raw line
            size_t raw_length = single_data.size() / 8;
            std::string tem;
            tem.resize(raw_length);
            for (size_t i = 0; i < raw_length; ++i)
            {
                char c = 0;
                for (size_t j = 0; j < 8; ++j)
                    if (single_data[i * 8 + j]) c |= (1 << j);
                tem[i] = c;
            }

            count_positions_total += raw_length;

            bool relevant = target_lengths.empty() ||
                            target_lengths.count(raw_length) > 0;

            if (relevant)
            {
                bool kw_ok = keyword.empty() ||
                             tem.find(keyword) != std::string::npos;
                if (kw_ok)
                {
                    bool first_field = true;
                    for (const auto &fr : fields)
                    {
                        size_t s = fr.start < raw_length ? fr.start : raw_length;
                        size_t e = fr.end   < raw_length ? fr.end   : raw_length;
                        if (s >= e) continue;
                        if (!first_field) output_data += "\t";
                        output_data += tem.substr(s, e - s);
                        first_field = false;
                    }
                    output_data += "\n";
                    ++count_emitted;
                    count_positions_filled += raw_length;
                }
                else
                    ++count_postfilter_dropped;
            }
            else
                ++count_skipped;

            if (original_length_or_window_id < MAX_LEN)
            {
                std::deque<std::string> newDeque;
                newDeque.push_back(tem);
                this->window[original_length_or_window_id] = newDeque;
            }
        }
    }
}
