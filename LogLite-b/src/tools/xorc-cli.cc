#include <ctime>
#include <iostream>
#include <vector>
#include <string.h>
#include <boost/dynamic_bitset.hpp>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <filesystem>
#include <set>

#include "common/file.h"
#include "compress/stream_compress.h"

static struct config
{
    bool stream_compress;
    bool stream_decompress;
    bool is_test;
    int  filter_length;
    const char *keyword;
    const char *file_path;
    const char *com_output_path;
    const char *decom_output_path;
    std::vector<XORC::FieldRange> fields;
} config;

static void parseOptions(int argc, const char **argv)
{
    config.stream_compress   = false;
    config.stream_decompress = false;
    config.is_test           = false;
    config.filter_length     = -1;
    config.keyword           = nullptr;

    for (int i = 1; i < argc; i++)
    {
        if      (!strcmp(argv[i], "--compress")          && i+1 < argc) config.stream_compress = true;
        else if (!strcmp(argv[i], "--decompress")        && i+1 < argc) config.stream_decompress = true;
        else if (!strcmp(argv[i], "--test")              && i+1 < argc) config.is_test = true;
        else if (!strcmp(argv[i], "--compress"))  config.stream_compress = true;
        else if (!strcmp(argv[i], "--decompress")) config.stream_decompress = true;
        else if (!strcmp(argv[i], "--test"))      config.is_test = true;
        else if (!strcmp(argv[i], "--filter-length") && i+1 < argc)
            config.filter_length = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--keyword") && i+1 < argc)
            config.keyword = argv[++i];
        else if (!strcmp(argv[i], "--file-path") && i+1 < argc)
            config.file_path = argv[++i];
        else if (!strcmp(argv[i], "--com-output-path") && i+1 < argc)
            config.com_output_path = argv[++i];
        else if (!strcmp(argv[i], "--decom-output-path") && i+1 < argc)
            config.decom_output_path = argv[++i];
        else if (!strcmp(argv[i], "--field") && i+2 < argc)
        {
            XORC::FieldRange fr;
            fr.start = atoi(argv[++i]);
            fr.end   = atoi(argv[++i]);
            config.fields.push_back(fr);
        }
        else if (argv[i][0] == '-')
        { std::cerr << "Unknown option: " << argv[i] << "\n"; exit(1); }
    }
}

std::streampos file_size(const char *filename)
{
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file) throw std::runtime_error("Failed to open file for size calculation.");
    return file.tellg();
}

int main(int argc, const char *argv[])
{
    parseOptions(argc, argv);

    if (config.is_test)
        std::cout << "==================Test mode==================\n";

    // ── COMPRESSION ──
    if (config.stream_compress || config.is_test)
    {
        std::cout << "-----using stream compress-----\n";
        std::cout << "raw file path:" << config.file_path << "\n";
        std::cout << "compressed output file path:" << config.com_output_path << "\n";

        std::string all_data;
        XORC::read_string_from_file(all_data, config.file_path);

        boost::dynamic_bitset<> output_data(all_data.size() * 8);
        uint64_t len_output_data = 0;

        std::vector<std::string> split_all_data;
        std::istringstream stream(all_data);
        std::string token;
        int line_count = 0;

        while (std::getline(stream, token, '\n'))
        {
            if (!token.empty() && token.back() == '\r') token.pop_back();
            split_all_data.push_back(token);
            ++line_count;
        }

        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        clock_t start_time = clock();
        for (size_t i = 0; i < split_all_data.size(); ++i)
            sc->stream_compress(split_all_data[i], output_data, len_output_data);
        clock_t end_time = clock();

        output_data.resize(len_output_data);
        XORC::write_bitset_to_file(output_data, config.com_output_path);

        std::string tmap_path = std::string(config.com_output_path) + ".tmap";
        sc->save_template_map(tmap_path);

        std::cout << "line_count:" << line_count << "\n";
        int64_t raw_size1        = (all_data.size() - 1 * line_count) * 8;
        int64_t compressed_size1 = len_output_data - line_count * STREAM_ENCODER_COUNT;
        std::cout << "compression rate:"
                  << static_cast<double>(compressed_size1) / static_cast<double>(raw_size1) << "\n";
        std::cout << "compression speed: "
                  << (double)raw_size1 / 8 / 1024 / 1024 /
                     (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s\n";
        delete sc;
    }

    // ── DECOMPRESSION ──
    if (config.stream_decompress || config.is_test)
    {
        if (config.is_test)
        {
            std::cout << "Testing Decompression...\n";
            config.file_path = config.com_output_path;
        }

        std::cout << "-----using stream decompress-----\n";
        std::cout << "compressed file path:" << config.file_path << "\n";
        std::cout << "decompressed output file path:" << config.decom_output_path << "\n";

        std::set<size_t> target_lengths;
        std::string keyword_str = config.keyword ? std::string(config.keyword) : "";
        bool use_fields = !config.fields.empty();
        bool use_skip   = (config.keyword != nullptr || config.filter_length > 0 || use_fields);

        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        if (use_skip)
        {
            std::string tmap_path = std::string(config.file_path) + ".tmap";
            sc->load_template_map(tmap_path);

            if (config.keyword != nullptr)
            {
                target_lengths = sc->lengths_matching_keyword(keyword_str);
                std::cout << "Keyword \"" << config.keyword << "\" matches "
                          << target_lengths.size() << " length bucket(s)\n";
            }

            if (config.filter_length > 0)
            {
                if (config.keyword != nullptr)
                {
                    if (target_lengths.count((size_t)config.filter_length))
                        target_lengths = {(size_t)config.filter_length};
                    else
                    {
                        std::cout << "Warning: filter-length " << config.filter_length
                                  << " not in keyword matches.\n";
                        target_lengths.clear();
                    }
                }
                else target_lengths = {(size_t)config.filter_length};
            }

            if (use_fields)
            {
                std::cout << "Field extraction mode: " << config.fields.size() << " field(s)\n";
                for (const auto &fr : config.fields)
                    std::cout << "  [" << fr.start << ":" << fr.end << "]\n";
            }
        }

        // ── Parse compressed bitstream ──
        boost::dynamic_bitset<> compressed_bitset;
        XORC::read_bitset_from_file(compressed_bitset, config.file_path);

        std::vector<boost::dynamic_bitset<>> split_compressed_bitset;
        std::vector<bool> isRLE;
        std::vector<int>  original_length_or_window_id;

        size_t len_compressed_bitset = compressed_bitset.size();
        size_t i = 0;

        while (i < len_compressed_bitset)
        {
            if (compressed_bitset[i] == 0)
            {
                isRLE.push_back(false);
                i++;
                int tem_original_length = 0;
                for (size_t j = 0; j < ORIGINAL_LENGTH_COUNT; ++j, ++i)
                    if (compressed_bitset[i]) tem_original_length |= (1 << j);
                original_length_or_window_id.push_back(tem_original_length);

                boost::dynamic_bitset<> tem_bitset(tem_original_length * 8);
                for (size_t j = 0; j < tem_original_length * 8; j++)
                    tem_bitset[j] = compressed_bitset[i + j];
                i += tem_original_length * 8;
                split_compressed_bitset.push_back(tem_bitset);
            }
            else
            {
                isRLE.push_back(true);
                i++;
                int tem_window_id = 0;
                for (size_t j = 0; j < EACH_WINDOW_SIZE_COUNT; ++j, ++i)
                    if (compressed_bitset[i]) tem_window_id |= (1 << j);
                original_length_or_window_id.push_back(tem_window_id);

                int len_single_data = 0;
                for (size_t j = 0; j < STREAM_ENCODER_COUNT; ++j, ++i)
                    if (compressed_bitset[i]) len_single_data |= (1 << j);

                boost::dynamic_bitset<> tem_bitset(len_single_data);
                for (size_t j = 0; j < len_single_data; j++)
                    tem_bitset[j] = compressed_bitset[i + j];
                i += len_single_data;
                split_compressed_bitset.push_back(tem_bitset);
            }
        }

        std::string all_data;
        all_data.reserve(static_cast<size_t>(1024) * 1024 * 1024 * 33);
        std::string xor_result;
        xor_result.reserve(500);

        clock_t start_time = clock();

        if (use_fields)
        {
            for (size_t i = 0; i < split_compressed_bitset.size(); ++i)
                sc->stream_decompress_fields(split_compressed_bitset[i], isRLE[i],
                                             original_length_or_window_id[i],
                                             all_data, xor_result,
                                             target_lengths, keyword_str,
                                             config.fields);
        }
        else if (use_skip)
        {
            for (size_t i = 0; i < split_compressed_bitset.size(); ++i)
                sc->stream_decompress_skip(split_compressed_bitset[i], isRLE[i],
                                           original_length_or_window_id[i],
                                           all_data, xor_result,
                                           target_lengths, keyword_str);
        }
        else
        {
            for (size_t i = 0; i < split_compressed_bitset.size(); ++i)
                sc->stream_decompress(split_compressed_bitset[i], isRLE[i],
                                      original_length_or_window_id[i],
                                      all_data, xor_result);
        }

        clock_t end_time = clock();

        XORC::write_string_to_file(all_data, config.decom_output_path);

        int64_t raw_size = static_cast<int64_t>(file_size(config.decom_output_path))
                           - 2 * split_compressed_bitset.size();
        std::cout << "decompression speed: "
                  << (double)raw_size / 1024 / 1024 /
                     (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s\n";

        if (use_skip || use_fields)
        {
            std::cout << "Lines emitted          : " << XORC::count_emitted           << "\n";
            std::cout << "Lines skipped (bucket) : " << XORC::count_skipped           << "\n";
            std::cout << "Lines dropped (post)   : " << XORC::count_postfilter_dropped << "\n";
        }

        delete sc;
    }

    XORC::print_timing_report();
    return 0;
}
