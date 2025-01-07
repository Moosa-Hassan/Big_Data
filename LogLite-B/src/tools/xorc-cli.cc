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

#include "common/file.h"
#include "compress/stream_compress.h"

static struct config
{
    bool stream_compress;
    bool stream_decompress;

    bool is_test;

    const char *file_path;
    const char *com_output_path;
    const char *decom_output_path;
} config;

static void parseOptions(int argc, const char **argv)
{
    // Default values
    config.stream_compress = false;
    config.stream_decompress = false;
    config.is_test = false;

    for (int i = 1; i < argc; i++)
    {
        int lastarg = (i == argc - 1);
        if (!strcmp(argv[i], "--compress") && !lastarg)
        {
            config.stream_compress = true;
        }
        else if (!strcmp(argv[i], "--decompress") && !lastarg)
        {
            config.stream_decompress = true;
        }
        else if (!strcmp(argv[i], "--test") && !lastarg)
        {
            config.is_test = true;
        }
        else if (!strcmp(argv[i], "--file-path") && !lastarg)
        {
            config.file_path = const_cast<char *>(argv[++i]);
        }
        else if (!strcmp(argv[i], "--com-output-path") && !lastarg)
        {
            config.com_output_path = const_cast<char *>(argv[++i]);
        }
        else if (!strcmp(argv[i], "--decom-output-path") && !lastarg)
        {
            config.decom_output_path = const_cast<char *>(argv[++i]);
        }
        else
        {
            std::cerr << "Unknown option: " << argv[i] << std::endl;
            exit(1);
        }
    }
}

std::streampos file_size(const char *filename)
{
    std::ifstream file(filename, std::ios::binary | std::ios::ate);
    if (!file)
    {
        throw std::runtime_error("Failed to open file for size calculation.");
    }
    return file.tellg();
}

bool areFilesEqual(const std::string &filePath1, const std::string &filePath2)
{
    std::ifstream file1(filePath1, std::ifstream::binary | std::ifstream::ate);
    std::ifstream file2(filePath2, std::ifstream::binary | std::ifstream::ate);

    if (!file1.is_open() || !file2.is_open())
    {
        std::cerr << "Error: Could not open one of the files." << std::endl;
        return false;
    }

    if (file1.tellg() != file2.tellg())
    {

        return false;
    }

    file1.seekg(0, std::ifstream::beg);
    file2.seekg(0, std::ifstream::beg);

    // Compare file contents
    std::istreambuf_iterator<char> begin1(file1), begin2(file2);
    std::istreambuf_iterator<char> end;
    return std::equal(begin1, end, begin2);
}

boost::dynamic_bitset<> merge_bitset_vector(const std::vector<boost::dynamic_bitset<>> &bitset_vector)
{

    std::size_t total_bits = 0;
    for (const auto &bitset : bitset_vector)
    {
        total_bits += bitset.size();
    }

    boost::dynamic_bitset<> merged_bitset(total_bits);

    std::size_t current_position = 0;

    for (const auto &bitset : bitset_vector)
    {

        for (std::size_t i = 0; i < bitset.size(); ++i)
        {
            merged_bitset[current_position + i] = bitset[i];
        }

        current_position += bitset.size();
    }

    return merged_bitset;
}

int main(int argc, const char *argv[])
{

    parseOptions(argc, argv);

    if (config.is_test)
    {
        std::cout << "==================Test mode==================" << std::endl;
    }

    if (config.stream_compress || config.is_test)
    {

        std::cout << "-----using stream compress-----" << std::endl;
        std::cout << "raw file path:" << config.file_path << std::endl;
        std::cout << "compressed output file path:" << config.com_output_path << std::endl;

        std::string all_data;
        XORC::read_string_from_file(all_data, config.file_path);

        std::vector<std::string> split_all_data;

        std::istringstream stream(all_data);
        std::string token;

        int line_count = 0;
        while (std::getline(stream, token, '\n'))
        {

            token.push_back('\n');

            split_all_data.push_back(token);
            ++line_count;
        }

        std::vector<boost::dynamic_bitset<>> bitset_vector;
        bitset_vector.reserve(line_count * 20);

        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        clock_t start_time, end_time;
        start_time = clock();
        for (size_t i = 0; i < split_all_data.size(); ++i)
        {
            sc->stream_compress(split_all_data[i], bitset_vector);
        }
        end_time = clock();

        boost::dynamic_bitset<> merged_bitset = merge_bitset_vector(bitset_vector);
        XORC::write_bitset_to_file(merged_bitset, config.com_output_path);

        int64_t raw_size = file_size(config.file_path);
        int64_t compressed_size = file_size(config.com_output_path);

        if (raw_size <= 0 || compressed_size < 0)
        {
            std::cerr << "Error: Invalid file size. Raw size: " << raw_size
                      << ", Compressed size: " << compressed_size << std::endl;
            return 0;
        }

        std::cout << "compression rate:"
                  << static_cast<double>(compressed_size) / static_cast<double>(raw_size)
                  << std::endl;

        std::cout << "compression speed: "
                  << (double)raw_size / (double)1024 / (double)1024 /
                         (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s" << std::endl;

        delete sc;
    }

    if (config.stream_decompress || config.is_test)
    {

        const char *temp;
        if (config.is_test)
        {
            temp = config.file_path;
            std::cout << "Testing Decompression..." << std::endl;
            config.file_path = config.com_output_path;
        }

        std::cout << "-----using stream decompress-----" << std::endl;
        std::cout << "compressed file path:" << config.file_path << std::endl;
        std::cout << "decompressed output file path:" << config.decom_output_path << std::endl;

        boost::dynamic_bitset<> compressed_bitset;
        XORC::read_bitset_from_file(compressed_bitset, config.file_path);

        std::vector<int> vec_len_header_bit;
        std::vector<boost::dynamic_bitset<>> split_compressed_bitset;
        std::vector<bool> isRLE;
        std::vector<int> original_length_or_window_id;
        size_t len_compressed_bitset = compressed_bitset.size();
        size_t i = 0;
        while (i < len_compressed_bitset)
        {
            if (compressed_bitset[i] == 0)
            {
                isRLE.push_back(false);
                i++;

                size_t tem_original_length = 0;
                for (size_t j = 0; j < ORIGINAL_LENGTH_COUNT; ++j, ++i)
                {
                    if (compressed_bitset[i])
                    {
                        tem_original_length |= (1 << j);
                    }
                }
                original_length_or_window_id.push_back(tem_original_length);

                boost::dynamic_bitset<> tem_bitset(tem_original_length * 8);
                for (size_t j = 0; j < tem_original_length * 8; j++)
                {
                    tem_bitset[j] = compressed_bitset[i + j];
                }
                i += tem_original_length * 8;

                vec_len_header_bit.push_back(-1);
                split_compressed_bitset.push_back(tem_bitset);
            }
            else
            {
                isRLE.push_back(true);
                i++;

                int tem_window_id = 0;
                for (size_t j = 0; j < EACH_WINDOW_SIZE_COUNT; ++j, ++i)
                {
                    if (compressed_bitset[i])
                    {
                        tem_window_id |= (1 << j);
                    }
                }
                original_length_or_window_id.push_back(tem_window_id);

                int len_last_bit = 0;
                for (size_t j = 0; j < 3; ++j, ++i)
                {
                    if (compressed_bitset[i])
                    {
                        len_last_bit |= (1 << j);
                    }
                }
                if (len_last_bit == 0)
                {
                    len_last_bit = 8;
                }

                int padding = 8 - (1 + EACH_WINDOW_SIZE_COUNT + 3) % 8;
                if (padding != 8)
                    i += (padding);

                int len_header_byte = 0;
                for (size_t j = 0; j < HEAD_BIT_LEN; ++j, ++i)
                {
                    if (compressed_bitset[i])

                    {
                        len_header_byte |= (1 << j);
                    }
                }

                int len_header_bit = len_header_byte * 8 - 8 + len_last_bit;

                boost::dynamic_bitset<> tem_bitset(len_header_bit + len_header_bit * 8);
                size_t len_tem_bitset = 0;

                for (int j = 0; j < len_header_bit; j++)
                {
                    tem_bitset[len_tem_bitset++] = compressed_bitset[i++];
                }

                i += 8 - len_last_bit;

                for (int j = 0; j < len_header_bit * 8; j++)
                {
                    tem_bitset[len_tem_bitset++] = compressed_bitset[i++];
                }

                vec_len_header_bit.push_back(len_header_bit);
                split_compressed_bitset.push_back(tem_bitset);
            }
        }

        std::string all_data;
        all_data.reserve(static_cast<size_t>(1024) * 1024 * 1024 * Reserved_Memory);

        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        std::string xor_result;
        xor_result.reserve(500);

        clock_t start_time, end_time;
        start_time = clock();
        for (size_t i = 0; i < split_compressed_bitset.size(); ++i)
        {
            sc->stream_decompress(split_compressed_bitset[i], vec_len_header_bit[i], isRLE[i], original_length_or_window_id[i], all_data, xor_result);
        }
        end_time = clock();

        XORC::write_string_to_file(all_data, config.decom_output_path);

        int64_t raw_size = static_cast<int64_t>(file_size(config.decom_output_path)) - 2 * split_compressed_bitset.size();
        std::cout << "decompression speed: "
                  << (double)raw_size / (double)1024 / (double)1024 /
                         (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s" << std::endl;

        delete sc;
    }
}
