// xorc-cli.cc
// -------------
// This file implements the **command-line front-end** for LogLite-B.
//
// It does **not** implement the compression algorithm itself; instead it:
//   - Parses command-line flags (compress / decompress / test).
//   - Loads the raw log file into memory.
//   - Calls `XORC::Stream_Compress::stream_compress` once per line to
//     build a single global bitstream in a `boost::dynamic_bitset`.
//   - Writes that bitstream to disk via `write_bitset_to_file`.
//   - For decompression, reads the bitstream back, splits it into
//     per-line chunks, and calls `Stream_Compress::stream_decompress`
//     to reconstruct the original text.
//
// In other words: this file wires together I/O + timing + the
// **per-line** compressor/decompressor implemented in
// `compress/stream_compress.{h,cc}`.

#include <ctime>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <boost/dynamic_bitset.hpp>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <filesystem>

#include "common/file.h"              // read/write compressed bitsets and plain strings
#include "compress/stream_compress.h" // Stream_Compress: core LogLite per-line codec

// Simple global configuration object filled by `parseOptions` and used in `main`.
//
// Important fields:
//   - stream_compress / stream_decompress / is_test
//       control which phases run.
//   - file_path
//       path to the raw input log for compression, or compressed file for
//       decompression (in test mode this is reassigned).
//   - com_output_path
//       where the compressed `.lite` bitstream is written.
//   - decom_output_path
//       where the fully decompressed text is written when we run
//       stream decompression.
//   - window_output_path
//       optional path where we dump the internal L-window (templates
//       used by the compressor) after compression.
static struct config
{
    bool stream_compress;
    bool stream_decompress;
    bool is_test;

    const char *file_path;
    const char *com_output_path;
    const char *decom_output_path;

    const char *window_output_path;

} config;

// parseOptions
// -------------
// Parses the `xorc-cli` command-line arguments and populates the
// global `config` structure. This function does not do any I/O or
// compression; it only sets flags and paths that `main` will use.
//
// Expected options (used elsewhere in this file):
//   --compress             : enable streaming compression
//   --decompress           : enable streaming decompression
//   --test                 : run both compression and decompression
//                            in one invocation
//   --file-path <path>     : input file (raw log for compress, or
//                            compressed file in test/decompress mode)
//   --com-output-path <p>  : compressed bitstream output path
//   --decom-output-path <p>: decompressed text output path
//   --window-output-path <p>: optional dump of the final L-window
static void parseOptions(int argc, const char **argv)
{
    // Default values
    config.stream_compress = false;
    config.stream_decompress = false;
    config.is_test = false;
    config.window_output_path = nullptr;

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
        else if (!strcmp(argv[i], "--window-output-path") && !lastarg)
        {
            config.window_output_path = const_cast<char *>(argv[++i]);
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

    std::istreambuf_iterator<char> begin1(file1), begin2(file2);
    std::istreambuf_iterator<char> end;
    return std::equal(begin1, end, begin2);
}

// Insert ".static" before the last filename extension so static builds
// write distinct artifacts (for example: x.lite.b -> x.lite.static.b,
// x.window.txt -> x.window.static.txt).
static std::string appendStaticInfix(const char *path)
{
    if (path == nullptr)
    {
        return "";
    }

    std::string p(path);
    if (p.find(".static") != std::string::npos)
    {
        return p;
    }

    size_t slash_pos = p.find_last_of("/\\");
    size_t dot_pos = p.find_last_of('.');
    if (dot_pos == std::string::npos || (slash_pos != std::string::npos && dot_pos < slash_pos))
    {
        return p + ".static";
    }

    return p.substr(0, dot_pos) + ".static" + p.substr(dot_pos);
}

int main(int argc, const char *argv[])
{
    // Step 1: parse command-line options into `config`.
    parseOptions(argc, argv);

    const std::string static_com_output_path = appendStaticInfix(config.com_output_path);
    const std::string static_window_output_path = appendStaticInfix(config.window_output_path);

    if (config.is_test)
    {
        std::cout << "==================Test mode==================" << std::endl;
    }

    // ==========================
    //  Compression (stream mode)
    // ==========================
    //
    // Enabled when either:
    //   - user passes `--compress`, or
    //   - user passes `--test` (run compress + decompress back-to-back).
    //
    // High level flow under the hood:
    //   1) Read the entire raw log into `all_data`.
    //   2) Split into lines and store them in `split_all_data`.
    //   3) For each line, call `Stream_Compress::stream_compress`.
    //      That function:
    //        - looks up a similar template in the L-window,
    //        - encodes either raw or XOR+RLE, and
    //        - appends bits into `output_data`.
    //   4) After all lines, trim `output_data` to the used bit length
    //      and write it to disk as a `.lite` file.
    if (config.stream_compress || config.is_test)
    {

        std::cout << "-----using stream compress-----" << std::endl;
        std::cout << "raw file path:" << config.file_path << std::endl;
        std::cout << "compressed output file path:" << static_com_output_path << std::endl;

        std::string all_data;
        // Load the entire raw log file into memory as one big string.
        // This is the input stream that we will split into individual
        // log lines and feed into `Stream_Compress::stream_compress`.
        XORC::read_string_from_file(all_data, config.file_path);

        // `split_all_data` holds the log file as **one string per line**.
        // `Stream_Compress` operates at this granularity: each call
        // compresses exactly one line.
        std::vector<std::string> split_all_data;
        std::istringstream stream(all_data);
        std::string token;

        int line_count = 0;
        // Split `all_data` into lines on '\n'. Any trailing '\r'
        // (Windows-style CRLF) is stripped so the compressor always
        // sees pure log content.
        while (std::getline(stream, token, '\n'))
        {
            if (!token.empty() && token.back() == '\r')
            {
                token.pop_back();
            }
            split_all_data.push_back(token);
            ++line_count;
        }

        // `output_data` holds the **global compressed bitstream**.
        // We over-allocate and include 64 metadata bits per line for
        // the hashed word bitmap.
        boost::dynamic_bitset<> output_data(all_data.size() * 8 + static_cast<size_t>(line_count) * 96);
        uint64_t len_output_data = 0;

        // Create the LogLite per-line compressor. Internally this
        // object maintains the L-window (templates) and exposes
        // `stream_compress` / `stream_decompress`.
        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        clock_t start_time, end_time;
        start_time = clock();
        // For each raw line, call into LogLite's **core compressor**.
        // `stream_compress` decides, per line, whether to:
        //   - store it raw (flag=0 + length + bytes), or
        //   - encode it as XOR+RLE against a template from the window
        //     (flag=1 + window id + RLE-encoded mask).
        for (size_t i = 0; i < split_all_data.size(); ++i)
        {
            sc->stream_compress(split_all_data[i], output_data, len_output_data);
        }
        end_time = clock();

        // Trim the global bitset down to the number of bits actually
        // produced, then persist it using the `file.cc` helper. On
        // disk, this is stored as blocks of unsigned long + a trailing
        // `last_block_bits` field.
        output_data.resize(len_output_data);
        XORC::write_bitset_to_file(output_data, static_com_output_path.c_str());

        std::cout << "line_count:"
                  << line_count
                  << std::endl;

        // Rough compression-ratio accounting in **bits**:
        //   raw_size1         ~ size of original payload bits
        //   compressed_size1  ~ size of encoded bits minus per-line
        //                        length bookkeeping
        int64_t raw_size1 = (all_data.size() - 1 * line_count) * 8;
        int64_t compressed_size1 = len_output_data - line_count * STREAM_ENCODER_COUNT;
        std::cout << "compression rate:"
                  << static_cast<double>(compressed_size1) / static_cast<double>(raw_size1)
                  << std::endl;

        std::cout << "compression speed: "
                  << (double)raw_size1 / 8 / (double)1024 / (double)1024 /
                         (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s" << std::endl;

        // Optional: dump the **final L-window** used by the compressor.
        // This is useful for analysis and for building compressed-
        // domain query engines, since it exposes, per length, the set
        // of templates that future lines reference.
        if (config.window_output_path)
        {
            std::ofstream ofs(static_window_output_path);
            if (ofs)
            {
                const auto &window = sc->get_window();
                for (const auto &entry : window)
                {
                    ofs << "len=" << entry.first << '\n';
                    for (const auto &tmpl : entry.second)
                    {
                        ofs << tmpl << '\n';
                    }
                    ofs << "---" << '\n';
                }
            }
        }

        delete sc;
    }

    // ============================
    //  Decompression (stream mode)
    // ============================
    //
    // Enabled when either:
    //   - user passes `--decompress`, or
    //   - user passes `--test` (after the compression phase completes).
    //
    // High level flow under the hood:
    //   1) Read the compressed `.lite` file into a global bitset.
    //   2) Iterate bit-by-bit, reconstructing **per-line** bitsets
    //      and their metadata (raw vs compressed, length/window id).
    //   3) For each per-line bitset, call `Stream_Compress::stream_decompress`
    //      which rebuilds the original text and updates its own L-window.
    //   4) Concatenate all lines and write them to `decom_output_path`.
    if (config.stream_decompress || config.is_test)
    {
        const char *temp;
        if (config.is_test)
        {
            temp = config.file_path;
            std::cout << "Testing Decompression..." << std::endl;
            config.file_path = static_com_output_path.c_str();
        }

        std::cout << "-----using stream decompress-----" << std::endl;
        std::cout << "compressed file path:" << config.file_path << std::endl;
        std::cout << "decompressed output file path:" << config.decom_output_path << std::endl;

        // Load the entire compressed bitstream produced earlier by
        // `write_bitset_to_file`. This reconstructs the global
        // `boost::dynamic_bitset` used below.
        boost::dynamic_bitset<> compressed_bitset;
        XORC::read_bitset_from_file(compressed_bitset, config.file_path);

        // We now **parse the global bitstream** into per-line chunks,
        // mirroring the encoding decisions made in `stream_compress`.
        // For each line we collect:
        //   - a boolean flag `isRLE`  (0 = raw, 1 = XOR+RLE compressed)
        //   - an integer `original_length_or_window_id`
        //       * if raw: original byte length of the line
        //       * if compressed: index into the L-window of templates
        //   - a `boost::dynamic_bitset` holding that line's payload bits
        //       * raw:  original_length * 8 bits of data
        //       * compressed: RLE-coded XOR mask bits
        std::vector<boost::dynamic_bitset<>> split_compressed_bitset;
        std::vector<bool> isRLE;
        std::vector<int> original_length_or_window_id;
        size_t len_compressed_bitset = compressed_bitset.size();
        size_t i = 0;
        // Walk the entire compressed bitstream and split it into
        // per-line records using the same layout as in
        // `Stream_Compress::stream_compress`:
        //   - bit 0            : isRLE flag
        //   - next 64 bits     : per-record word bitmap metadata
        //   - if flag == 0     : ORIGINAL_LENGTH_COUNT bits of length,
        //                        then `length * 8` bits of raw bytes
        //   - if flag == 1     : EACH_WINDOW_SIZE_COUNT bits of window id,
        //                        STREAM_ENCODER_COUNT bits of RLE length,
        //                        then that many bits of RLE payload
        while (i < len_compressed_bitset)
        {
            if (compressed_bitset[i] == 0)
            {
                isRLE.push_back(false);
                i++;

                // Read and ignore the 64-bit per-record bitmap metadata.
                i += WORD_BITMAP_BITS;

                int tem_original_length = 0;
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

                split_compressed_bitset.push_back(tem_bitset);
            }
            else
            {
                isRLE.push_back(true);
                i++;

                // Read and ignore the 64-bit per-record bitmap metadata.
                i += WORD_BITMAP_BITS;

                int tem_window_id = 0;
                for (size_t j = 0; j < EACH_WINDOW_SIZE_COUNT; ++j, ++i)
                {
                    if (compressed_bitset[i])
                    {
                        tem_window_id |= (1 << j);
                    }
                }
                original_length_or_window_id.push_back(tem_window_id);

                int len_single_data = 0;
                for (size_t j = 0; j < STREAM_ENCODER_COUNT; ++j, ++i)
                {
                    if (compressed_bitset[i])
                    {
                        len_single_data |= (1 << j);
                    }
                }

                boost::dynamic_bitset<> tem_bitset(len_single_data);
                for (size_t j = 0; j < len_single_data; j++)
                {
                    tem_bitset[j] = compressed_bitset[i + j];
                }
                i += len_single_data;

                split_compressed_bitset.push_back(tem_bitset);
            }
        }

        std::string all_data;
        all_data.reserve(static_cast<size_t>(1024) *  1024 * 256);

        XORC::Stream_Compress *sc = new XORC::Stream_Compress();

        std::string xor_result;
        xor_result.reserve(500);

        clock_t start_time, end_time;
        start_time = clock();
        for (size_t i = 0; i < split_compressed_bitset.size(); ++i)
        {
            sc->stream_decompress(split_compressed_bitset[i], isRLE[i], original_length_or_window_id[i], all_data, xor_result);
        }
        end_time = clock();

        XORC::write_string_to_file(all_data, config.decom_output_path);

        int64_t raw_size = static_cast<int64_t>(file_size(config.decom_output_path)) - 2 * split_compressed_bitset.size();
        std::cout << "decompression speed: "
                  << (double)raw_size / (double)1024 / (double)1024 /
                         (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
                  << "MB/s" << std::endl;

        // bool isEqual=areFilesEqual(original_file_path,output_path);
        // std::cout << "is Equal?  "<< (isEqual?"yes":"no") << std::endl;

        delete sc;
    }

    return 0;
}