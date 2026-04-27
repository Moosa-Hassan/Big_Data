// file.h / file.cc
// -----------------
// Small collection of helper functions for reading and writing data to
// disk. The most important for LogLite-B are:
//   - `write_bitset_to_file` / `read_bitset_from_file`
//       which define the on-disk layout of the compressed `.lite`
//       bitstream that `xorc-cli` produces and consumes.
//
// The other helpers (`write_string_to_file`, `write_vector_to_file`,
// etc.) are used for performance measurement and for storing
// intermediate results, but they do not define the compression
// *format* itself.

#ifndef FILE_H_
#define FILE_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <boost/dynamic_bitset.hpp>

namespace XORC
{
    // Persist a `boost::dynamic_bitset<>` to disk. Implementation
    // packs the bitset into `unsigned long` blocks and appends a
    // trailing `size_t last_block_bits` indicating how many bits are
    // valid in the final block. This layout is reversed by
    // `read_bitset_from_file`.
    void write_bitset_to_file(const boost::dynamic_bitset<> &bitset, const char *filename);
    // Reconstruct a `boost::dynamic_bitset<>` from the file format
    // written by `write_bitset_to_file`.
    void read_bitset_from_file(boost::dynamic_bitset<> &bitset, const char *filename);

    // Convenience helpers for reading/writing plain strings.
    void write_string_to_file(const std::string &content, const char *filename);
    void read_string_from_file(std::string &content, const char *filename);

    // Binary I/O for small numeric vectors, used in some experiments
    // and measurement utilities.
    bool write_vector_to_file(const std::vector<uint16_t> &data, const std::string &filename);
    bool read_vector_from_file(std::vector<uint16_t> &data, const std::string &filename);

    void write_bytes_to_file(const std::vector<unsigned char> &data, const std::string &filename);
    void read_bytes_from_file(std::vector<unsigned char> &data, const std::string &filename);

    void write_sizetvector_to_file(const std::vector<size_t> &data, const std::string &filename);
    void read_sizetvector_from_file(std::vector<size_t> &data, const std::string &filename);

}

#endif