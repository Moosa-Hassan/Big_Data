#include <algorithm>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr const char kMagic[8] = {'Q', 'I', 'D', 'X', '3', 'M', 'M', '\0'};
constexpr std::uint32_t kVersion = 5;
constexpr std::size_t kHeaderSize = 152;
constexpr std::size_t kDictionaryEntrySize = 16;
constexpr std::size_t kLineDirectoryEntrySize = 12;

std::uint32_t load_u32(const unsigned char* data) {
    return static_cast<std::uint32_t>(data[0]) |
           (static_cast<std::uint32_t>(data[1]) << 8) |
           (static_cast<std::uint32_t>(data[2]) << 16) |
           (static_cast<std::uint32_t>(data[3]) << 24);
}

std::uint64_t load_u64(const unsigned char* data) {
    std::uint64_t value = 0;
    for (int index = 7; index >= 0; --index) {
        value = (value << 8) | static_cast<std::uint64_t>(data[index]);
    }
    return value;
}

struct Header {
    std::uint32_t version = 0;
    std::uint32_t header_size = 0;
    std::uint64_t record_count = 0;
    std::uint64_t q1_offset = 0;
    std::uint64_t q1_count = 0;
    std::uint64_t q2_offset = 0;
    std::uint64_t q2_count = 0;
    std::uint64_t q3_offset = 0;
    std::uint64_t q3_count = 0;
    std::uint64_t postings_offset = 0;
    std::uint64_t postings_size = 0;
    std::uint64_t line_directory_offset = 0;
    std::uint64_t line_directory_size = 0;
    std::uint64_t line_slab_offset = 0;
    std::uint64_t line_slab_size = 0;
};

struct PostingMeta {
    std::uint32_t relative_offset = 0;
    std::uint32_t byte_length = 0;
    std::uint32_t count = 0;
    bool present = false;
};

struct GramRequest {
    int gram_size = 0;
    std::uint32_t gram = 0;
    std::uint32_t count = 0;
};

bool parse_header(const unsigned char* data, std::size_t size, Header& header) {
    if (size < kHeaderSize) {
        std::cerr << "qidx3 file is too small\n";
        return false;
    }
    if (std::memcmp(data, kMagic, sizeof(kMagic)) != 0) {
        std::cerr << "unsupported qidx3 magic\n";
        return false;
    }
    header.version = load_u32(data + 8);
    header.header_size = load_u32(data + 12);
    if (header.version != kVersion || header.header_size != kHeaderSize) {
        std::cerr << "unsupported qidx3 version or header size\n";
        return false;
    }
    const unsigned char* values = data + 16;
    header.record_count = load_u64(values + 4 * 8);
    header.q1_offset = load_u64(values + 5 * 8);
    header.q1_count = load_u64(values + 6 * 8);
    header.q2_offset = load_u64(values + 7 * 8);
    header.q2_count = load_u64(values + 8 * 8);
    header.q3_offset = load_u64(values + 9 * 8);
    header.q3_count = load_u64(values + 10 * 8);
    header.postings_offset = load_u64(values + 11 * 8);
    header.postings_size = load_u64(values + 12 * 8);
    header.line_directory_offset = load_u64(values + 13 * 8);
    header.line_directory_size = load_u64(values + 14 * 8);
    header.line_slab_offset = load_u64(values + 15 * 8);
    header.line_slab_size = load_u64(values + 16 * 8);

    if (header.line_directory_offset + header.line_directory_size > size ||
        header.line_slab_offset + header.line_slab_size > size ||
        header.postings_offset + header.postings_size > size ||
        header.line_directory_size != header.record_count * kLineDirectoryEntrySize ||
        header.q2_offset != header.q1_offset + header.q1_count * kDictionaryEntrySize ||
        header.q3_offset != header.q2_offset + header.q2_count * kDictionaryEntrySize ||
        header.postings_offset != header.q3_offset + header.q3_count * kDictionaryEntrySize) {
        std::cerr << "qidx3 section offsets are invalid\n";
        return false;
    }
    return true;
}

std::pair<std::uint64_t, std::uint64_t> section_for_gram_size(const Header& header, int gram_size) {
    if (gram_size == 1) {
        return {header.q1_offset, header.q1_count};
    }
    if (gram_size == 2) {
        return {header.q2_offset, header.q2_count};
    }
    if (gram_size == 3) {
        return {header.q3_offset, header.q3_count};
    }
    return {0, 0};
}

PostingMeta find_posting_meta(const unsigned char* data, const Header& header, int gram_size, std::uint32_t gram) {
    auto [section_offset, section_count] = section_for_gram_size(header, gram_size);
    std::uint64_t low = 0;
    std::uint64_t high = section_count;
    while (low < high) {
        std::uint64_t middle = (low + high) / 2;
        const unsigned char* entry = data + section_offset + middle * kDictionaryEntrySize;
        std::uint32_t current_gram = load_u32(entry);
        if (current_gram == gram) {
            PostingMeta meta;
            meta.relative_offset = load_u32(entry + 4);
            meta.byte_length = load_u32(entry + 8);
            meta.count = load_u32(entry + 12);
            meta.present = true;
            return meta;
        }
        if (current_gram < gram) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return PostingMeta{};
}

std::vector<std::uint32_t> decode_postings(
    const unsigned char* data,
    const Header& header,
    const PostingMeta& meta
) {
    std::vector<std::uint32_t> postings;
    postings.reserve(meta.count);
    const unsigned char* cursor = data + header.postings_offset + meta.relative_offset;
    const unsigned char* end = cursor + meta.byte_length;
    std::uint32_t previous = 0;
    while (cursor < end) {
        std::uint64_t value = 0;
        int shift = 0;
        while (true) {
            if (cursor >= end) {
                throw std::runtime_error("truncated varint posting list");
            }
            unsigned char byte = *cursor++;
            value |= static_cast<std::uint64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 63) {
                throw std::runtime_error("varint posting value is too large");
            }
        }
        std::uint32_t record_id = postings.empty()
            ? static_cast<std::uint32_t>(value)
            : previous + static_cast<std::uint32_t>(value);
        postings.push_back(record_id);
        previous = record_id;
    }
    if (postings.size() != meta.count) {
        throw std::runtime_error("posting count mismatch");
    }
    return postings;
}

std::vector<std::uint32_t> intersect_sorted(
    const std::vector<std::uint32_t>& left,
    const std::vector<std::uint32_t>& right
) {
    std::vector<std::uint32_t> result;
    std::set_intersection(
        left.begin(),
        left.end(),
        right.begin(),
        right.end(),
        std::back_inserter(result)
    );
    return result;
}

std::set<std::uint32_t> qgrams_for_term(const std::string& term, int gram_size) {
    std::set<std::uint32_t> grams;
    if (gram_size <= 0 || term.size() < static_cast<std::size_t>(gram_size)) {
        return grams;
    }
    for (std::size_t offset = 0; offset + gram_size <= term.size(); ++offset) {
        std::uint32_t gram = 0;
        for (int index = 0; index < gram_size; ++index) {
            gram = (gram << 8) | static_cast<unsigned char>(term[offset + index]);
        }
        grams.insert(gram);
    }
    return grams;
}

bool choose_slab_scan(
    std::uint64_t record_count,
    const std::vector<GramRequest>& requests,
    std::uint64_t estimated_postings_ids
) {
    if (record_count == 0 || requests.empty()) {
        return requests.empty();
    }
    for (const GramRequest& request : requests) {
        if (request.gram_size <= 2 && request.count >= record_count * 3 / 4) {
            return true;
        }
    }
    return estimated_postings_ids >= record_count * 2;
}

bool contains_all(std::string_view line, const std::vector<std::string>& terms) {
    for (const std::string& term : terms) {
        if (line.find(term) == std::string_view::npos) {
            return false;
        }
    }
    return true;
}

std::string_view line_view(const unsigned char* data, const Header& header, std::uint32_t record_id) {
    const unsigned char* directory_entry =
        data + header.line_directory_offset + static_cast<std::uint64_t>(record_id) * kLineDirectoryEntrySize;
    std::uint64_t line_offset = load_u64(directory_entry);
    std::uint32_t line_length = load_u32(directory_entry + 8);
    if (line_offset + line_length > header.line_slab_size) {
        throw std::runtime_error("qidx3 line directory points outside the slab");
    }
    const char* line_data = reinterpret_cast<const char*>(data + header.line_slab_offset + line_offset);
    return std::string_view(line_data, line_length);
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: qidx-search <path.qidx3> [term ...]\n";
        return 2;
    }

    std::vector<std::string> terms;
    for (int index = 2; index < argc; ++index) {
        terms.emplace_back(argv[index]);
    }

    int fd = open(argv[1], O_RDONLY);
    if (fd < 0) {
        std::cerr << "failed to open qidx3 file\n";
        return 2;
    }

    struct stat file_stat {};
    if (fstat(fd, &file_stat) != 0 || file_stat.st_size <= 0) {
        std::cerr << "failed to stat qidx3 file\n";
        close(fd);
        return 2;
    }

    std::size_t file_size = static_cast<std::size_t>(file_stat.st_size);
    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        std::cerr << "failed to mmap qidx3 file\n";
        return 2;
    }

    const unsigned char* data = static_cast<const unsigned char*>(mapped);
    Header header;
    if (!parse_header(data, file_size, header)) {
        munmap(mapped, file_size);
        return 2;
    }

    try {
        bool missing_required_gram = false;
        std::uint64_t estimated_postings_ids = 0;
        std::uint64_t postings_lists_touched = 0;
        std::uint64_t postings_ids_read = 0;
        std::uint64_t verified_records = 0;
        std::uint64_t verified_bytes = 0;
        std::vector<GramRequest> gram_requests;
        std::set<std::pair<int, std::uint32_t>> seen_grams;
        for (const std::string& term : terms) {
            if (term.empty()) {
                continue;
            }
            int gram_size = std::min<std::size_t>(3, term.size());
            for (std::uint32_t gram : qgrams_for_term(term, gram_size)) {
                if (!seen_grams.insert({gram_size, gram}).second) {
                    continue;
                }
                PostingMeta meta = find_posting_meta(data, header, gram_size, gram);
                if (!meta.present || meta.count == 0) {
                    missing_required_gram = true;
                    break;
                }
                gram_requests.push_back(GramRequest{gram_size, gram, meta.count});
                estimated_postings_ids += meta.count;
            }
            if (missing_required_gram) {
                break;
            }
        }

        std::vector<std::uint32_t> candidates;
        std::string planner_strategy = "postings_intersection";
        if (!missing_required_gram) {
            if (choose_slab_scan(header.record_count, gram_requests, estimated_postings_ids)) {
                planner_strategy = "line_slab_scan";
                candidates.reserve(static_cast<std::size_t>(header.record_count));
                for (std::uint64_t record_id = 0; record_id < header.record_count; ++record_id) {
                    candidates.push_back(static_cast<std::uint32_t>(record_id));
                }
            } else {
                planner_strategy = "postings_intersection";
                std::sort(
                    gram_requests.begin(),
                    gram_requests.end(),
                    [](const GramRequest& left, const GramRequest& right) {
                        return left.count < right.count;
                    }
                );
                bool initialized = false;
                for (const GramRequest& request : gram_requests) {
                    PostingMeta meta = find_posting_meta(data, header, request.gram_size, request.gram);
                    std::vector<std::uint32_t> postings = decode_postings(data, header, meta);
                    ++postings_lists_touched;
                    postings_ids_read += postings.size();
                    if (!initialized) {
                        candidates = std::move(postings);
                        initialized = true;
                    } else {
                        candidates = intersect_sorted(candidates, postings);
                    }
                    if (candidates.empty()) {
                        break;
                    }
                }
            }
        } else {
            planner_strategy = "missing_gram";
        }

        for (std::uint32_t record_id : candidates) {
            std::string_view line = line_view(data, header, record_id);
            ++verified_records;
            verified_bytes += line.size();
            if (contains_all(line, terms)) {
                std::cout.write(line.data(), static_cast<std::streamsize>(line.size()));
                std::cout.put('\n');
            }
        }
        std::cerr << "QIDX3_STATS"
                  << " planner_strategy=" << planner_strategy
                  << " total_records=" << header.record_count
                  << " verified_records=" << verified_records
                  << " verified_bytes=" << verified_bytes
                  << " postings_lists_touched=" << postings_lists_touched
                  << " postings_ids_read=" << postings_ids_read
                  << '\n';
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        munmap(mapped, file_size);
        return 2;
    }

    munmap(mapped, file_size);
    return 0;
}
