
#include "compress/compress.h"

#include "base/memcpy.h"

namespace PBC {

PBC_Compress::PBC_Compress() {}

static int onMatch(unsigned int id, unsigned long long from, unsigned long long to,  // NOLINT
                   unsigned int flags, void* ctx) {
    // Our context points to a size_t storing the match count
    size_t* matches = reinterpret_cast<size_t*>(ctx);
    if (pattern_len_list[id] > pattern_len_list[static_cast<int>(*matches)]) {
        (*matches) = id;
    }
    return 0;  // continue matching
}
static hs_database_t* buildDatabase(const std::vector<const char*>& expressions,
                                    const std::vector<unsigned> flags,
                                    const std::vector<unsigned> ids, unsigned int mode) {
    hs_database_t* db;
    hs_compile_error_t* compileErr;
    hs_error_t err;

    err = hs_compile_multi(expressions.data(), flags.data(), ids.data(), expressions.size(), mode,
                           nullptr, &db, &compileErr);

    if (err != HS_SUCCESS) {
        if (compileErr->expression < 0) {
            // The error does not refer to a particular expression.
            std::cerr << "ERROR: " << compileErr->message << std::endl;
        } else {
            std::cerr << "ERROR: Pattern '" << expressions[compileErr->expression]
                      << "' failed compilation with error: " << compileErr->message << " "
                      << compileErr->expression << std::endl;
        }
        // As the compileErr pointer points to dynamically allocated memory, if
        // we get an error, we must be sure to release it. This is not
        // necessary when no error is detected.
        hs_free_compile_error(compileErr);
        exit(-1);
    }
    return db;
}

static unsigned parseFlags(const std::string& flagsStr) {
    unsigned flags = 0;
    for (const auto& c : flagsStr) {
        switch (c) {
            case 'i':
                flags |= HS_FLAG_CASELESS;
                break;
            case 'm':
                flags |= HS_FLAG_MULTILINE;
                break;
            case 's':
                flags |= HS_FLAG_DOTALL;
                break;
            case 'H':
                flags |= HS_FLAG_SINGLEMATCH;
                break;
            case 'V':
                flags |= HS_FLAG_ALLOWEMPTY;
                break;
            case '8':
                flags |= HS_FLAG_UTF8;
                break;
            case 'W':
                flags |= HS_FLAG_UCP;
                break;
            case 'B':
                flags |= HS_TUNE_FAMILY_IVB;
                break;
            case 'a':
                flags |= HS_CPU_FEATURES_AVX512;
                break;
            case '\r':  // stray carriage-return
                break;
            default:
                std::cerr << "Unsupported flag \'" << c << "\'" << std::endl;
                exit(-1);
        }
    }
    return flags;
}

/*
    hs_compile_multi requires three parallel arrays containing the patterns,
    flags and ids that we want to work with. To achieve this we use
    vectors and new entries onto each for each valid line of input from
    the pattern file.
    do the actual file reading and std::string handling
    parseFile(filename, patterns, flags, ids);
    Turn our std::vector of strings into a std::vector of char*'s to pass in to
    hs_compile_multi. (This is just using the std::vector of strings as dynamic
    storage.)
*/
static void databasesFromFile(std::vector<std::string> patterns, std::vector<unsigned> flags,
                              std::vector<unsigned> ids, hs_database_t** db_block) {
    std::vector<const char*> cstrPatterns;
    for (const auto& pattern : patterns) {
        cstrPatterns.push_back(pattern.c_str());
    }

    *db_block = buildDatabase(cstrPatterns, flags, ids, HS_MODE_BLOCK);
}

int PBC_Compress::FillingSubsequences(int common_str_id, std::string& new_str, char* new_str3,
                                      int new_str_len) {
    int new_str3_i = 2;
    char* common_str = &pattern_list[common_str_id].data[0];
    int i = 0, i2;

    char* new_str2 = &new_str[0];
    int num_i = 0;
    for (; num_i < pattern_list[common_str_id].num; num_i++) {
        if (pattern_list[common_str_id].pos[num_i + 1] - pattern_list[common_str_id].pos[num_i] ==
            0)
            continue;
        i2 = new_str.find(
            common_str + pattern_list[common_str_id].pos[num_i], i,
            pattern_list[common_str_id].pos[num_i + 1] - pattern_list[common_str_id].pos[num_i]);

        if (i2 - i == 0) {
            if (num_i > 0) {
                new_str3[new_str3_i++] = 0;
            }
        } else {
            WriteVarint((uint32_t)(i2 - i), (unsigned char*)new_str3 + new_str3_i, new_str3_i);
            memcpy(new_str3 + new_str3_i, new_str2 + i, i2 - i);

            new_str3_i += i2 - i;
        }
        i = i2 + pattern_list[common_str_id].pos[num_i + 1] -
            pattern_list[common_str_id].pos[num_i];
    }
    if (i < new_str_len) {
        WriteVarint((uint32_t)(new_str_len - i), (unsigned char*)new_str3 + new_str3_i, new_str3_i);
        memcpy(new_str3 + new_str3_i, new_str2 + i, new_str_len - i);
        new_str3_i += new_str_len - i;
    } else {
        if (pattern_list[common_str_id].pos[num_i] - pattern_list[common_str_id].pos[num_i - 1] ==
            0) {
            new_str3[new_str3_i++] = 0;
        }
    }
    new_str3[new_str3_i] = 0;
    return new_str3_i;
}

void PBC_Compress::WriteVarint(uint32_t value, uint8_t* ptr, int& ptr_i) {
    if (value < 0x80) {
        ptr[0] = static_cast<uint8_t>(value);
        ptr_i += 1;
        return;
    }
    ptr[0] = static_cast<uint8_t>(value | 0x80);
    value >>= 7;
    if (value < 0x80) {
        ptr[1] = static_cast<uint8_t>(value);
        ptr_i += 2;
        return;
    }
    ptr++;
    ptr_i += 2;
    do {
        *ptr = static_cast<uint8_t>(value | 0x80);
        value >>= 7;
        ++ptr;
        ptr_i++;
    } while ((value > 0x80));
    *ptr++ = static_cast<uint8_t>(value);
}

uint32_t PBC_Compress::Variant_Decode(uint8_t* data, int& data_i) {
    data_i++;
    unsigned int value = data[0];
    if ((value & 0x80) == 0) return value;
    value &= 0x7F;
    data_i++;
    unsigned int chunk = data[1];
    value |= (chunk & 0x7F) << 7;
    if ((chunk & 0x80) == 0) return value;
    chunk = data[2];
    data_i++;
    value |= (chunk & 0x7F) << 14;
    if ((chunk & 0x80) == 0) return value;
    chunk = data[3];
    data_i++;
    value |= (chunk & 0x7F) << 21;
    if ((chunk & 0x80) == 0) return value;
    chunk = data[4];
    data_i++;
    value |= chunk << 28;
    if ((chunk & 0xF0) == 0) return value;
    data_i -= 5;
    return 0;
}

void PBC_Compress::readData(char* data, int64_t len) {
    int64_t data_i = 0;
    std::string P_num_str = "";

    while (data_i < len && data[data_i] != '\n') {
        P_num_str += data[data_i];
        data_i++;
    }
    pattern_num = atoi(P_num_str.c_str());
    data_i++;

    pattern_list.resize(pattern_num);
    pattern_len_list.resize(pattern_num + 1);

    patterns.resize(pattern_num);
    flags.resize(pattern_num);
    ids.resize(pattern_num);

    unsigned pattern_i = 0;

    unsigned flag = parseFlags("Has");

    while (pattern_i < pattern_num) {
        flags[pattern_i] = flag;
        ids[pattern_i] = pattern_i;
        std::string pattern_HS = "";

        pattern_list[pattern_i].num = 0;
        pattern_list[pattern_i].pos.push_back(0);

        if (data[data_i] != '*') pattern_HS += '^';

        int pattern_list_pos = 0;

        while (data_i < len && data[data_i] != '\n') {
            if (data[data_i] == '\\') {
                data_i++;
            } else {
                if (data[data_i] == '*') {
                    pattern_HS += ".*";
                    pattern_list[pattern_i].num++;
                    pattern_list[pattern_i].pos.push_back(pattern_list_pos);
                    data_i++;
                    continue;
                }
            }
            if (data[data_i] == '$' || data[data_i] == '(' || data[data_i] == ')' ||
                data[data_i] == '[' || data[data_i] == ']' || data[data_i] == '$' ||
                data[data_i] == '{' || data[data_i] == '}' || data[data_i] == '?' ||
                data[data_i] == '^' || data[data_i] == '.' || data[data_i] == '+' ||
                data[data_i] == '*' || data[data_i] == '|' || data[data_i] == '-' ||
                data[data_i] == '\\' || data[data_i] == '=' || data[data_i] == ':' ||
                data[data_i] == '/') {
                pattern_HS += '\\';
            }
            pattern_HS += data[data_i];
            pattern_list[pattern_i].data += data[data_i];

            pattern_list_pos++;
            data_i++;
        }

        if (pattern_HS[pattern_HS.length() - 1] != '*') pattern_HS += '$';
        patterns[pattern_i] = pattern_HS;
        pattern_list[pattern_i].num++;
        pattern_list[pattern_i].pos.push_back(pattern_list_pos);
        pattern_len_list[pattern_i] =
            pattern_list[pattern_i].data.length() - pattern_list[pattern_i].num;
        data_i++;
        pattern_i++;
    }
    pattern_len_list[pattern_i] = 0;

    databasesFromFile(patterns, flags, ids, &db_block);

    hs_error_t err = hs_alloc_scratch(db_block, &scratch);

    if (err != HS_SUCCESS) {
        std::cerr << "ERROR: could not allocate scratch space. Exiting." << std::endl;
        exit(-1);
    }

    if (len - data_i == 0) return;

    unsigned maxSymbolValue = SYMBOL_SIZE;
    unsigned tableLog;

    FSE_readNCount(g_normTable, &maxSymbolValue, &tableLog, data + data_i, len - data_i);
    g_max = maxSymbolValue;
    g_tableLog = tableLog;

    g_CTable = FSE_createCTable(g_max, g_tableLog);
    FSE_buildCTable(g_CTable, g_normTable, g_max, g_tableLog);

    g_DTable = FSE_createDTable(g_tableLog);
    FSE_buildDTable(g_DTable, g_normTable, g_max, g_tableLog);
}

int64_t PBC_Compress::getFseUnique(char* pattern_data, int64_t pattern_data_len, char* train_data,
                                   int64_t train_data_len, char* unique_data,
                                   int64_t unique_data_len) {
    readData(pattern_data, pattern_data_len);

    int64_t train_data_i = 0;
    char* compressed_data = new char[BUFFER_SIZE];
    char* single_key = new char[BUFFER_SIZE];
    int64_t single_key_i = 0;
    int64_t unique_i = 0;

    while (train_data_i < train_data_len && train_data[train_data_i] != '\n') {
        single_key[single_key_i++] = train_data[train_data_i];
        train_data_i++;
    }
    single_key[single_key_i] = 0;
    train_data_i++;

    while (train_data_i < train_data_len) {
        int l = compress_usingPattern_getUnique(single_key, single_key_i, compressed_data);

        if (l > 0) {
            memcpy(unique_data + unique_i, compressed_data, l);
            unique_i += l;
        }
        single_key_i = 0;
        while (train_data_i < train_data_len && train_data[train_data_i] != '\n') {
            single_key[single_key_i++] = train_data[train_data_i];
            train_data_i++;
        }
        single_key[single_key_i] = 0;
        train_data_i++;
    }

    if (single_key_i > 0) {
        int l = compress_usingPattern_getUnique(single_key, single_key_i, compressed_data);
        if (l > 0) {
            memcpy(unique_data + unique_i, compressed_data, l);
            unique_i += l;
        }
    }
    for (int i = 0; i < SYMBOL_SIZE; i++) {
        unique_data[unique_i++] = (unsigned char)i;
    }
    return unique_i;
}

char* out_str_temp = new char[BUFFER_SIZE];

int PBC_Compress::compress_usingPattern(char* in_str, int in_str_len, char* out_str) {
    size_t matchCount = this->pattern_num;

    hs_error_t err = hs_scan(db_block, in_str, in_str_len, 0, scratch, onMatch, &matchCount);

    if (err != HS_SUCCESS) {
        std::cout << "ERROR: Unable to scan packet. Exiting." << std::endl;
        exit(-1);
    }
    if (matchCount != pattern_num) {
        out_str[0] = 1;
        out_str[1] = matchCount / SYMBOL_SIZE;
        out_str[2] = matchCount % SYMBOL_SIZE;

        std::string in_str2(in_str, in_str_len);

        int l = FillingSubsequences(matchCount, in_str2, out_str + 1, in_str_len);

        return -1 * (l + 1);
    } else {
        return -1;
    }
}

int PBC_Compress::compress_usingPattern_fse(char* in_str, int in_str_len, char* out_str) {
    size_t matchCount = this->pattern_num;

    hs_error_t err = hs_scan(db_block, in_str, in_str_len, 0, scratch, onMatch, &matchCount);

    if (err != HS_SUCCESS) {
        std::cout << "ERROR: Unable to scan packet. Exiting." << std::endl;
        exit(-1);
    }
    if (matchCount != pattern_num) {
        out_str[0] = 1;
        out_str[1] = matchCount / SYMBOL_SIZE;
        out_str[2] = matchCount % SYMBOL_SIZE;

        std::string in_str2(in_str, in_str_len);

        int l = FillingSubsequences(matchCount, in_str2, out_str + 1, in_str_len);

        size_t cBSize =
            FSE_compress_usingCTable(out_str_temp, BUFFER_SIZE, out_str + 1, l, g_CTable);

        if (cBSize == 0) {
            return -1 * (l + 1);
        }

        memcpy(out_str + 1, out_str_temp, cBSize);
        out_str[cBSize + 1] = 0;
        return cBSize + 1;
    } else {
        return -1;
    }
}

char* decomp_data = new char[BUFFER_SIZE];
int PBC_Compress::decompress_usingPattern(char* str, int len, char* out_str) {
    size_t cBSize;
    if (len < 2 && len > -2) {
        return -1;
    }
    int common_str_id;
    int out_str_i = 0;

    uint32_t varint_num;
    if (len > 0) {
        cBSize = FSE_decompress_usingDTable(decomp_data, BUFFER_SIZE, str + 1, len - 1, g_DTable);
        decomp_data[cBSize] = 0;
        len = cBSize;
    } else {
        len = -1 * (len + 1);
        memcpy(decomp_data, str + 1, len);
    }
    common_str_id =
        (static_cast<int32_t>(static_cast<unsigned char>(decomp_data[0])) * SYMBOL_SIZE +
         static_cast<int32_t>(static_cast<unsigned char>(decomp_data[1])));

    if (len == 2) {
        memcpy(out_str + out_str_i, pattern_list[common_str_id].data.c_str(),
               pattern_list[common_str_id].data.length());
        out_str_i += pattern_list[common_str_id].data.length();
        out_str[out_str_i] = 0;
        return pattern_list[common_str_id].data.length();
    }
    char* common_str = &pattern_list[common_str_id].data[0];

    int i = 2;
    if (pattern_list[common_str_id].pos[1] - pattern_list[common_str_id].pos[0] == 0) {
        varint_num = Variant_Decode((unsigned char*)(decomp_data + i), i);
        memcpy(out_str + out_str_i, decomp_data + i, varint_num);
        out_str_i += varint_num;
        i = i + varint_num;
    }

    for (int num_i = 0; num_i < pattern_list[common_str_id].num; num_i++) {
        if (pattern_list[common_str_id].pos[num_i + 1] - pattern_list[common_str_id].pos[num_i] ==
            0)
            continue;
        memcpy(out_str + out_str_i, common_str + pattern_list[common_str_id].pos[num_i],
               pattern_list[common_str_id].pos[num_i + 1] - pattern_list[common_str_id].pos[num_i]);
        out_str_i +=
            pattern_list[common_str_id].pos[num_i + 1] - pattern_list[common_str_id].pos[num_i];
        if (i < len) {
            varint_num = Variant_Decode((unsigned char*)(decomp_data + i), i);
            memcpy(out_str + out_str_i, decomp_data + i, varint_num);
            out_str_i += varint_num;
            i = i + varint_num;
        }
    }
    out_str[out_str_i] = 0;
    return out_str_i;
}

int PBC_Compress::compress_usingPattern_getUnique(char* in_str, int in_str_len, char* out_str) {
    size_t matchCount = this->pattern_num;

    hs_error_t err = hs_scan(db_block, in_str, in_str_len, 0, scratch, onMatch, &matchCount);

    if (err != HS_SUCCESS) {
        std::cout << "ERROR: Unable to scan packet. Exiting." << std::endl;
        exit(-1);
    }
    if (matchCount != pattern_num) {
        out_str[0] = matchCount / SYMBOL_SIZE;
        out_str[1] = matchCount % SYMBOL_SIZE;
        std::string in_str2(in_str, in_str_len);
        int l = FillingSubsequences(matchCount, in_str2, out_str, in_str_len);

        return l;
    } else {
        return -1;
    }
}
}  // namespace PBC
