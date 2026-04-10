
#ifndef SRC_COMPRESS_COMPRESS_H_
#define SRC_COMPRESS_COMPRESS_H_

#include <time.h>
#include <unistd.h>

// We use the BSD primitives throughout as they exist on both BSD and Linux.
#define __FAVOR_BSD
#include <arpa/inet.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <hs/hs.h>
#include <immintrin.h>
#include <net/ethernet.h>
#include <netinet/in.h>
#include <netinet/in_systm.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <netinet/tcp.h>
#include <netinet/udp.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

extern "C" {
#include "deps/fse/bitstream.h"
#include "deps/fse/compiler.h"
#include "deps/fse/debug.h"
#include "deps/fse/error_private.h"
#include "deps/fse/error_public.h"
#include "deps/fse/fse.h"
#include "deps/fse/fseU16.h"
#include "deps/fse/hist.h"
#include "deps/fse/huf.h"
#include "deps/fse/mem.h"
}

typedef unsigned FSE_DTable;
typedef unsigned FSE_CTable;

#define FSE_DTABLE_SIZE_U32(maxTableLog) (1 + (1 << (maxTableLog)))

constexpr int SYMBOL_SIZE = 256;
constexpr int BUFFER_SIZE = (1024 * 1024);
constexpr int MAX_BUFFER_SIZE = (1024 * 1024);

struct patternInfo {
    int num;
    std::vector<int> pos;
    std::string data;
};

static std::vector<int> pattern_len_list;

namespace PBC {
class PBC_Compress {
private:
    std::vector<patternInfo> pattern_list;

    std::vector<std::string> patterns;
    std::vector<unsigned> flags;
    std::vector<unsigned> ids;

    FSE_CTable* g_CTable;
    FSE_DTable* g_DTable;

    int FillingSubsequences(int common_str_id, std::string& new_str, char* new_str3,
                            int new_str_len);
    void WriteVarint(uint32_t value, uint8_t* ptr, int& ptr_i);
    uint32_t Variant_Decode(uint8_t* data, int& data_i);

public:
    PBC_Compress();
    ~PBC_Compress();

    int pattern_num;
    hs_database_t* db_block;
    hs_scratch_t* scratch = nullptr;

    uint32_t g_max;
    uint32_t g_tableLog;

    int16_t g_normTable[SYMBOL_SIZE + 1];

    void readData(char* data, int64_t len);

    int compress_usingPattern(char* in_str, int in_str_len, char* out_str);
    int compress_usingPattern_fse(char* in_str, int in_str_len, char* out_str);
    int decompress_usingPattern(char* in_str, int in_str_len, char* out_str);

    int compress_usingPattern_getUnique(char* in_str, int in_str_len, char* out_str);
    int64_t getFseUnique(char* pattern_data, int64_t pattern_data_len, char* train_data,
                         int64_t train_data_len, char* unique_data, int64_t unique_data_len);
};
}  // namespace PBC

#endif  // SRC_COMPRESS_COMPRESS_H_
