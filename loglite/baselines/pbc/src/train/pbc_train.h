
#ifndef SRC_TRAIN_PBC_TRAIN_H_
#define SRC_TRAIN_PBC_TRAIN_H_

#include <limits.h>
#include <stdio.h>
#include <time.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "compress/compress.h"

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

struct MinValueKey {
    int value;
    int key;
};

enum Type { pat, fs };

enum SourcePos { leftpos, uppos, upperleft, esc };
namespace PBC {
class PBC_Train {
private:
    char* data_buffer;
    uint64_t len;
    std::vector<char*> all_pattern_, all_data_;
    std::vector<int> all_pattern_len_, all_data_len_;
    // the state of clusters, cluster_id_[i] != i means cluster i had been merged.
    std::vector<int> cluster_id_;
    // the number of records in each pattern
    std::vector<int> record_num_;
    // the frequency of each character
    std::vector<int> char_freq_;
    // the initial pattern number
    int all_pattern_num_;
    // min_value_table_[i] stores the information about the cluster that has
    // the minimal EL increment for each cluster
    std::vector<MinValueKey> min_value_table_;
    // the 1-gram std::vector of each pattern
    std::vector<std::vector<int> > one_gram_table_;
    // the pruning threshold
    int last_value_ = INT_MAX;
    // compute the state transfer
    int UpdateState(int cur_state, enum Type suf_type, int isWildcard, int num_a, int num_b);
    // compute the minimal EL increment between two clusters
    int MinEncodingLength(char* str_a, char* str_b, int len_a, int len_b, int num_a, int num_b);
    // compute the pattern of a merged cluster
    int MergePattern(char* str_a, char* str_b, int len_a, int len_b, int num_a, int num_b,
                     std::string& str);
    // adding escape string to initialize strings
    char* InitializeKey(char* str, int64_t& len);
    // compute the minimal encoding length and corresponding cluster for each cluster (the
    // min_value_table_)
    void ComputeMinTable();
    // return the minimal encoding length and corresponding cluster ID
    void ComputeMinValue(int& v1, int& v2, int& value);
    // update the min_value_table_ after a merging
    void UpdateMinTable(int j, int v1, int v2);
    // convert a pattern to hyperscan format
    int Pattern2HS(char* str, int len, std::string num, char* str_hs);
    void CreatFseTable(char* tableBuffer, int64_t& buffer_len);

public:
    PBC_Train();
    ~PBC_Train();
    void LoadData(char* data_buffer, int64_t len);
    int64_t TrainPattern(int k, char* pattern_buffer);
};
}  // namespace PBC
#endif  // SRC_TRAIN_PBC_TRAIN_H_
