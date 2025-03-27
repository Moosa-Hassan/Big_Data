#include "train/pbc_train.h"

#include <algorithm>

namespace PBC {

PBC_Train::PBC_Train() {}
PBC_Train::~PBC_Train() {}

void PBC_Train::LoadData(char* data_buffer, int64_t len) {
    this->data_buffer = data_buffer;
    this->len = len;
    int64_t temp_len, len_with_escapestr;
    int cluster_i = 0;

    char* temp = new char[len];
    int64_t temp_i = 0;

    int64_t buffer_i = 0;
    while (buffer_i < len && data_buffer[buffer_i] != '\n') {
        temp[temp_i++] = data_buffer[buffer_i];
        buffer_i++;
    }
    buffer_i++;

    while (buffer_i < len) {
        char* single_temp = new char[temp_i + 1];
        temp_len = len_with_escapestr = temp_i;
        memcpy(single_temp, temp, temp_i);
        all_data_.push_back(temp);
        all_pattern_.push_back(InitializeKey(temp, len_with_escapestr));
        all_pattern_len_.push_back(len_with_escapestr);
        all_data_len_.push_back(temp_len);
        // initially each cluster only contains 1 string
        record_num_.push_back(1);
        // cluster id initialization
        cluster_id_.push_back(cluster_i++);
        // counting the symbol frequency for 1-gram pruning
        std::vector<int> char_table(SYMBOL_SIZE, 0);
        char_freq_.push_back(temp_len);
        for (int i = 0; i < temp_len; i++) {
            char_table[static_cast<int32_t>(static_cast<unsigned char>(temp[i]))]++;
        }
        one_gram_table_.push_back(char_table);

        temp_i = 0;
        while (buffer_i < len && data_buffer[buffer_i] != '\n') {
            temp[temp_i++] = data_buffer[buffer_i];
            buffer_i++;
        }
        buffer_i++;
    }
    if (temp_i > 0) {
        char* single_temp = new char[temp_i + 1];
        temp_len = len_with_escapestr = temp_i;
        memcpy(single_temp, temp, temp_i);
        all_data_.push_back(temp);
        all_pattern_.push_back(InitializeKey(temp, len_with_escapestr));
        all_pattern_len_.push_back(len_with_escapestr);
        all_data_len_.push_back(temp_len);
        // initially each cluster only contains 1 string
        record_num_.push_back(1);
        // cluster id initialization
        cluster_id_.push_back(cluster_i++);
        // counting the symbol frequency for 1-gram pruning
        std::vector<int> char_table(SYMBOL_SIZE, 0);
        char_freq_.push_back(temp_len);
        for (int i = 0; i < temp_len; i++) {
            char_table[static_cast<int32_t>(static_cast<unsigned char>(temp[i]))]++;
        }
        one_gram_table_.push_back(char_table);
    }
    all_pattern_num_ = static_cast<int>(all_pattern_.size());
}

int PBC_Train::UpdateState(int cur_state, enum Type suf_type, int isWildcard, int num_a,
                           int num_b) {
    if (suf_type == pat)
        // if current suffix is in pattern, we should count the wildcard for both two clusters
        cur_state = cur_state + num_a + num_b;
    if (isWildcard == 0)
        // if current suffix is not wildcard, we should count the symbol
        cur_state = cur_state + num_a;
    else
        // if current suffix is wildcard, we should minus the repeated wildcard
        cur_state = cur_state - num_a;
    return cur_state;
}

int PBC_Train::MinEncodingLength(char* str_a, char* str_b, int len_a, int len_b, int num_a,
                                 int num_b) {
    std::vector<std::vector<Type> > type_table(len_a + 1, std::vector<Type>(len_b + 1));
    type_table[0][0] = pat;
    // recording the encoding length increment
    std::vector<std::vector<int> > state_table(len_a + 1, std::vector<int>(len_b + 1));
    state_table[0][0] = 0;

    for (int i = 1; i < len_a + 1; i++) {
        // the type of this position should be filling subsequence
        type_table[i][0] = fs;
        // processing the escape string
        if (str_a[i - 1] == '\\') {
            i++;
            state_table[i][0] =
                UpdateState(state_table[i - 2][0], type_table[i - 2][0], 0, num_a, num_b);
        } else {
            if (str_a[i - 1] == '*')
                state_table[i][0] =
                    UpdateState(state_table[i - 1][0], type_table[i - 1][0], 1, num_a, num_b);
            else
                state_table[i][0] =
                    UpdateState(state_table[i - 1][0], type_table[i - 1][0], 0, num_a, num_b);
        }
    }
    for (int j = 1; j < len_b + 1; j++) {
        type_table[0][j] = fs;
        // processing the escape string
        if (str_b[j - 1] == '\\') {
            j++;
            state_table[0][j] =
                UpdateState(state_table[0][j - 2], type_table[0][j - 2], 0, num_b, num_a);
        } else {
            if (str_b[j - 1] == '*')
                state_table[0][j] =
                    UpdateState(state_table[0][j - 1], type_table[0][j - 1], 1, num_b, num_a);
            else
                state_table[0][j] =
                    UpdateState(state_table[0][j - 1], type_table[0][j - 1], 0, num_b, num_a);
        }
    }

    int min_single = INT_MAX;
    // add_a, add_b record the position of value transition
    int add_a = 0, add_b = 0;
    for (int i = 1; i < len_a + 1; i++) {
        add_a = 0;
        if (str_a[i - 1] == '\\') {
            add_a++;
            i++;
        }
        for (int j = 1; j < len_b + 1; j++) {
            add_b = 0;
            if (str_b[j - 1] == '\\') {
                add_b++;
                j++;
            }
            if (str_a[i - 1] == str_b[j - 1] && str_a[i - 1 - add_a] != '*' &&
                str_b[j - 1 - add_b] != '*' && str_a[i - 1 - add_a] != '\0' &&
                str_b[j - 1 - add_b] != '\0') {
                int value1, value2;

                // compute the value transfered from state[i][j-1] and state[i-1][j]
                // respectively
                value1 = UpdateState(state_table[i - 1 - add_a][j], type_table[i - 1 - add_a][j], 0,
                                     num_a, num_b);
                value2 = UpdateState(state_table[i][j - 1 - add_b], type_table[i][j - 1 - add_b], 0,
                                     num_b, num_a);

                //  the minimal EL is transfered from state[i][j-1], state[i-1][j] and
                //  state[i-1][j-1]
                if (value1 < state_table[i - 1 - add_a][j - 1 - add_b] ||
                    value2 < state_table[i - 1 - add_a][j - 1 - add_b]) {
                    type_table[i][j] = fs;

                    if (value1 >= value2) {
                        state_table[i][j] = value2;
                    } else {
                        state_table[i][j] = value1;
                    }

                } else if (value1 == state_table[i - 1 - add_a][j - 1 - add_b] ||
                           value2 == state_table[i - 1 - add_a][j - 1 - add_b]) {
                    state_table[i][j] = state_table[i - 1 - add_a][j - 1 - add_b];
                    type_table[i][j] = fs;
                } else {
                    state_table[i][j] = state_table[i - 1 - add_a][j - 1 - add_b];
                    type_table[i][j] = pat;
                }
            } else {
                // update the value transfered from state[i][j-1] and state[i-1][j] respectively
                int value1, value2;
                if (str_a[i - 1 - add_a] == '*')
                    value1 = UpdateState(state_table[i - 1 - add_a][j],
                                         type_table[i - 1 - add_a][j], 1, num_a, num_b);
                else
                    value1 = UpdateState(state_table[i - 1 - add_a][j],
                                         type_table[i - 1 - add_a][j], 0, num_a, num_b);
                if (str_b[j - 1 - add_b] == '*')
                    value2 = UpdateState(state_table[i][j - 1 - add_b],
                                         type_table[i][j - 1 - add_b], 1, num_b, num_a);
                else
                    value2 = UpdateState(state_table[i][j - 1 - add_b],
                                         type_table[i][j - 1 - add_b], 0, num_b, num_a);
                type_table[i][j] = fs;

                // the minimal EL is transfered from state[i][j-1] and state[i-1][j]
                if (value1 >= value2) {
                    state_table[i][j] = value2;
                } else {
                    state_table[i][j] = value1;
                }
            }
            if (state_table[i][j] < min_single) min_single = state_table[i][j];
        }
        if (min_single >= last_value_) return INT_MAX;
    }
    return state_table[len_a][len_b];
}

int PBC_Train::MergePattern(char* str_a, char* str_b, int len_a, int len_b, int num_a, int num_b,
                            std::string& str) {
    // store the suffix is in pattern or in filling subsequence
    std::vector<std::vector<Type> > type_table(len_a + 1, std::vector<Type>(len_b + 1));
    // store the source of state transition (left, up or upper left)
    std::vector<std::vector<SourcePos> > trans_sources(len_a + 1,
                                                       std::vector<SourcePos>(len_b + 1, esc));
    std::vector<std::vector<int> > state_table(len_a + 1, std::vector<int>(len_b + 1));
    state_table[0][0] = 0;
    type_table[0][0] = pat;
    for (int i = 1; i < len_a + 1; i++) {
        type_table[i][0] = fs;

        if (str_a[i - 1] == '\\') {
            i++;
            state_table[i][0] =
                UpdateState(state_table[i - 2][0], type_table[i - 2][0], 0, num_a, num_b);
        } else {
            if (str_a[i - 1] == '*')
                state_table[i][0] =
                    UpdateState(state_table[i - 1][0], type_table[i - 1][0], 1, num_a, num_b);
            else
                state_table[i][0] =
                    UpdateState(state_table[i - 1][0], type_table[i - 1][0], 0, num_a, num_b);
        }
    }

    for (int j = 1; j < len_b + 1; j++) {
        type_table[0][j] = fs;
        if (str_b[j - 1] == '\\') {
            j++;
            state_table[0][j] =
                UpdateState(state_table[0][j - 2], type_table[0][j - 2], 0, num_b, num_a);
        } else {
            if (str_b[j - 1] == '*')
                state_table[0][j] =
                    UpdateState(state_table[0][j - 1], type_table[0][j - 1], 1, num_b, num_a);
            else
                state_table[0][j] =
                    UpdateState(state_table[0][j - 1], type_table[0][j - 1], 0, num_b, num_a);
        }
    }

    int add_a = 0, add_b = 0;
    for (int i = 1; i < len_a + 1; i++) {
        add_a = 0;
        if (str_a[i - 1] == '\\') {
            add_a++;
            i++;
        }
        for (int j = 1; j < len_b + 1; j++) {
            add_b = 0;
            if (str_b[j - 1] == '\\') {
                add_b++;
                j++;
            }

            if (str_a[i - 1] == str_b[j - 1] && str_a[i - 1 - add_a] != '*' &&
                str_b[j - 1 - add_b] != '*' && str_a[i - 1 - add_a] != '\0' &&
                str_b[j - 1 - add_b] != '\0') {
                int value1, value2;
                value1 = UpdateState(state_table[i - 1 - add_a][j], type_table[i - 1 - add_a][j], 0,
                                     num_a, num_b);
                value2 = UpdateState(state_table[i][j - 1 - add_b], type_table[i][j - 1 - add_b], 0,
                                     num_b, num_a);

                if (value1 < state_table[i - 1 - add_a][j - 1 - add_b] ||
                    value2 < state_table[i - 1 - add_a][j - 1 - add_b]) {
                    type_table[i][j] = fs;

                    if (value1 >= value2) {
                        state_table[i][j] = value2;
                        trans_sources[i][j] = uppos;
                    } else {
                        trans_sources[i][j] = leftpos;
                        state_table[i][j] = value1;
                    }
                } else if (value1 == state_table[i - 1 - add_a][j - 1 - add_b] ||
                           value2 == state_table[i - 1 - add_a][j - 1 - add_b]) {
                    state_table[i][j] = state_table[i - 1 - add_a][j - 1 - add_b];
                    type_table[i][j] = fs;
                    if (value1 >= value2) {
                        trans_sources[i][j] = uppos;
                    } else {
                        trans_sources[i][j] = leftpos;
                    }
                } else {
                    state_table[i][j] = state_table[i - 1 - add_a][j - 1 - add_b];
                    type_table[i][j] = pat;
                    trans_sources[i][j] = upperleft;
                }

            } else {
                int value1, value2;
                if (str_a[i - 1 - add_a] == '*')
                    value1 = UpdateState(state_table[i - 1 - add_a][j],
                                         type_table[i - 1 - add_a][j], 1, num_a, num_b);
                else
                    value1 = UpdateState(state_table[i - 1 - add_a][j],
                                         type_table[i - 1 - add_a][j], 0, num_a, num_b);
                if (str_b[j - 1 - add_b] == '*')
                    value2 = UpdateState(state_table[i][j - 1 - add_b],
                                         type_table[i][j - 1 - add_b], 1, num_b, num_a);
                else
                    value2 = UpdateState(state_table[i][j - 1 - add_b],
                                         type_table[i][j - 1 - add_b], 0, num_b, num_a);

                type_table[i][j] = fs;
                if (value1 >= value2) {
                    state_table[i][j] = value2;
                    trans_sources[i][j] = uppos;
                } else {
                    trans_sources[i][j] = leftpos;
                    state_table[i][j] = value1;
                }
            }
        }
    }

    // last_type is suffix type of the current pos
    int pos_a = len_a, pos_b = len_b;
    Type last_type = type_table[len_a][len_b];

    if (type_table[len_a][len_b] != pat) {
        str = "*";
    }

    while (pos_a > 0 && pos_b > 0) {
        // only when the state is transfered from upperleft, we add the current suffix to the
        // pattern
        if (trans_sources[pos_a][pos_b] == upperleft) {
            str = str_a[pos_a - 1] + str;
            last_type = pat;
            pos_a--;
            pos_b--;
            // skip the escape string
            while (pos_a > 0 && pos_b > 0 && trans_sources[pos_a][pos_b] == esc) {
                if (last_type == pat) str = "\\" + str;
                pos_a--;
                pos_b--;
            }

        } else if (trans_sources[pos_a][pos_b] == uppos) {
            if (last_type == pat) {
                str = "*" + str;
                last_type = fs;
            }
            pos_b--;
            while (pos_a > 0 && pos_b > 0 && trans_sources[pos_a][pos_b] == esc) {
                if (last_type == pat) str = "\\" + str;
                pos_b--;
            }

        } else if (trans_sources[pos_a][pos_b] == leftpos) {
            if (last_type == pat) {
                str = "*" + str;
                last_type = fs;
            }
            pos_a--;
            while (pos_a > 0 && pos_b > 0 && trans_sources[pos_a][pos_b] == esc) {
                if (last_type == pat) str = "\\" + str;
                pos_a--;
            }
        }
    }

    if (pos_a != pos_b && str[0] != '*') str = "*" + str;
    return state_table[len_a][len_b];
}

// initialize the string and add the escape string
char* PBC_Train::InitializeKey(char* str, int64_t& len) {
    char* new_str = new char[2 * len];
    uint32_t new_str_i = 0;
    for (int i = 0; i < len; i++) {
        // add the escape string for wildcard symbol '*'
        if (str[i] == '*' || str[i] == '\\') {
            new_str[new_str_i++] = '\\';
        }
        new_str[new_str_i++] = str[i];
    }
    new_str[new_str_i] = 0;
    len = new_str_i;
    return new_str;
}

// compute the minimal encoding length and corresponding cluster for each cluster (the
// min_value_table_)
// To avoid duplicate computation, cluster(pattern_id = i) only compare with clusters(pattern_id >
// i)
void PBC_Train::ComputeMinTable() {
    min_value_table_.resize(all_pattern_num_);
    std::cout << "------------ compute minimal encoding length ------------" << std::endl;
    std::cout << "init pattern count:" << all_pattern_num_ << std::endl;
    std::cout << "---------------------------------------------------------" << std::endl
              << std::endl;

    for (int i = 0; i < all_pattern_num_ - 1; i++) {
        last_value_ = INT_MAX;
        MinValueKey temp;
        std::cout << "current compute MEL progress: " << i << "/" << all_pattern_num_ << std::endl;
        for (int j = i + 1; j < all_pattern_num_; j++) {
            int value = 0, value_common = 0;
            for (int k = 0; k < SYMBOL_SIZE; k++) {
                value_common =
                    value_common + std::min(one_gram_table_[i][k], one_gram_table_[j][k]);
            }

            if (((char_freq_[i] - value_common) * record_num_[i] +
                 (char_freq_[j] - value_common) * record_num_[j]) >= last_value_) {
                continue;
            }
            value = MinEncodingLength(all_pattern_[i], all_pattern_[j], all_pattern_len_[i],
                                      all_pattern_len_[j], record_num_[i], record_num_[j]);

            if (value < last_value_) {
                temp.value = value;
                temp.key = j;
                last_value_ = value;
            }
        }
        min_value_table_[i] = temp;
    }
}

void PBC_Train::ComputeMinValue(int& v1, int& v2, int& value) {
    v1 = v2 = -1;
    value = INT_MAX;
    for (int i = 0; i < all_pattern_num_ - 1; i++) {
        if (cluster_id_[i] != i) continue;
        if (min_value_table_[i].value < value) {
            v1 = i;
            v2 = min_value_table_[i].key;
            value = min_value_table_[i].value;
        }
    }
}
void PBC_Train::UpdateMinTable(int j, int v1, int v2) {
    // only update when the cluster j and its corresponding cluster with minimal EL is a merged
    // cluster v1
    if (min_value_table_[j].key == v1 || min_value_table_[j].key == v2) {
        int j2 = j;
        last_value_ = INT_MAX;
        MinValueKey temp;
        temp.value = INT_MAX;
        temp.key = -1;
        for (j = j + 1; j < all_pattern_num_; j++) {
            if (cluster_id_[j] != j) continue;
            int value = 0, value_common = 0;
            for (int k = 0; k < SYMBOL_SIZE; k++) {
                value_common =
                    value_common + std::min(one_gram_table_[j2][k], one_gram_table_[j][k]);
            }

            if (((char_freq_[j2] - value_common) * record_num_[j2] +
                 (char_freq_[j] - value_common) * record_num_[j]) >= last_value_)
                continue;

            value = MinEncodingLength(all_pattern_[j2], all_pattern_[j], all_pattern_len_[j2],
                                      all_pattern_len_[j], record_num_[j2], record_num_[j]);

            if (value < last_value_) {
                temp.value = value;
                temp.key = j;
                last_value_ = value;
            }
        }
        min_value_table_[j2] = temp;
    } else {
        if (j < v1) {
            int value = 0, value_common = 0;
            for (int k = 0; k < SYMBOL_SIZE; k++) {
                value_common =
                    value_common + std::min(one_gram_table_[v1][k], one_gram_table_[j][k]);
            }

            if (((char_freq_[v1] - value_common) * record_num_[v1] +
                 (char_freq_[j] - value_common) * record_num_[j]) < min_value_table_[j].value) {
                value = MinEncodingLength(all_pattern_[v1], all_pattern_[j], all_pattern_len_[v1],
                                          all_pattern_len_[j], record_num_[v1], record_num_[j]);

                if (value < min_value_table_[j].value) {
                    min_value_table_[j].value = value;
                    min_value_table_[j].key = v1;
                }
            }
        }
    }
}

int PBC_Train::Pattern2HS(char* str, int len, std::string num, char* str_hs) {
    int str_hs_i = 0;
    for (int i = 0; i < num.length(); i++) str_hs[str_hs_i++] = num[i];
    str_hs[str_hs_i++] = ':';
    str_hs[str_hs_i++] = '/';
    if (str[0] != '*') str_hs[str_hs_i++] = '^';

    for (int j = 0; j < len; j++) {
        if (static_cast<int>(str[j]) == '\\') {
            j++;
        } else {
            if (str[j] == '*') {
                str_hs[str_hs_i++] = '.';
                str_hs[str_hs_i++] = '*';
                continue;
            }
        }

        if (str[j] == '$' || str[j] == '(' || str[j] == ')' || str[j] == '[' || str[j] == ']' ||
            str[j] == '$' || str[j] == '{' || str[j] == '}' || str[j] == '?' || str[j] == '^' ||
            str[j] == '.' || str[j] == '+' || str[j] == '*' || str[j] == '|' || str[j] == '-' ||
            str[j] == '\\' || str[j] == '=' || str[j] == ':' || str[j] == '/') {
            str_hs[str_hs_i++] = '\\';
        }
        str_hs[str_hs_i++] = str[j];
    }
    if (str_hs[str_hs_i - 1] != '*') str_hs[str_hs_i++] = '$';
    str_hs[str_hs_i++] = '/';
    str_hs[str_hs_i++] = 'H';
    str_hs[str_hs_i] = 0;

    return str_hs_i;
}

void PBC_Train::CreatFseTable(char* tableBuffer, int64_t& buffer_len) {
    uint32_t g_max = SYMBOL_SIZE;
    uint32_t g_tableLog = 12;
    int16_t g_normTable[SYMBOL_SIZE];
    char* all_fillingSubsequence = new char[BUFFER_SIZE];
    int64_t all_fillingSubsequence_i = 0;
    PBC_Compress* c = new PBC_Compress();
    all_fillingSubsequence_i = c->getFseUnique(tableBuffer, buffer_len, this->data_buffer,
                                               this->len, all_fillingSubsequence, BUFFER_SIZE);

    unsigned int* g_countTable = new unsigned int[g_max + 1];
    HIST_count(g_countTable, &g_max, all_fillingSubsequence, all_fillingSubsequence_i);
    g_tableLog = FSE_optimalTableLog(g_tableLog, all_fillingSubsequence_i, g_max);
    FSE_normalizeCount(g_normTable, g_tableLog, g_countTable, all_fillingSubsequence_i, g_max);

    size_t cBSize =
        FSE_writeNCount(tableBuffer + buffer_len, BUFFER_SIZE, g_normTable, g_max, g_tableLog);

    buffer_len += cBSize;

    tableBuffer[buffer_len] = 0;
}

int64_t PBC_Train::TrainPattern(int k, char* pattern_buffer) {
    ComputeMinTable();
    int end_num = all_pattern_num_;

    int train_perc_count = 0;
    int64_t report_num = (end_num) / 100;
    int64_t itr_count = 0;
    int64_t real_p = 0, real_k = 0;

    std::cout << "------------ merge pattern ---------------" << std::endl;
    std::cout << "init pattern count:" << end_num << std::endl;
    std::cout << "pattern num:" << k << std::endl;
    std::cout << "------------------------------------------" << std::endl;

    while (end_num > 0) {
        if (itr_count > report_num * train_perc_count) {
            std::cout << "Pattern training " << train_perc_count
                      << "%. current pattern num: " << end_num << std::endl;
            train_perc_count++;
        }
        itr_count++;
        int v1, v2;
        int min_value;
        ComputeMinValue(v1, v2, min_value);
        cluster_id_[v2] = v1;

        std::string new_pattern;

        MergePattern(all_pattern_[v1], all_pattern_[v2], all_pattern_len_[v1], all_pattern_len_[v2],
                     record_num_[v1], record_num_[v2], new_pattern);

        int new_pattern_len = new_pattern.length();
        // update one_gram_table_
        std::vector<int> ch(SYMBOL_SIZE, 0);
        char_freq_[v1] = new_pattern_len;
        for (int i = 0; i < new_pattern_len; i++) {
            ch[static_cast<int32_t>(static_cast<unsigned char>(new_pattern[i]))]++;

            if (new_pattern[i] == '\\' && i > 0 && new_pattern[i - 1] != '\\') {
                ch[static_cast<int32_t>(static_cast<unsigned char>(new_pattern[i]))]--;
                char_freq_[v1]--;
            }
            if (new_pattern[i] == '*' && i > 0 && new_pattern[i - 1] != '\\') {
                ch[static_cast<int32_t>(static_cast<unsigned char>(new_pattern[i]))]--;
                char_freq_[v1]--;
            }
            all_pattern_[v1][i] = new_pattern[i];
        }
        all_pattern_[v1][new_pattern_len] = '\0';

        one_gram_table_[v1] = ch;
        all_pattern_len_[v1] = new_pattern_len;
        record_num_[v1] = record_num_[v1] + record_num_[v2];

        for (int j = 0; j < v2; j++) {
            if (cluster_id_[j] != j) continue;
            if (j == v1) continue;
            UpdateMinTable(j, v1, v2);
        }

        // update the minmal EL of v1 and corresponding pattern ID
        last_value_ = INT_MAX;
        MinValueKey temp;
        temp.value = INT_MAX;
        temp.key = -1;

        for (int j = v1 + 1; j < all_pattern_num_; j++) {
            if (cluster_id_[j] != j) continue;
            int value = 0, value_common = 0;
            for (int k = 0; k < SYMBOL_SIZE; k++) {
                value_common =
                    value_common + std::min(one_gram_table_[v1][k], one_gram_table_[j][k]);
            }

            if (((char_freq_[v1] - value_common) * record_num_[v1] +
                 (char_freq_[j] - value_common) * record_num_[j]) >= last_value_)
                continue;

            value = MinEncodingLength(all_pattern_[v1], all_pattern_[j], all_pattern_len_[v1],
                                      all_pattern_len_[j], record_num_[v1], record_num_[j]);

            if (value < last_value_) {
                temp.value = value;
                temp.key = j;
                last_value_ = value;
            }
        }
        min_value_table_[v1] = temp;
        end_num--;
        real_p = 0;
        real_k = 0;

        for (int i = 0; i < all_pattern_num_; i++) {
            if (cluster_id_[i] == i && all_pattern_len_[i] > 1 && record_num_[i] > 1) real_p++;
        }

        if((float)real_p/(float)end_num > 0.6){
            for (int i = 0; i < all_pattern_num_; i++) {
                if (cluster_id_[i] == i && all_pattern_len_[i] > 1 && record_num_[i] > 1) real_k+=all_pattern_len_[i];
            }
            if(real_k < k)break;
        }
    }

    int64_t buffer_len = 0;
    int pattern_num = 0;
    int pattern_num_test = 0;
    for (int i = 0; i < all_pattern_num_; i++) {
        if (cluster_id_[i] != i) continue;
        pattern_num_test++;
        if (all_pattern_len_[i] > 1 && record_num_[i] > 1) pattern_num++;
    }

    std::string pattern_num_ = std::to_string(pattern_num);
    for (int i = 0; i < pattern_num_.length(); i++) pattern_buffer[buffer_len++] = pattern_num_[i];
    pattern_buffer[buffer_len++] = '\n';

    for (int i = 0; i < all_pattern_num_; i++) {
        if (cluster_id_[i] != i) continue;
        if (all_pattern_len_[i] > 1 && record_num_[i] > 1) {
            memcpy(pattern_buffer + buffer_len, all_pattern_[i], all_pattern_len_[i]);
            buffer_len += all_pattern_len_[i];
            pattern_buffer[buffer_len++] = '\n';
        }
    }
    CreatFseTable(pattern_buffer, buffer_len);
    return buffer_len;
}
}  // namespace PBC
