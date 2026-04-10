

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "compress/compress.h"
#include "train/pbc_train.h"

// constexpr int MAX_FILE_SIZE = (1024 * 1024 * 1024);
constexpr int MAX_PATTERN_SIZE = (1024 * 1024);
constexpr int DEFAULT_PATTERN_SIZE = 50;
constexpr int DEFAULT_DATASET_SIZE = 500;
constexpr int SAMPLE_STEP = 5;
// constexpr int MIN_RANDOM_LEN = 30;
// constexpr int MAX_RANDOM_LEN = 50;
constexpr int MIN_RP_LEN = 30;
constexpr int MAX_RP_LEN = 50;
constexpr int MIN_RFS_LEN = 25;
constexpr int MAX_RFS_LEN = 30;
constexpr int MAX_RECORD_SIZE = 1024 * 8;
// constexpr int MIN_RECORD_NUM = 5000;

DEFINE_string(dataset_path, "./", "dataset_path");

// std::string test_datasets[] = {
//     "cities", "github_json", "unece", "ccf_",    "rmckeys_key", "tmp-compress_test_kv_",
//     "LogA",   "LogB",        "LogC",  "Android", "Apache",      "BGL",
//     "HDFS",   "email",       "urls",  "urls2"};

// std::string test_datasets[] = {
//     "cities", "github_json", "unece", "ccf_", "rmckeys_key", "tmp-compress_test_kv_",
// };

const std::vector<std::string> test_datasets = {"test_data"};
// std::string test_datasets[] = {"cities"};

// std::vector<std::string> datasets(test_datasets, test_datasets + 1);

#if 0

static int64_t generateRandomData(char* buffer, int64_t buffer_len, int64_t data_num, int64_t min_len,
                               int64_t max_len) {
    if (buffer_len < data_num * max_len) {
        std::cout << "The buffer size is too small." << std::endl;
        exit(1);
    }
    srand(time(0));
    int64_t buffer_size = 0;

    for (int i = 0; i < data_num; i++) {
        int add_len = rand() % (max_len - min_len);

        for (int j = 0; j < static_cast<int>(min_len) + add_len; j++) {
            int c_str = rand() % 255 + 1;
            while (c_str == 10) c_str = rand() % 255 + 1;
            buffer[buffer_size++] = static_cast<char>(c_str);
        }
        buffer[buffer_size++] = '\n';
    }

    return buffer_size;
}

#endif

static int64_t generateRandomDataWithPattern(char* buffer, int64_t buffer_len, int64_t data_num,
                                             int64_t data_min_len, int64_t data_max_len,
                                             int64_t pattern_num, int64_t pattern_min_len,
                                             int64_t pattern_max_len) {
    if (buffer_len < data_num * data_max_len) {
        std::cout << "The buffer size is too small." << std::endl;
        exit(1);
    }
    srand(time(0));
    int64_t buffer_size = 0;
    int c_str;

    int64_t data_num_i = 0;

    char* pattern_buffer = new char[pattern_max_len];
    int pattern_buffer_i = 0;

    int single_pattern_num = data_num / pattern_num;

    for (int i = 0; i < pattern_num; i++) {
        int add_pattern_len = rand() % (pattern_max_len - pattern_min_len);

        pattern_buffer_i = 0;

        for (int j = 0; j < pattern_min_len + add_pattern_len; j++) {
            c_str = rand() % 255 + 1;
            while (c_str == 10) c_str = rand() % 255 + 1;
            pattern_buffer[pattern_buffer_i++] = static_cast<char>(c_str);
        }

        for (int j = 0; j < single_pattern_num; j++) {
            memcpy(buffer + buffer_size, pattern_buffer, pattern_min_len + add_pattern_len);
            buffer_size += pattern_min_len + add_pattern_len;

            int add_len = rand() % (data_max_len - data_min_len);

            for (int k = 0; k < data_min_len + add_len - pattern_min_len - add_pattern_len; k++) {
                c_str = rand() % 255 + 1;
                while (c_str == 10) c_str = rand() % 255 + 1;
                buffer[buffer_size++] = static_cast<char>(c_str);
            }
            buffer[buffer_size++] = '\n';
            data_num_i++;
        }

        while (i == pattern_num - 1 && data_num_i < data_num) {
            memcpy(buffer + buffer_size, pattern_buffer, pattern_min_len + add_pattern_len);
            buffer_size += pattern_min_len + add_pattern_len;

            int add_len = rand() % (data_max_len - data_min_len);

            for (int k = 0; k < data_min_len + add_len - pattern_min_len - add_pattern_len; k++) {
                c_str = rand() % 255 + 1;
                while (c_str == 10) c_str = rand() % 255 + 1;
                buffer[buffer_size++] = static_cast<char>(c_str);
            }
            buffer[buffer_size++] = '\n';
            data_num_i++;
        }
    }

    return buffer_size;
}

std::vector<std::string> splitString(std::string str, const std::string& sep) {
    std::vector<std::string> str_vec;
    std::string::size_type pos1, pos2;
    pos2 = str.find(sep);
    pos1 = 0;
    while (std::string::npos != pos2) {
        str_vec.push_back(str.substr(pos1, pos2 - pos1));

        pos1 = pos2 + sep.size();
        pos2 = str.find(sep, pos1);
    }
    if (pos1 != str.length()) str_vec.push_back(str.substr(pos1));
    return str_vec;
}

static int64_t readFileAndSample(char* file_path, char** buffer_test, int64_t& buffer_test_len,
                                 char** buffer_train, int step_len) {
    struct stat statbuf;
    int64_t buffer_size = 0;
    int train_num = 1, data_num = 0;

    if (stat(file_path, &statbuf) != 0) {
        std::cout << "The input file does not exist: " << file_path << std::endl;
        exit(1);
    }

    int fd = open(file_path, O_RDONLY);
    char* temp =
        reinterpret_cast<char*>(mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0));
    int64_t buffer_train_len = 0;
    *buffer_test = new char[statbuf.st_size + 10];
    *buffer_train = new char[statbuf.st_size + 10];

    // if (MAX_FILE_SIZE < statbuf.st_size) {
    //     std::cout << "The buffer size is too small." << std::endl;
    //     exit(1);
    // }
    memcpy(*buffer_test, temp, statbuf.st_size);
    buffer_test_len = statbuf.st_size;

    while (buffer_size < statbuf.st_size && temp[buffer_size] != '\n') {
        (*buffer_train)[buffer_train_len++] = temp[buffer_size];
        buffer_size++;
    }
    (*buffer_train)[buffer_train_len++] = '\n';

    while (buffer_size < statbuf.st_size) {
        if (temp[buffer_size] == '\n') {
            data_num++;
            buffer_size++;
            if (data_num % step_len == 0) {
                while (buffer_size < statbuf.st_size && temp[buffer_size] != '\n') {
                    (*buffer_train)[buffer_train_len++] = temp[buffer_size];
                    buffer_size++;
                }
                (*buffer_train)[buffer_train_len++] = '\n';
                if (train_num++ > DEFAULT_DATASET_SIZE) break;
            }
        }
        buffer_size++;
    }

    return buffer_train_len;
}

TEST(PBC_TrainTest, GivenDatasets) {
    char* file_buffer = nullptr;
    char* train_data_buffer = nullptr;
    char* pattern_buffer = new char[MAX_PATTERN_SIZE];
    int64_t file_buffer_len = 0, train_data_len = 0;
    int64_t pattern_buffer_len = 0;

    for (auto& data : test_datasets) {
        // pattern training test
        std::string file_path = FLAGS_dataset_path + data;
        train_data_len = readFileAndSample(file_path.data(), &file_buffer, file_buffer_len,
                                           &train_data_buffer, SAMPLE_STEP);

        PBC::PBC_Train* t = new PBC::PBC_Train();
        t->PBC::PBC_Train::LoadData(train_data_buffer, train_data_len);
        pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);

        EXPECT_GT(pattern_buffer_len, 0);
        EXPECT_GT(strlen(pattern_buffer), 0);

        // int pattern_num = 0;

        // char* str_buffer = strtok(pattern_buffer, "\n");
        // while (str_buffer != NULL) {
        //     // each pattern is not empty
        //     EXPECT_GT(strlen(str_buffer), 0);
        //     pattern_num++;
        //     str_buffer = strtok(NULL, "\n");
        // }
        // // check the number of pattern
        // EXPECT_EQ(pattern_num - 2, DEFAULT_PATTERN_SIZE);
    }
}

#if 0  // ignore random data tests as pattern cannot be generated.

TEST(PBC_TrainTest, RandomData) {
    char* file_buffer = new char[DEFAULT_DATASET_SIZE * (MAX_RANDOM_LEN + 1)];
    char* pattern_buffer = new char[(DEFAULT_PATTERN_SIZE + 1) * MAX_RANDOM_LEN];
    // char* file_buffer = new char[MAX_FILE_SIZE];
    // char* pattern_buffer = new char[MAX_PATTERN_SIZE];

    int64_t file_buffer_len = 0;
    int64_t pattern_buffer_len = 0;

    // generate random dataset
    file_buffer_len = generateRandomData(file_buffer, DEFAULT_DATASET_SIZE * (MAX_RANDOM_LEN + 1),
                                         DEFAULT_DATASET_SIZE, MIN_RANDOM_LEN, MAX_RANDOM_LEN);
    // file_buffer_len = generateRandomData(file_buffer, MAX_FILE_SIZE, DEFAULT_DATASET_SIZE,
    //                                      MIN_RANDOM_LEN, MAX_RANDOM_LEN);
    // pattern training test
    PBC::PBC_Train* t = new PBC::PBC_Train();

    t->PBC::PBC_Train::LoadData(file_buffer, file_buffer_len);

    pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);
    EXPECT_GT(pattern_buffer_len, 0);
}

#endif

TEST(PBC_TrainTest, RandomDataWithPattern) {
    char* file_buffer = new char[DEFAULT_DATASET_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1];
    char* pattern_buffer = new char[DEFAULT_PATTERN_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1];
    int64_t file_buffer_len = 0;
    int64_t pattern_buffer_len = 0;

    // generate random dataset
    file_buffer_len = generateRandomDataWithPattern(
        file_buffer, DEFAULT_DATASET_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1, DEFAULT_DATASET_SIZE,
        MIN_RP_LEN, MAX_RP_LEN, DEFAULT_PATTERN_SIZE, MIN_RFS_LEN, MAX_RFS_LEN);
    // pattern training test

    PBC::PBC_Train* t = new PBC::PBC_Train();

    t->PBC::PBC_Train::LoadData(file_buffer, file_buffer_len);
    pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);
    EXPECT_GT(pattern_buffer_len, 0);
    EXPECT_GT(strlen(pattern_buffer), 0);
}

TEST(PBC_CompressionTest, GivenDatasets) {
    char* file_buffer = nullptr;
    char* train_data_buffer = nullptr;
    char* pattern_buffer = new char[MAX_PATTERN_SIZE];
    int64_t file_buffer_len = 0, train_data_len = 0;
    int64_t pattern_buffer_len = 0;

    for (auto& data : test_datasets) {
        // pattern training test
        std::string file_path = FLAGS_dataset_path + data;
        train_data_len = readFileAndSample(file_path.data(), &file_buffer, file_buffer_len,
                                           &train_data_buffer, SAMPLE_STEP);
        PBC::PBC_Train* t = new PBC::PBC_Train();
        t->PBC::PBC_Train::LoadData(train_data_buffer, train_data_len);
        pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);
        // compression test
        PBC::PBC_Compress* c = new PBC::PBC_Compress();
        c->PBC::PBC_Compress::readData(pattern_buffer, pattern_buffer_len);
        // test readData
        // EXPECT_EQ(c->PBC::PBC_Compress::pattern_num, DEFAULT_PATTERN_SIZE);

        std::string test_str;
        std::fstream str_fstream;
        str_fstream.open(file_path, std::ios::in);
        char* compressed_data = new char[MAX_RECORD_SIZE];
        char* decompressed_data = new char[MAX_RECORD_SIZE];
        int sum_compressed_len = 0, sum_raw_len = 0;
        int unmatch_num = 0, match_num = 0;

        while (getline(str_fstream, test_str)) {
            sum_raw_len += test_str.length();
            int compressed_len = c->PBC::PBC_Compress::compress_usingPattern(
                const_cast<char*>(test_str.c_str()), test_str.length(), compressed_data);
            if (compressed_len == -1) {
                sum_compressed_len += test_str.length();
                unmatch_num++;
                continue;
            } else if (compressed_len > 0) {
                sum_compressed_len += compressed_len;
                EXPECT_GT(strlen(compressed_data), 0);
            } else {
                sum_compressed_len -= compressed_len;
                EXPECT_GT(0, strlen(compressed_data));
            }
            match_num++;

            c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, compressed_len,
                                                          decompressed_data);
            // int decompressed_len = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data,
            // compressed_len, decompressed_data);

            EXPECT_EQ(0, strcmp(test_str.c_str(), decompressed_data))
                << "wrong compression and decompression";
            // EXPECT_EQ(std::string(decompressed_data), test_str) << "wrong compression and
            // decompression";
        }
        std::cout << "Test Set Compression ratio: "
                  << static_cast<double>(sum_compressed_len) / sum_raw_len << std::endl;
    }
}

#if 0  // ignore random data tests as pattern cannot be generated.

TEST(PBC_CompressionTest, RandomData) {
    char* file_buffer = new char[DEFAULT_DATASET_SIZE * MAX_RANDOM_LEN + 1];
    char* pattern_buffer = new char[(DEFAULT_PATTERN_SIZE + 1) * MAX_RANDOM_LEN];
    // char* train_data_buffer = new char[];
    int64_t file_buffer_len = 0;
    // int64_t train_data_len = 0;
    int64_t pattern_buffer_len = 0;

    // pattern training test
    file_buffer_len = generateRandomData(file_buffer, DEFAULT_DATASET_SIZE * MAX_RANDOM_LEN + 1,
                                         DEFAULT_DATASET_SIZE, MIN_RANDOM_LEN, MAX_RANDOM_LEN);
    std::string raw_data = file_buffer;
    PBC::PBC_Train* t = new PBC::PBC_Train();
    t->PBC::PBC_Train::LoadData(file_buffer, file_buffer_len);
    pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);
    // compression test
    PBC::PBC_Compress* c = new PBC::PBC_Compress();
    c->PBC::PBC_Compress::readData(pattern_buffer, pattern_buffer_len);
    // test readData
    // EXPECT_EQ(c->PBC::PBC_Compress::pattern_num, DEFAULT_PATTERN_SIZE);

    std::vector<std::string> raw_data_vec = splitString(raw_data, "\n");
    char* compressed_data = new char[MAX_RECORD_SIZE];
    char* decompressed_data = new char[MAX_RECORD_SIZE];
    int sum_compressed_len = 0, sum_raw_len = 0;
    int unmatch_num = 0, match_num = 0;

    for (auto& test_str : raw_data_vec) {
        sum_raw_len += test_str.length();
        int compressed_len = c->PBC::PBC_Compress::compress_usingPattern(
            test_str.c_str(), test_str.length(), compressed_data);
        if (compressed_len == -1) {
            sum_compressed_len += test_str.length();
            unmatch_num++;
            continue;
        } else if (compressed_len > 0) {
            sum_compressed_len += compressed_len;
            // EXPECT_GT(strlen(compressed_data), 0);
        } else {
            sum_compressed_len -= compressed_len;
            // EXPECT_GT(0, strlen(compressed_data));
        }
        match_num++;

        c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, compressed_len,
                                                      decompressed_data);
        // int decompressed_len = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data,
        // compressed_len, decompressed_data);

        EXPECT_EQ(0, strcmp(test_str.c_str(), decompressed_data))
            << "wrong compression and decompression";
        // EXPECT_EQ(std::string(decompressed_data), test_str) << "wrong compression and
        // decompression";
        std::cout << "Compression ratio: " << static_cast<double>(sum_compressed_len) / sum_raw_len
                  << std::endl;
    }
}

#endif

TEST(PBC_CompressionTest, RandomDataWithPattern) {
    char* file_buffer = new char[DEFAULT_DATASET_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1];

    // char* train_data_buffer = new char[MAX_FILE_SIZE];
    char* pattern_buffer = new char[DEFAULT_PATTERN_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1];
    int64_t file_buffer_len = 0;
    // int64_t train_data_len = 0;
    int64_t pattern_buffer_len = 0;

    // generate random dataset
    file_buffer_len = generateRandomDataWithPattern(
        file_buffer, DEFAULT_DATASET_SIZE * (MAX_RP_LEN + MAX_RFS_LEN) + 1, DEFAULT_DATASET_SIZE,
        MIN_RP_LEN, MAX_RP_LEN, DEFAULT_PATTERN_SIZE, MIN_RFS_LEN, MAX_RFS_LEN);
    std::string raw_data = file_buffer;
    // pattern training
    PBC::PBC_Train* t = new PBC::PBC_Train();
    t->PBC::PBC_Train::LoadData(file_buffer, file_buffer_len);
    pattern_buffer_len = t->PBC::PBC_Train::TrainPattern(DEFAULT_PATTERN_SIZE, pattern_buffer);
    // compression test
    PBC::PBC_Compress* c = new PBC::PBC_Compress();
    c->PBC::PBC_Compress::readData(pattern_buffer, pattern_buffer_len);
    // test readData
    EXPECT_EQ(c->PBC::PBC_Compress::pattern_num, DEFAULT_PATTERN_SIZE);

    std::vector<std::string> raw_data_vec = splitString(raw_data, "\n");
    char* compressed_data = new char[MAX_RECORD_SIZE];
    char* decompressed_data = new char[MAX_RECORD_SIZE];
    int sum_compressed_len = 0, sum_raw_len = 0;
    int unmatch_num = 0, match_num = 0;

    for (auto& test_str : raw_data_vec) {
        sum_raw_len += test_str.length();
        int compressed_len = c->PBC::PBC_Compress::compress_usingPattern(
            const_cast<char*>(test_str.c_str()), test_str.length(), compressed_data);
        if (compressed_len == -1) {
            sum_compressed_len += test_str.length();
            unmatch_num++;
            continue;
        } else if (compressed_len > 0) {
            sum_compressed_len += compressed_len;
            // EXPECT_GT(strlen(compressed_data), 0);
        } else {
            sum_compressed_len -= compressed_len;
            // EXPECT_GT(0, strlen(compressed_data));
        }
        match_num++;

        c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, compressed_len,
                                                      decompressed_data);
        // int decompressed_len = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data,
        // compressed_len, decompressed_data);

        EXPECT_EQ(0, strcmp(test_str.c_str(), decompressed_data))
            << "wrong compression and decompression";
        // EXPECT_EQ(std::string(decompressed_data), test_str) << "wrong compression and
        // decompression";
        std::cout << "Compression ratio: " << static_cast<double>(sum_compressed_len) / sum_raw_len
                  << std::endl;
    }
}
