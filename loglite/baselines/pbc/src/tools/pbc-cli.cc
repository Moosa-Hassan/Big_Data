
#include <ctime>
#include <iostream>
#include <vector>

#include "compress/compress.h"
#include "train/pbc_train.h"

using namespace std;

constexpr int MAXE_SIZE = (1024 * 1024 * 1024);
constexpr int MAX_PATTERN_SIZE = (1024 * 1024);
constexpr int MAX_RECORD_SIZE = 1024 * 10;
constexpr int DEFAULT_PATTERN_SIZE = 100;

static struct config {
    int train_pattern;
    int test_compress;
    int test_compress_fse;
    int is_output;
    int decompress_gzip;
    size_t target_pattern_size;
    int train_data_number;
    char* file_path;
    size_t file_num;
    char* pattern_path;
    char* compressed_file_path;
} config;

static void parseOptions(int argc, const char** argv) {
    for (int i = 1; i < argc; i++) {
        int lastarg = (i == argc - 1);

        if (!strcmp(argv[i], "--train-pattern")) {
            config.train_pattern = 1;
        } else if (!strcmp(argv[i], "--test-compress")) {
            config.test_compress = 1;
        } else if (!strcmp(argv[i], "--test-compress-fse")) {
            config.test_compress_fse = 1;
        } else if (!strcmp(argv[i], "--file-path") && !lastarg) {
            config.file_path = const_cast<char*>(argv[++i]);
        } else if (!strcmp(argv[i], "--pattern-path") && !lastarg) {
            config.pattern_path = const_cast<char*>(argv[++i]);
        } else if (!strcmp(argv[i], "--pattern-size") && !lastarg) {
            config.target_pattern_size = atoi(argv[++i]);
            if (config.target_pattern_size > MAX_PATTERN_SIZE) {
                fprintf(stderr, "dict size overflow");
                exit(1);
            }
        } else if (!strcmp(argv[i], "--train-value-number") && !lastarg) {
            config.train_data_number = atoi(argv[++i]);
            // if (config.train_data_number < 0 || config.train_data_number > 100) {
            //     fprintf(stderr, "train-value-proportion %d illegal", config.train_data_number);
            //     exit(1);
            // }
        } else if (!strcmp(argv[i], "--compressed-file-path")) {
            config.is_output = 1;
            config.compressed_file_path = const_cast<char*>(argv[++i]);
        }
    }
}

static int64_t readFile(char* file_path, char* buffer, int64_t buffer_len) {
    struct stat statbuf;
    if (stat(file_path, &statbuf) != 0) {
        std::cout << "The input file does not exist: " << file_path << std::endl;
        exit(1);
    }

    int fd = open(file_path, O_RDONLY);
    char* temp =
        reinterpret_cast<char*>(mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0));

    if (buffer_len < statbuf.st_size) {
        std::cout << "The buffer size is too small." << std::endl;
        exit(1);
    }
    memcpy(buffer, temp, statbuf.st_size);

    return statbuf.st_size;
}

static int64_t readFile_(char* file_path, char* buffer_test, int64_t buffer_test_origin_len,
                         int64_t& buffer_test_len, char* buffer_train,
                         int64_t buffer_origin_train_len, int64_t& buffer_train_len,
                         int train_pattern_num) {
    struct stat statbuf;
    int64_t buffer_i = 0;
    int train_num = 1, data_num = 0;
    int gap;
    buffer_train_len = 0;

    if (stat(file_path, &statbuf) != 0) {
        std::cout << "The input file does not exist: " << file_path << std::endl;
        exit(1);
    }

    int fd = open(file_path, O_RDONLY);
    char* temp =
        reinterpret_cast<char*>(mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_PRIVATE, fd, 0));

    if (buffer_test_origin_len < statbuf.st_size) {
        std::cout << "The buffer size is too small." << std::endl;
        exit(1);
    }

    int data_num2 = 0;

    while (buffer_i < statbuf.st_size) {
        if (temp[buffer_i] == '\n') data_num2++;
        buffer_i++;
    }

    gap = data_num2 / train_pattern_num;
    if (gap < 1) gap = 1;

    buffer_i = 0;
    memcpy(buffer_test, temp, statbuf.st_size);
    buffer_test_len = statbuf.st_size;

    while (buffer_i < statbuf.st_size && temp[buffer_i] != '\n') {
        buffer_train[buffer_train_len++] = temp[buffer_i];
        buffer_i++;
    }
    buffer_train[buffer_train_len++] = '\n';

    while (buffer_i < statbuf.st_size) {
        if (temp[buffer_i] == '\n') {
            data_num++;
            buffer_i++;
            if (data_num % gap == 0) {
                while (buffer_i < statbuf.st_size && temp[buffer_i] != '\n') {
                    buffer_train[buffer_train_len++] = temp[buffer_i];
                    buffer_i++;
                }
                buffer_train[buffer_train_len++] = '\n';
                train_num++;
            }

        } else {
            buffer_i++;
        }
    }

    return statbuf.st_size;
}

static void writeFile(char* file_path, char* buffer, int64_t buffer_len) {
    std::ofstream outFile;
    outFile.open(file_path, std::ios::out);
    if (!outFile) {
        std::cout << "The output file path does not exist." << std::endl;
    }

    outFile.write(buffer, buffer_len);
    outFile.close();
}

int main(int argc, const char* argv[]) {
    config.train_pattern = 0;
    config.test_compress = 0;
    config.test_compress_fse = 0;
    config.is_output = 0;
    config.file_num = 1;
    config.target_pattern_size = DEFAULT_PATTERN_SIZE;
    config.train_data_number = 1000;

    parseOptions(argc, argv);

    if (config.train_pattern) {
        std::cout << "train pattern" << std::endl;

        char* file_buffer_test = new char[MAXE_SIZE];
        char* file_buffer_train = new char[MAXE_SIZE];
        char* pattern_buffer = new char[MAX_PATTERN_SIZE];
        int64_t file_buffer_test_len = 0;
        int64_t file_buffer_train_len = 0;
        int64_t pattern_buffer_len = 0;

        readFile_(config.file_path, file_buffer_test, MAXE_SIZE, file_buffer_test_len,
                  file_buffer_train, MAXE_SIZE, file_buffer_train_len, config.train_data_number);

        PBC::PBC_Train* t = new PBC::PBC_Train();
        t->PBC::PBC_Train::LoadData(file_buffer_train, file_buffer_train_len);
        pattern_buffer_len =
            t->PBC::PBC_Train::TrainPattern(config.target_pattern_size, pattern_buffer);
        writeFile(config.pattern_path, pattern_buffer, pattern_buffer_len);
    }

    if (config.test_compress) {
        std::cout << "test compress" << std::endl;
        std::cout << "pattern path:" << config.pattern_path << std::endl;

        char* pattern_buffer = new char[MAX_PATTERN_SIZE];
        char* file_buffer_test = new char[MAXE_SIZE];

        int64_t pattern_buffer_len = 0;
        PBC::PBC_Compress* c = new PBC::PBC_Compress();

        pattern_buffer_len = readFile(config.pattern_path, pattern_buffer, MAX_PATTERN_SIZE);
        int64_t file_buffer_test_len = readFile(config.file_path, file_buffer_test, MAXE_SIZE);

        c->PBC::PBC_Compress::readData(pattern_buffer, pattern_buffer_len);

        char* compressed_data = new char[MAX_RECORD_SIZE];
        char* decompressed_data = new char[MAX_RECORD_SIZE];
        char* single_key = new char[MAX_RECORD_SIZE];
        int64_t single_key_i = 0;
        int64_t file_buffer_test_i = 0;
        int i = 0;
        int compressed_len = 0, raw_len = 0;
        int no_match = 0, match = 0;

        int all_single_num = 0;
        while (file_buffer_test_i < file_buffer_test_len) {
            if (file_buffer_test[file_buffer_test_i] == '\n') all_single_num++;
            file_buffer_test_i++;
        }
        vector<char*> single_keys(all_single_num);
        vector<uint64_t> single_key_is(all_single_num);
        vector<char*> compressed_datas(all_single_num);
        vector<int> compressed_datas_is(all_single_num);
        int single_keys_i = 0, compressed_datas_i = 0;

        file_buffer_test_i = 0;

        while (file_buffer_test_i < file_buffer_test_len &&
               file_buffer_test[file_buffer_test_i] != '\n') {
            single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
            file_buffer_test_i++;
        }
        single_key[single_key_i] = 0;
        file_buffer_test_i++;
        int compressed_datas_l = 0;

        ofstream outFile;

        if (config.is_output) {
            outFile.open(config.compressed_file_path);
        }

        while (file_buffer_test_i < file_buffer_test_len) {
            raw_len += single_key_i;
            int l = c->PBC::PBC_Compress::compress_usingPattern(single_key, single_key_i,
                                                                compressed_data);

            single_keys[single_keys_i] = new char[single_key_i];
            memcpy(single_keys[single_keys_i], single_key, single_key_i);
            single_key_is[single_keys_i++] = single_key_i;

            if (l == -1) {
                compressed_len += single_key_i;
                if (config.is_output) outFile.write(single_key, single_key_i + 1);
                no_match++;
                single_key_i = 0;
                while (file_buffer_test_i < file_buffer_test_len &&
                       file_buffer_test[file_buffer_test_i] != '\n') {
                    single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
                    file_buffer_test_i++;
                }
                single_key[single_key_i] = 0;
                file_buffer_test_i++;
                continue;
            }
            if (l > 0)
                compressed_len += l;
            else
                compressed_len -= l;

            if (l > 0)
                compressed_datas_l = l;
            else
                compressed_datas_l = -1 * l;
            compressed_datas[compressed_datas_i] = new char[compressed_datas_l];
            memcpy(compressed_datas[compressed_datas_i], compressed_data, compressed_datas_l);
            compressed_datas_is[compressed_datas_i++] = l;

            compressed_data[compressed_datas_l] = '\n';

            if (config.is_output) outFile.write(compressed_data, compressed_datas_l + 1);

            match++;
            int l2 = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, l,
                                                                   decompressed_data);

            if (l2 != single_key_i) {
                std::cout << "errrrrrr" << std::endl;
            }

            single_key_i = 0;
            while (file_buffer_test_i < file_buffer_test_len &&
                   file_buffer_test[file_buffer_test_i] != '\n') {
                single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
                file_buffer_test_i++;
            }
            single_key[single_key_i] = 0;
            file_buffer_test_i++;
        }

        if (single_key_i > 0) {
            raw_len += single_key_i;
            int l = c->PBC::PBC_Compress::compress_usingPattern(single_key, single_key_i,
                                                                compressed_data);
            if (l == -1) {
                compressed_len += single_key_i;
                no_match++;
                if (config.is_output) outFile.write(single_key, single_key_i + 1);
            } else {
                if (l > 0)
                    compressed_len += l;
                else
                    compressed_len -= l;
                match++;
                int l2 = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, l,
                                                                       decompressed_data);

                if (l2 != single_key_i) {
                    std::cout << "errrrrrr" << std::endl;
                }
            }

            i++;
        }

        std::cout << "compression rate:"
                  << static_cast<double>(compressed_len) / static_cast<double>(raw_len)
                  << std::endl;

        clock_t start_time, end_time;

        start_time = clock();
        for (int i = 0; i < single_keys_i; i++) {
            c->PBC::PBC_Compress::compress_usingPattern(single_keys[i], single_key_is[i],
                                                        compressed_data);
        }
        end_time = clock();
        cout << "compression speed: "
             << (double)raw_len / (double)1024 / (double)1024 /
                    (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
             << "MB/s" << endl;

        start_time = clock();
        for (int i = 0; i < compressed_datas_i; i++) {
            c->PBC::PBC_Compress::decompress_usingPattern(
                compressed_datas[i], compressed_datas_is[i], decompressed_data);
        }

        end_time = clock();
        cout << "decompression speed: "
             << (double)raw_len / (double)1024 / (double)1024 /
                    (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
             << "MB/s" << endl;

        delete[] compressed_data;
        delete[] decompressed_data;
        if (config.is_output) outFile.close();
    }

    if (config.test_compress_fse) {
        std::cout << "test compress fse" << std::endl;
        std::cout << "pattern path:" << config.pattern_path << std::endl;

        char* pattern_buffer = new char[MAX_PATTERN_SIZE];
        char* file_buffer_test = new char[MAXE_SIZE];

        int64_t pattern_buffer_len = 0;
        PBC::PBC_Compress* c = new PBC::PBC_Compress();

        pattern_buffer_len = readFile(config.pattern_path, pattern_buffer, MAX_PATTERN_SIZE);
        int64_t file_buffer_test_len = readFile(config.file_path, file_buffer_test, MAXE_SIZE);

        c->PBC::PBC_Compress::readData(pattern_buffer, pattern_buffer_len);

        char* compressed_data = new char[MAX_RECORD_SIZE];
        char* decompressed_data = new char[MAX_RECORD_SIZE];
        char* single_key = new char[MAX_RECORD_SIZE];
        int64_t single_key_i = 0;
        int64_t file_buffer_test_i = 0;
        int i = 0;
        int compressed_len = 0, raw_len = 0;
        int no_match = 0, match = 0;

        int all_single_num = 0;
        while (file_buffer_test_i < file_buffer_test_len) {
            if (file_buffer_test[file_buffer_test_i] == '\n') all_single_num++;
            file_buffer_test_i++;
        }
        vector<char*> single_keys(all_single_num);
        vector<uint64_t> single_key_is(all_single_num);
        vector<char*> compressed_datas(all_single_num);
        vector<uint64_t> compressed_datas_is(all_single_num);
        int single_keys_i = 0, compressed_datas_i = 0;

        file_buffer_test_i = 0;

        while (file_buffer_test_i < file_buffer_test_len &&
               file_buffer_test[file_buffer_test_i] != '\n') {
            single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
            file_buffer_test_i++;
        }
        single_key[single_key_i] = 0;
        file_buffer_test_i++;

        ofstream outFile;

        if (config.is_output) {
            outFile.open(string(config.compressed_file_path) + ".fse");
        }

        while (file_buffer_test_i < file_buffer_test_len) {
            raw_len += single_key_i;
            int l = c->PBC::PBC_Compress::compress_usingPattern_fse(single_key, single_key_i,
                                                                    compressed_data);

            single_keys[single_keys_i] = new char[single_key_i];
            memcpy(single_keys[single_keys_i], single_key, single_key_i);
            single_key_is[single_keys_i++] = single_key_i;

            if (l == -1) {
                compressed_len += single_key_i;
                no_match++;
                if (config.is_output) outFile.write(single_key, single_key_i + 1);
                single_key_i = 0;
                while (file_buffer_test_i < file_buffer_test_len &&
                       file_buffer_test[file_buffer_test_i] != '\n') {
                    single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
                    file_buffer_test_i++;
                }
                single_key[single_key_i] = 0;
                file_buffer_test_i++;
                continue;
            }
            if (l > 0)
                compressed_len += l;
            else
                compressed_len -= l;

            int compressed_datas_l;
            if (l > 0)
                compressed_datas_l = l;
            else
                compressed_datas_l = -1 * l;
            // cout<<compressed_datas_l<<" "<<compressed_datas_i<<endl;
            compressed_datas[compressed_datas_i] = new char[compressed_datas_l];
            memcpy(compressed_datas[compressed_datas_i], compressed_data, compressed_datas_l);
            compressed_datas_is[compressed_datas_i++] = l;

            compressed_data[compressed_datas_l] = '\n';

            if (config.is_output) outFile.write(compressed_data, compressed_datas_l + 1);

            match++;
            int l2 = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, l,
                                                                   decompressed_data);

            if (l2 != single_key_i) {
                std::cout << "errrrrrr" << std::endl;
            }

            single_key_i = 0;
            while (file_buffer_test_i < file_buffer_test_len &&
                   file_buffer_test[file_buffer_test_i] != '\n') {
                single_key[single_key_i++] = file_buffer_test[file_buffer_test_i];
                file_buffer_test_i++;
            }
            single_key[single_key_i] = 0;
            file_buffer_test_i++;
        }

        if (single_key_i > 0) {
            raw_len += single_key_i;
            int l = c->PBC::PBC_Compress::compress_usingPattern_fse(single_key, single_key_i,
                                                                    compressed_data);
            if (l == -1) {
                compressed_len += single_key_i;
                no_match++;
                if (config.is_output) outFile.write(single_key, single_key_i + 1);
            } else {
                if (l > 0)
                    compressed_len += l;
                else
                    compressed_len -= l;
                match++;

                int l2 = c->PBC::PBC_Compress::decompress_usingPattern(compressed_data, l,
                                                                       decompressed_data);

                if (l2 != single_key_i) {
                    std::cout << "errrrrrr" << std::endl;
                }
            }

            i++;
        }

        std::cout << "compression rate:"
                  << static_cast<double>(compressed_len) / static_cast<double>(raw_len)
                  << std::endl;

        clock_t start_time, end_time;

        start_time = clock();
        for (int i = 0; i < single_keys_i; i++) {
            c->PBC::PBC_Compress::compress_usingPattern(single_keys[i], single_key_is[i],
                                                        compressed_data);
        }
        end_time = clock();
        cout << "compression speed: "
             << (double)raw_len / (double)1024 / (double)1024 /
                    (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
             << "MB/s" << endl;

        start_time = clock();
        for (int i = 0; i < compressed_datas_i; i++) {
            c->PBC::PBC_Compress::decompress_usingPattern(
                compressed_datas[i], compressed_datas_is[i], decompressed_data);
        }

        end_time = clock();
        cout << "decompression speed: "
             << (double)raw_len / (double)1024 / (double)1024 /
                    (static_cast<double>(end_time - start_time) / CLOCKS_PER_SEC)
             << "MB/s" << endl;

        delete[] compressed_data;
        delete[] decompressed_data;
        if (config.is_output) outFile.close();
    }

    return 0;
}
