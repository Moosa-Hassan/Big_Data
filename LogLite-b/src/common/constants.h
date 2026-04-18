#ifndef CONSTANTS_H_
#define CONSTANTS_H_

// Parameters that can be adjusted

constexpr int EACH_WINDOW_SIZE_COUNT = 3;  // k
constexpr double Similarity_Threshold = 1 - 0.85;  //default Sitha θ is 0.85

// the following 2 arameters smaller, the compress ratio better (if too small, it can not decompress)
constexpr int STREAM_ENCODER_COUNT = 13;
constexpr int ORIGINAL_LENGTH_COUNT = 15;

// // for large dataset, have to use the following.
// constexpr int STREAM_ENCODER_COUNT = 13 + 8;
// constexpr int ORIGINAL_LENGTH_COUNT = 15 + 8;

// If the log length is larger than this value, the log is not compressed
// it can influence the compression rate of certain data sets
constexpr int MAX_LEN = 10000;

//Reserved memory GB
//Adjust according to the size of the compressed file
constexpr double Reserved_Memory = 33;



//====================================//
//Parameters that should not be adjusted
constexpr int RLE_COUNT = 8;
constexpr int RLE_POW_COUNT = 1 << RLE_COUNT;
constexpr int RLE_SKIM = 8;
constexpr int EACH_WINDOW_SIZE = 1 << EACH_WINDOW_SIZE_COUNT;
constexpr size_t simd_width32 = 32;
constexpr size_t simd_width16 = 16;

#endif // CONSTANTS_H