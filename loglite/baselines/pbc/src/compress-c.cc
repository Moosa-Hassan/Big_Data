

#include "compress-c.h"  // NOLINT

#include "compress/compress.h"
#include "train/pbc_train.h"

using PBC::PBC_Compress;
using PBC::PBC_Train;

void* PBC_createCompressCtx() { return new PBC_Compress(); }

void PBC_setPattern(void* pbc_ctx, char* pattern_buffer, size_t pattern_buffer_len) {
    PBC_Compress* pbc = reinterpret_cast<PBC_Compress*>(pbc_ctx);
    pbc->readData(pattern_buffer, pattern_buffer_len);
}

int PBC_compressUsingPattern(void* pbc_ctx, char* data, size_t data_len, char* compress_buffer) {
    PBC_Compress* pbc = reinterpret_cast<PBC_Compress*>(pbc_ctx);
    return pbc->compress_usingPattern(data, data_len, compress_buffer);
}

int PBC_decompressUsingPattern(void* pbc_ctx, char* compress_data, size_t compress_data_len,
                               char* data_buffer) {
    PBC_Compress* pbc = reinterpret_cast<PBC_Compress*>(pbc_ctx);
    return pbc->decompress_usingPattern(compress_data, compress_data_len, data_buffer);
}

int PBC_getCtxPatternNum(const void* pbc_ctx) {
    const PBC_Compress* pbc = reinterpret_cast<const PBC_Compress*>(pbc_ctx);
    return pbc->pattern_num;
}

void* PBC_createTrainCtx() { return new PBC_Train(); }

void PBC_loadPbcTrainData(void* pbc_ctx, char* file_buffer_train, size_t file_buffer_len) {
    PBC_Train* pbc = reinterpret_cast<PBC_Train*>(pbc_ctx);
    pbc->LoadData(file_buffer_train, file_buffer_len);
}

size_t PBC_trainPattern(void* pbc_ctx, int pattern_size, char* pattern_buffer) {
    PBC_Train* pbc = reinterpret_cast<PBC_Train*>(pbc_ctx);
    return pbc->TrainPattern(pattern_size, pattern_buffer);
}
