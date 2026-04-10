
#ifndef SRC_COMPRESS_C_H_
#define SRC_COMPRESS_C_H_

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

void* PBC_createCompressCtx();  // new_PBC

void PBC_setPattern(const void* pbc_ctx, char* pattern, size_t pattern_buffer_len);  // setPattern

int PBC_compressUsingPattern(const void* pbc_ctx, char* data, size_t data_len,
                             char* compress_buffer);  // PBC_compress_usingPattern

int PBC_decompressUsingPattern(const void* pbc_ctx, char* compress_data, size_t compress_data_len,
                               char* data_buffer);  // PBC_decompress_usingPattern

int PBC_getCtxPatternNum(const void* pbc_ctx);  // get_Pattern_num

void* PBC_createTrainCtx();  // new_PBC_Train

void PBC_loadPbcTrainData(const void* pbc_ctx, char* data_buffer, size_t len);  // load_data

size_t PBC_trainPattern(const void* pbc_ctx, int k, char* pattern_buffer);  // train_pattern

#ifdef __cplusplus
}
#endif
#endif  // SRC_COMPRESS_C_H_
