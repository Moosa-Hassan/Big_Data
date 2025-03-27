#include <stdio.h>     // printf
#include <stdlib.h>    // free
#include <string.h>    // memset, strcat
#include <sys/time.h>

#include "zstd.h"      // presumes zstd library is installed
#include "zdict.h"      // presumes zstd library is installed
#include "common.h"    // Helper functions, CHECK(), and CHECK_ZSTD()
#include "zmalloc.h"
#include <zlib.h>

long long raw_size, compressed_size;


static void string_compress(const char* fstr, char* ostr, const ZSTD_CDict* cdict)
{
    size_t fSize = strlen(fstr);
    // void* const fBuff = mallocAndLoadFile_orDie(fname, &fSize);
    // size_t const cBuffSize = ZSTD_compressBound(fSize);
    // void* const cBuff = malloc_orDie(cBuffSize);
    void* const fBuff = fstr;
    size_t const cBuffSize = ZSTD_compressBound(fSize);
    void* const cBuff = malloc_orDie(cBuffSize);

    /* Compress using the dictionary.
     * This function writes the dictionary id, and content size into the header.
     * But, it doesn't use a checksum. You can control these options using the
     * advanced API: ZSTD_CCtx_setParameter(), ZSTD_CCtx_refCDict(),
     * and ZSTD_compress2().
     */
    ZSTD_CCtx* const cctx = ZSTD_createCCtx();
    CHECK(cctx != NULL, "ZSTD_createCCtx() failed!");
    size_t const cSize = ZSTD_compress_usingCDict(cctx, cBuff, cBuffSize, fBuff, fSize, cdict);
    CHECK_ZSTD(cSize);

    // saveFile_orDie(oname, cBuff, cSize);
    // printf("%s \n", cBuff);
    ostr = cBuff;

    /* success */
    // printf("compression success: %6u -> %7u \n", (unsigned)fSize, (unsigned)cSize);
    raw_size += fSize;
    compressed_size += cSize;

    ZSTD_freeCCtx(cctx);   /* never fails */
    free(fBuff);
    free(cBuff);
}

/* createDict() :
** `dictFileName` is supposed already created using `zstd --train` */
static ZSTD_CDict* createCDict_orDie(const char* dictFileName, int cLevel)
{
    size_t dictSize;
    printf("loading dictionary %s \n", dictFileName);
    void* const dictBuffer = mallocAndLoadFile_orDie(dictFileName, &dictSize);
    ZSTD_CDict* const cdict = ZSTD_createCDict(dictBuffer, dictSize, cLevel);
    CHECK(cdict != NULL, "ZSTD_createCDict() failed!");
    free(dictBuffer);
    return cdict;
}

static ZSTD_DDict* createDict_orDie(const char* dictFileName)
{
    size_t dictSize;
    printf("loading dictionary %s \n", dictFileName);
    void* const dictBuffer = mallocAndLoadFile_orDie(dictFileName, &dictSize);
    ZSTD_DDict* const ddict = ZSTD_createDDict(dictBuffer, dictSize);
    CHECK(ddict != NULL, "ZSTD_createDDict() failed!");
    free(dictBuffer);
    return ddict;
}


static char* createOutFilename_orDie(const char* filename)
{
    size_t const inL = strlen(filename);
    size_t const outL = inL + 5;
    void* outSpace = malloc_orDie(outL);
    memset(outSpace, 0, outL);
    strcat(outSpace, filename);
    strcat(outSpace, ".zst");
    return (char*)outSpace;
}
static char* createDictFilename_orDie(const char* filename)
{
    size_t const inL = strlen(filename);
    size_t const outL = inL + 6;
    void* outSpace = malloc_orDie(outL);
    memset(outSpace, 0, outL);
    // strcat(outSpace, "../datasets_dict/");
    strcat(outSpace, filename);
    strcat(outSpace, ".dict");
    return (char*)outSpace;
}

typedef struct dataInfo {
    void *buffer;
    size_t total_size;
    size_t data_num;
    /* current data size, decompressed by gzip */
    size_t *raw_data_sizes;
    size_t *ini_data_sizes;
} dataInfo;

typedef struct trainDictCtx {
    dataInfo train_data_info;
    dataInfo validate_data_info;

    unsigned start_d;
    unsigned end_d;
    unsigned d_step;
    unsigned start_k;
    unsigned end_k;
    unsigned k_step;
    void *best_dict_buffer;
    size_t best_dict_size;
    double best_rate;
} trainDictCtx;




// void *trainDictThreadFunc(void *arg) {
//     #define MAX_DICT_SIZE (1024 * 1024)
//     #define DEFAULT_DICT_SIZE (110 * 1024)
//     size_t target_dict_size = DEFAULT_DICT_SIZE;

//     train_data_info_buffer = zmalloc(train_data_info.total_size);
//     train_data_info_raw_data_sizes = zmalloc(size * sizeof(size_t));


//     ZSTD_CCtx *cctx = ZSTD_createCCtx();
//     trainDictCtx *ctx = (trainDictCtx*)arg;
//     void *dict_buffer = zmalloc(target_dict_size);
//     size_t dict_size;
//     ctx->best_rate = 1.0;

//     ZDICT_fastCover_params_t params;
//     memset(&params, 0, sizeof(params));
//     params.zParams.compressionLevel = 3;
//     for (unsigned d = ctx->start_d; d <= ctx->end_d; d += ctx->d_step) {
//         params.d = d;
//         for(unsigned k = ctx->start_k; k < ctx->end_k; k += ctx->k_step) {
//             params.k = k;
//             dict_size = ZDICT_trainFromBuffer_fastCover(dict_buffer, target_dict_size,
//                                                         ctx->train_data_info.buffer, ctx->train_data_info.raw_data_sizes,
//                                                         ctx->train_data_info.data_num, params);
//             if (ZDICT_isError(dict_size)) {
//                 fprintf(stderr, "create dict failed, params.d = %u, params.k = %u, "
//                                 "error message: %s\n", d, k, ZDICT_getErrorName(dict_size));
//                 continue;
//             }
//             ZSTD_CDict *cdict = ZSTD_createCDict(dict_buffer, dict_size, 1);
//             assert(cdict);
//             compressStat stat = getCompressDictStats(cctx, cdict, ctx->validate_data_info);
//             double cur_rate = (double) stat.total_zstd_compress_size / stat.total_ori_size;
//             if (config.detail) {
//                 printf("params.d = %u, params.k = %u, compress rate is %.3f\n", d, k, cur_rate);
//             }
//             if (cur_rate < ctx->best_rate) {
//                 memcpy(ctx->best_dict_buffer, dict_buffer, dict_size);
//                 ctx->best_dict_size = dict_size;
//                 ctx->best_rate = cur_rate;
//             }
//             ZSTD_freeCDict(cdict);
//         }
//     }
//     zfree(dict_buffer);
//     return NULL;
// }

// typedef struct {
//     unsigned k;                  /* Segment size : constraint: 0 < k : Reasonable range [16, 2048+] */
//     unsigned d;                  /* dmer size : constraint: 0 < d <= k : Reasonable range [6, 16] */
//     unsigned f;                  /* log of size of frequency array : constraint: 0 < f <= 31 : 1 means default(20)*/
//     unsigned steps;              /* Number of steps : Only used for optimization : 0 means default (40) : Higher means more parameters checked */
//     unsigned nbThreads;          /* Number of threads : constraint: 0 < nbThreads : 1 means single-threaded : Only used for optimization : Ignored if ZSTD_MULTITHREAD is not defined */
//     double splitPoint;           /* Percentage of samples used for training: Only used for optimization : the first nbSamples * splitPoint samples will be used to training, the last nbSamples * (1 - splitPoint) samples will be used for testing, 0 means default (0.75), 1.0 when all samples are used for both training and testing */
//     unsigned accel;              /* Acceleration level: constraint: 0 < accel <= 10, higher means faster and less accurate, 0 means default(1) */
//     unsigned shrinkDict;         /* Train dictionaries to shrink in size starting from the minimum size and selects the smallest dictionary that is shrinkDictMaxRegression% worse than the largest dictionary. 0 means no shrinking and 1 means shrinking  */
//     unsigned shrinkDictMaxRegression; /* Sets shrinkDictMaxRegression so that a smaller dictionary can be at worse shrinkDictMaxRegression% worse than the max dict size dictionary. */

//     ZDICT_params_t zParams;
// } ZDICT_fastCover_params_t;


int main(int argc, const char** argv)
{
    const char* const exeName = argv[0];
    int cLevel = 1;

    if (argc<2) {
        fprintf(stderr, "wrong arguments\n");
        fprintf(stderr, "usage:\n");
        fprintf(stderr, "%s [FILES] \n", exeName);
        return 1;
    }
    if (argc > 2) {
        cLevel = atoi(argv[2]);
    }

    // const char* const dictName = argv[argc-1];
    // ZSTD_CDict* const cdictPtr = createCDict_orDie(dictName, cLevel);
    // ZSTD_DDict* const ddictPtr = createDict_orDie(dictName);
    const char* inFilename = argv[1];
    char* const DictFilename = createDictFilename_orDie(inFilename);
    FILE * pFile;
    pFile = fopen (inFilename , "r");
    if (pFile == NULL)
        perror ("Error opening file");


    printf("loading file to string[] \n");
    char **strings = NULL;
    char str[1024], cstr[1024];
    size_t size = 0, size_in_byte = 0;
    int MULTIPLE = 10;

    while (fgets(str, sizeof str, pFile))
    {
        if ((size % MULTIPLE) == 0)
        {
            char **temp = realloc(strings, sizeof *strings * (size + MULTIPLE));

            if (temp == NULL)
            {
                perror("realloc");
                exit(EXIT_FAILURE);
            }
            strings = temp;
        }
        strings[size] = malloc(strlen(str) + 1);
        if (strings[size] == NULL)
        {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        strcpy(strings[size], str);
        size_in_byte += (strlen(str) );
        size++;
    }
    printf("string loaded \n");

    #define MAX_DICT_SIZE (1024 * 1024)
    #define DEFAULT_DICT_SIZE (110 * 1024)
    size_t target_dict_size = DEFAULT_DICT_SIZE;

    // train_data_info_buffer = zmalloc(train_data_info.total_size);
    // size_t *train_data_info_raw_data_sizes = malloc(size * sizeof(size_t));
    void *dict_buffer = malloc(target_dict_size);

    // ZDICT_fastCover_params_t params;
    // memset(&params, 0, sizeof(params));
    // ZDICT_fastCover_params_t fastCoverParams;
    // memset(&fastCoverParams, 0, sizeof(fastCoverParams));
    // fastCoverParams.steps = 8;
    // fastCoverParams.nbThreads = 4;
    // params.compressionLevel = 3;

    void* buffer;
    size_t* samplesSizes;
    printf("%u bytes %u lines \n", (unsigned)size_in_byte, (unsigned)size);
    buffer = (char *)malloc(size_in_byte * sizeof(char));
    samplesSizes = (size_t *)malloc(size * sizeof(size_t));
    printf("cache malloc \n");
    if(size < 10000){
        for (size_t i = 0; i < size; i++)
        {
            strcat(buffer, strings[i]);
            samplesSizes[i] = strlen(strings[i]);
        }
    }
    else{
        for (size_t i = 0; i < size; i+=size/10000)
        {
            strcat(buffer, strings[i]);
            samplesSizes[i] = strlen(strings[i]);
        }      
    }


    printf("start training \n");
    size_t dict_size;
    dict_size = ZDICT_trainFromBuffer(dict_buffer, target_dict_size,
                                                buffer, samplesSizes,
                                                size);

    // dict_size = ZDICT_optimizeTrainFromBuffer_fastCover(dict_buffer, target_dict_size,
    //                                             buffer, samplesSizes,
    //                                             size, fastCoverParams);
                                                
    
    printf("%f\n",dict_size); 
    if (ZDICT_isError(dict_size)) {
        fprintf(stderr, "create dict failed ");
        return 0;
    }
    saveFile_orDie(DictFilename, dict_buffer, dict_size);



    // ZSTD_CDict *cdict = ZSTD_createCDict(dict_buffer, dict_size, 3);
    ZSTD_CDict* const cdict = createCDict_orDie(DictFilename, cLevel);

    raw_size = 0;
    compressed_size = 0;

    struct timeval cstart,cend, dstart, dend;  
    gettimeofday(&cstart, NULL ); 
    for (size_t i = 0; i < size; i++)
    {
        string_compress(strings[i], cstr, cdict);
    }
    gettimeofday(&cend, NULL );  

    printf("comprssion finished \n");

    free(DictFilename);

    ZSTD_freeCDict(cdict);
    // ZSTD_freeCDict(cdictPtr);
    // ZSTD_freeDDict(ddictPtr);
    printf("All %u strings compressed. \n", size);
    double ctimeuse = (cend.tv_sec - cstart.tv_sec) + (cend.tv_usec - cstart.tv_usec)/1000000.0;  //second
    printf("compression time=%f\n",ctimeuse);  

    printf("compression : %6u -> %7u \n", (unsigned)raw_size, (unsigned)compressed_size);
    printf("compression ratio: %f \n", (double)compressed_size/(double)raw_size);
    return 0;
}