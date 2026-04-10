// g++ -std=c++14 -W -Wall -ozstddictblock -g -O3 -DNDEBUG blockzstd_dict.cpp  -I.. -L.. -lfsst -lzstd
// ./zstddictblock blocksize 1 ../../datasets/rmckeys_key
#include "PerfEvent.hpp"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <map>
#include <zstd.h>
#include <zdict.h>
#include "common.h" 

using namespace std;
string cur_file = "sldjfioskl";

static ZSTD_CDict* createCDict_orDie(const char* dictFileName, int cLevel)
{
    size_t dictSize;
    // cout<<cur_file<<endl;
    string a = cur_file+".dict";
    dictFileName = a.data();
    // printf("loading dictionary %s \n", dictFileName);
    void* const dictBuffer = mallocAndLoadFile_orDie(dictFileName, &dictSize);
    ZSTD_CDict* const cdict = ZSTD_createCDict(dictBuffer, dictSize, cLevel);
    CHECK(cdict != NULL, "ZSTD_createCDict() failed!");
    free(dictBuffer);
    return cdict;
}

static ZSTD_DDict* createDict_orDie(const char* dictFileName)
{
    size_t dictSize;
    string a = cur_file+".dict";
    dictFileName = a.data();
    // printf("loading dictionary %s \n", dictFileName);
    void* const dictBuffer = mallocAndLoadFile_orDie(dictFileName, &dictSize);
    ZSTD_DDict* const ddict = ZSTD_createDDict(dictBuffer, dictSize);
    CHECK(ddict != NULL, "ZSTD_createDDict() failed!");
    free(dictBuffer);
    return ddict;
}


/// Base class for all compression tests.
class CompressionRunner {
   public:
   /// Store the compressed corpus. Returns the compressed size
   virtual uint64_t compressCorpus(const vector<string>& data, unsigned long &bareSize, double &bulkTime, double& compressionTime, bool verbose) = 0;
   /// Decompress some selected rows, separated by newlines. The line number are in ascending order. The target buffer is guaranteed to be large enough
   virtual uint64_t decompressRows(vector<char>& target, const vector<unsigned>& lines) = 0;
};

/// No compresssion. Just used for debugging
class NoCompressionRunner : public CompressionRunner {
   private:
   /// The uncompressed data
   vector<string> data;

   public:
   /// Store the compressed corpus. Returns the compressed size
   uint64_t compressCorpus(const vector<string>& data, unsigned long& bareSize, double& bulkTime, double& compressionTime, bool /*verbose*/) override {
      auto startTime = std::chrono::steady_clock::now();
      this->data = data;
      uint64_t result = sizeof(uint32_t);
      for (auto& d : data)
         result += d.length() + sizeof(uint32_t);
      auto stopTime = std::chrono::steady_clock::now();
      bareSize = result;
      bulkTime = compressionTime = std::chrono::duration<double>(stopTime - startTime).count();
      return result;
   }
   /// Decompress some selected rows, separated by newlines. The line number are in ascending order. The target buffer is guaranteed to be large enough
   virtual uint64_t decompressRows(vector<char>& target, const vector<unsigned>& lines) {
      char* writer = target.data();
      for (auto l : lines) {
         auto& s = data[l];
         auto len = s.length();
         memcpy(writer, s.data(), len);
         writer[len] = '\n';
         writer += len + 1;
      }
      return writer - target.data();
   }
};



/// ZSTD compression with a given block size
class ZSTDCompressionRunner : public CompressionRunner {
   private:
   /// An uncompressed block
   struct Block {
      /// The row count
      unsigned rows;
      /// The row offsets
      unsigned offsets[];

      /// Get the string offer
      char* data() { return reinterpret_cast<char*>(offsets + rows); }
   };
   /// A compressed block
   struct CompressedBlock {
      /// The compressed size
      unsigned compressedSize;
      /// The uncompressed size
      unsigned uncompressedSize;
      /// The compressed data
      char data[];
   };
   /// The block size
   unsigned blockSize;
   /// The blocks
   vector<CompressedBlock*> blocks;

   ZSTDCompressionRunner(const ZSTDCompressionRunner&) = delete;
   void operator=(const ZSTDCompressionRunner&) = delete;

   public:
   /// Constructor. Sets the block size to the given number of rows
   explicit ZSTDCompressionRunner(unsigned blockSize) : blockSize(blockSize) {}
   /// Destructor
   ~ZSTDCompressionRunner() {
      for (auto b : blocks)
         free(b);
   }

   /// Store the compressed corpus. Returns the compressed size
   uint64_t compressCorpus(const vector<string>& data, unsigned long &bareSize, double &bulkTime, double& compressionTime, bool verbose) override {
      for (auto b : blocks)
         free(b);
      blocks.clear();

      bulkTime = compressionTime = 0;
      bareSize = 0;
      uint64_t result = 0;
      vector<char> compressionBuffer, blockBuffer;
      for (unsigned blockStart = 0, limit = data.size(); blockStart != limit;) {
         unsigned next = blockStart + blockSize;
         if (next > limit) next = limit;

         // Form a block of rows
         unsigned baseLen = sizeof(Block);
         for (unsigned index = blockStart; index != next; ++index)
            baseLen += data[index].length();
         unsigned len = baseLen + (sizeof(unsigned) * (next - blockStart));
         if (len > blockBuffer.size()) blockBuffer.resize(len);

         auto& block = *reinterpret_cast<Block*>(blockBuffer.data());
         block.rows = next - blockStart;
         unsigned maxLen = len + (len / 8) + 128;
         if (maxLen > compressionBuffer.size()) compressionBuffer.resize(maxLen);

         // just compress strings without the offsets, to measure that, also
         auto firstTime = std::chrono::steady_clock::now();
         bareSize += ZSTD_compress(compressionBuffer.data(), maxLen, block.data(), baseLen, 1);

         auto startTime = std::chrono::steady_clock::now();
         bulkTime += std::chrono::duration<double>(startTime - firstTime).count();

         char* strings = block.data();
         unsigned stringEnd = 0;
         for (unsigned index = blockStart; index != next; ++index) {
            memcpy(strings + stringEnd, data[index].data(), data[index].length());
            stringEnd += data[index].length();
            block.offsets[index - blockStart] = stringEnd;
         }

         // Compress it
         unsigned lz4Len = ZSTD_compress(compressionBuffer.data(), maxLen, blockBuffer.data(), len, 1);
         auto stopTime = std::chrono::steady_clock::now();
         compressionTime += std::chrono::duration<double>(stopTime - startTime).count();

         // And store the compressed data
         result += sizeof(CompressedBlock) + lz4Len;
         auto compressedBlock = static_cast<CompressedBlock*>(malloc(sizeof(CompressedBlock) + lz4Len));
         compressedBlock->compressedSize = lz4Len;
         compressedBlock->uncompressedSize = len;
         memcpy(compressedBlock->data, compressionBuffer.data(), lz4Len);

         blocks.push_back(compressedBlock);
         blockStart = next;
      }
      if (verbose)
         cout << "# compress time: " << compressionTime << endl;
      return result;
   }
   /// Decompress some selected rows, separated by newlines. The line number are in ascending order. The target buffer is guaranteed to be large enough
   virtual uint64_t decompressRows(vector<char>& target, const vector<unsigned>& lines) {
      char* writer = target.data();
      vector<char> decompressionBuffer;
      unsigned currentBlock = 0;
      for (auto l : lines) {
         // Switch block on demand
         if (decompressionBuffer.empty() || (l < (currentBlock * blockSize)) || (l >= ((currentBlock + 1) * blockSize))) {
            currentBlock = l / blockSize;
            auto compressedBlock = blocks[currentBlock];
            if (decompressionBuffer.size() < compressedBlock->uncompressedSize) decompressionBuffer.resize(compressedBlock->uncompressedSize);
            ZSTD_decompress(decompressionBuffer.data(), compressedBlock->uncompressedSize, compressedBlock->data, compressedBlock->compressedSize);
         }

         // Unpack the string
         unsigned localOfs = l - (currentBlock * blockSize);
         auto& block = *reinterpret_cast<Block*>(decompressionBuffer.data());
         auto start = localOfs ? block.offsets[localOfs - 1] : 0;
         auto end = block.offsets[localOfs];
         auto len = end - start;
         memcpy(writer, block.data() + start, len);
         writer[len] = '\n';
         writer += len + 1;
      }
      return writer - target.data();
   }
};


/// ZSTD compression with a given block size
class ZSTDdictCompressionRunner : public CompressionRunner {
   private:
   /// An uncompressed block
   struct Block {
      /// The row count
      unsigned rows;
      /// The row offsets
      unsigned offsets[];

      /// Get the string offer
      char* data() { return reinterpret_cast<char*>(offsets + rows); }
   };
   /// A compressed block
   struct CompressedBlock {
      /// The compressed size
      unsigned compressedSize;
      /// The uncompressed size
      unsigned uncompressedSize;
      /// The compressed data
      char data[];
   };
   /// The block size
   unsigned blockSize;
   /// The blocks
   vector<CompressedBlock*> blocks;

   ZSTDdictCompressionRunner(const ZSTDdictCompressionRunner&) = delete;
   void operator=(const ZSTDdictCompressionRunner&) = delete;

   public:
   /// Constructor. Sets the block size to the given number of rows
   explicit ZSTDdictCompressionRunner(unsigned blockSize) : blockSize(blockSize) {}
   /// Destructor
   ~ZSTDdictCompressionRunner() {
      for (auto b : blocks)
         free(b);
   }

   /// Store the compressed corpus. Returns the compressed size
    uint64_t compressCorpus(const vector<string>& data, unsigned long &bareSize, double &bulkTime, double& compressionTime, bool verbose) override {
        for (auto b : blocks)
            free(b);
        blocks.clear();

        const char* dictName = (cur_file + ".dict").data();
        ZSTD_CDict* const cdictPtr = createCDict_orDie(dictName, 3);

        bulkTime = compressionTime = 0;
        bareSize = 0;
        uint64_t result = 0;
        vector<char> compressionBuffer, blockBuffer;
        for (unsigned blockStart = 0, limit = data.size(); blockStart != limit;) {
            unsigned next = blockStart + blockSize;
            if (next > limit) next = limit;

            // Form a block of rows
            unsigned baseLen = sizeof(Block);
            for (unsigned index = blockStart; index != next; ++index)
            baseLen += data[index].length();
            unsigned len = baseLen + (sizeof(unsigned) * (next - blockStart));
            if (len > blockBuffer.size()) blockBuffer.resize(len);

            auto& block = *reinterpret_cast<Block*>(blockBuffer.data());
            block.rows = next - blockStart;
            unsigned maxLen = len + (len / 8) + 128;
            if (maxLen > compressionBuffer.size()) compressionBuffer.resize(maxLen);

            // just compress strings without the offsets, to measure that, also
            auto firstTime = std::chrono::steady_clock::now();
            ZSTD_CCtx* const cctx = ZSTD_createCCtx();
            bareSize += ZSTD_compress_usingCDict(cctx, compressionBuffer.data(), maxLen, block.data(), baseLen, cdictPtr);

            // bareSize += ZSTD_compress(compressionBuffer.data(), maxLen, block.data(), baseLen, 1);

            auto startTime = std::chrono::steady_clock::now();
            bulkTime += std::chrono::duration<double>(startTime - firstTime).count();

            char* strings = block.data();
            unsigned stringEnd = 0;
            for (unsigned index = blockStart; index != next; ++index) {
            memcpy(strings + stringEnd, data[index].data(), data[index].length());
            stringEnd += data[index].length();
            block.offsets[index - blockStart] = stringEnd;
            }

            // Compress it
            size_t cSize = ZSTD_compress_usingCDict(cctx, compressionBuffer.data(), maxLen, blockBuffer.data(), len, cdictPtr);
            ZSTD_freeCCtx(cctx);
            unsigned lz4Len = cSize;

            // unsigned lz4Len = ZSTD_compress(compressionBuffer.data(), maxLen, blockBuffer.data(), len, 1);
            auto stopTime = std::chrono::steady_clock::now();
            compressionTime += std::chrono::duration<double>(stopTime - startTime).count();

            // And store the compressed data
            result += sizeof(CompressedBlock) + lz4Len;
            auto compressedBlock = static_cast<CompressedBlock*>(malloc(sizeof(CompressedBlock) + lz4Len));
            compressedBlock->compressedSize = lz4Len;
            compressedBlock->uncompressedSize = len;
            memcpy(compressedBlock->data, compressionBuffer.data(), lz4Len);

            blocks.push_back(compressedBlock);
            blockStart = next;
        }
        ZSTD_freeCDict(cdictPtr);
        if (verbose)
            cout << "# compress time: " << compressionTime << endl;
        return result;
   }
   /// Decompress some selected rows, separated by newlines. The line number are in ascending order. The target buffer is guaranteed to be large enough
    virtual uint64_t decompressRows(vector<char>& target, const vector<unsigned>& lines) {
        const char* dictName = (cur_file + ".dict").data();
        ZSTD_DDict* const ddictPtr = createDict_orDie(dictName);

        char* writer = target.data();
        vector<char> decompressionBuffer;
        unsigned currentBlock = 0;
        for (auto l : lines) {
            // Switch block on demand
            if (decompressionBuffer.empty() || (l < (currentBlock * blockSize)) || (l >= ((currentBlock + 1) * blockSize))) {
            currentBlock = l / blockSize;
            auto compressedBlock = blocks[currentBlock];
            if (decompressionBuffer.size() < compressedBlock->uncompressedSize) decompressionBuffer.resize(compressedBlock->uncompressedSize);
            ZSTD_DCtx* const dctx = ZSTD_createDCtx();
            ZSTD_decompress_usingDDict(dctx, decompressionBuffer.data(), compressedBlock->uncompressedSize, compressedBlock->data, compressedBlock->compressedSize, ddictPtr);
            ZSTD_freeDCtx(dctx);
            // ZSTD_decompress(decompressionBuffer.data(), compressedBlock->uncompressedSize, compressedBlock->data, compressedBlock->compressedSize);
            }

            // Unpack the string
            unsigned localOfs = l - (currentBlock * blockSize);
            auto& block = *reinterpret_cast<Block*>(decompressionBuffer.data());
            auto start = localOfs ? block.offsets[localOfs - 1] : 0;
            auto end = block.offsets[localOfs];
            auto len = end - start;
            memcpy(writer, block.data() + start, len);
            writer[len] = '\n';
            writer += len + 1;
        }
        ZSTD_freeDDict(ddictPtr);
        return writer - target.data();
    }
};

static long long getFileSize(string file){
   ifstream ifs(file);
   if (!ifs.is_open())
   {
      return 0;
   }
   ifs.seekg(0, std::ios::end);
   long long len = ifs.tellg();
   ifs.seekg(0, std::ios::beg);

   char *buff = new char[len];

   ifs.read(buff, len);
   delete[]buff;
   return len;
}

static pair<bool, vector<pair<unsigned, double>>> doSelectivityTest(CompressionRunner& runner, const vector<string>& files, bool verbose)
// Test a runner for a given number of files
{
    uint64_t totalSize = 0;
    bool debug = getenv("DEBUG");
    NoCompressionRunner debugRunner;
    map<unsigned, vector<pair<double, unsigned>>> timings;
    constexpr unsigned repeat = 100;
    uint64_t totalRawSize = 0;
    for (auto& file : files) {
        // Read the corpus
        vector<string> corpus;
        uint64_t corpusLen = 0;
        {
            ifstream in(file);
            if (!in.is_open()) {
            cerr << "unable to open " << file << endl;
            return {false, {}};
            }
            string line;
            while (getline(in, line)) {
            corpusLen += line.length() + 1;
            corpus.push_back(move(line));
            if (corpusLen > 7000000) break;
            }
        }
        corpusLen += 4096;

        // Compress it
        double bulkTime, compressionTime;
        unsigned long bareSize;
        totalSize += runner.compressCorpus(corpus, bareSize, bulkTime, compressionTime, verbose);
        if (debug) {
            double ignored;
            debugRunner.compressCorpus(corpus, bareSize, ignored, ignored, false);
        }

        // Prepare row counts
        vector<unsigned> shuffledRows;
        for (unsigned index = 0, limit = corpus.size(); index != limit; ++index)
            shuffledRows.push_back(index);
        {
            // Use an explicit seed to get reproducibility
            mt19937 g(123);
            shuffle(shuffledRows.begin(), shuffledRows.end(), g);
        }

        // Test different selectivities
        vector<char> targetBuffer, debugBuffer;
        targetBuffer.resize(corpusLen);
        if (debug) debugBuffer.resize(corpusLen);
        unsigned sel = 1; 
        auto hits = shuffledRows;
        hits.resize(hits.size() * sel / 100);
        if (hits.empty()) continue;
        sort(hits.begin(), hits.end());

        unsigned len = 0;
        for (unsigned index = 0; index != repeat; ++index)
            len = runner.decompressRows(targetBuffer, hits);

        auto startTime = std::chrono::steady_clock::now();
        len = 0;
        for (unsigned index = 0; index != repeat; ++index)
            len = runner.decompressRows(targetBuffer, hits);
        auto stopTime = std::chrono::steady_clock::now();

        timings[sel].push_back(pair<double, unsigned>(std::chrono::duration<double>(stopTime - startTime).count(), hits.size()));

        if (debug) {
            unsigned len2 = debugRunner.decompressRows(debugBuffer, hits);
            if ((len != len2) || (memcmp(targetBuffer.data(), debugBuffer.data(), len) != 0)) {
            cerr << "result mismatch" << endl;
            return {false, {}};
            }
        }

        totalRawSize += corpusLen;
        // totalRawSize += getFileSize(file);
        // cout << totalRawSize << " " << getFileSize(file) << endl;
        // totalRawSize -= 4096;
   }

   // if (verbose)
      // cout << "# total compress size: " << totalSize << endl;
   double compression_ratio = totalSize/static_cast<double>(totalRawSize);
      // cout << "# total compress ratio: " << compression_ratio << endl;
   vector<pair<unsigned, double>> result;
   for (auto& t : timings) {
      double prod1 = 1, prod2 = 1;
      for (auto e : t.second) {
         prod1 *= e.first;
         prod2 *= (e.second / e.first) * repeat / 1000;
      }
      prod1 = pow(prod1, 1.0 / t.second.size());
      prod2 = pow(prod2, 1.0 / t.second.size());
      // if (verbose)
      cout << t.first << " " << prod1 << " " << prod2 << " " << compression_ratio << endl;
      result.push_back({t.first, prod2});
   }
   return {true, result};
}




int main(int argc, const char* argv[]) {
    if (argc < 3)
        return -1;

    string method = argv[1];
    int blockSize = atoi(argv[2]);
    vector<string> files;
    cur_file = argv[3];
    for (int index = 3; index < argc; ++index) {
        string f = argv[index];
        if (f == "--exclude") {
            auto iter = find(files.begin(), files.end(), argv[++index]);
            if (iter != files.end()) files.erase(iter);
        } else {
            files.push_back(move(f));
        }
    }

    //    if (method == "nocompression") {
    //       NoCompressionRunner runner;
    //       return !doTest(runner, files, true).first;
    //    } else 
    if (method == "blocksize") {
        // cout << "\t"  << "-crate\t"  << "-cMB/s\t"  << "-dMB/s" <<endl;
        for (auto& file : files) {
            string name = file;
            cout << name<< endl;
            int block_threshold = min(getFileSize(file)/10, (long long)16*1024);
            cout << getFileSize(file) << " "<< block_threshold <<endl;
            if (name.rfind('/') != string::npos)
            name = name.substr(name.rfind('/') + 1);
            for(int i = 1; i <= block_threshold; i*=4){
            cout<< i << " " ;
            ZSTDdictCompressionRunner runner(i);
            doSelectivityTest(runner, files, false);
            // cout << endl;
            }
            
        }
    } else {
        cerr << "unknown method " << method << endl;
        return 1;
    }
}
