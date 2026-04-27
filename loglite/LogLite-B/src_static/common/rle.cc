#include "rle.h"

namespace XORC
{
    // Check whether the sequence starting at position i in the XOR
    // mask has a long enough run of '\0' bytes to justify RLE.
    // i_len is incremented to the actual run length (capped by
    // RLE_POW_COUNT-1), and the function reports whether we should
    // use a zero-run code instead of literal bytes.
    static bool isContinuous(const std::string &input, int i, int &i_len)
    {
        i++;
        while (input[i] == '\0' && i < input.size())
        {
            i_len++;
            i++;
            if (i_len == RLE_POW_COUNT - 1)
            {
                break;
            }
        }
        // Require a minimum run length before switching to RLE
        // so that encoding is actually beneficial.
        if (i_len >= RLE_COUNT / 8 + 1)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    // Emit either a zero run (isRLE == true) or a literal byte (isRLE == false)
    // into the output bitset, updating the current write position and the
    // total number of bits produced for this XOR mask.
    static void encoder(boost::dynamic_bitset<> &output_data,
                        uint64_t &len_output_data,
                        size_t &length_encoded_bitset,
                        bool isRLE,
                        int &i,
                        int i_len,
                        const std::string &original_data)
    {
        if (isRLE)
        {
            // Zero-run: first write tag bit 0, then RLE_COUNT bits
            // that encode how many '\0' bytes we saw starting at i.
            output_data[len_output_data++] = 0;
            ++length_encoded_bitset;
            for (size_t j = 0; j < RLE_COUNT; ++j)
            {
                output_data[len_output_data++] = (i_len >> j) & 1;
                ++length_encoded_bitset;
            }
            i = i + i_len;
        }
        else
        {
            // Literal: first write tag bit 1, then 8 bits of the
            // original byte value from original_data.
            output_data[len_output_data++] = 1;
            ++length_encoded_bitset;
            for (int j = 0; j < 8; ++j)
            {
                output_data[len_output_data++] = (original_data[i] >> j) & 1;
                ++length_encoded_bitset;
            }
            ++i;
        }
    }

    // Walk over the XOR mask and encode it as a mixture of zero runs
    // and literal bytes. This is the only place that knows the exact
    // bit layout that the decoder in Stream_Compress::stream_decompress
    // expects when isRLE == true.
    size_t runLengthEncodeString(const std::string &input,
                                 boost::dynamic_bitset<> &output_data,
                                 uint64_t &len_output_data,
                                 const std::string &original_data)
    {

        const int len_input = input.size();

        size_t length_encoded_bitset = 0;

        int i = 0;
        int i_len;
        bool isRLE;
        while (i < len_input)
        {
            if (input[i] == '\0')
            {
                i_len = 1;
                isRLE = isContinuous(input, i, i_len);
                encoder(output_data, len_output_data, length_encoded_bitset, isRLE, i, i_len, original_data);
            }
            else
            {
                i_len = 0;
                encoder(output_data, len_output_data, length_encoded_bitset, false, i, i_len, original_data);
            }
        }

        return length_encoded_bitset;
    }
}