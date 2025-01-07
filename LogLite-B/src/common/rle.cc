#include "rle.h"

namespace XORC
{
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
        if (i_len >= RLE_COUNT / 8 + 1)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    static void encoder(boost::dynamic_bitset<> &encoded_bitset, size_t &length_encoded_bitset,
                        boost::dynamic_bitset<> &isRLE_bitset, size_t &length_isRLE_bitset,
                        bool isRLE, int &i, int i_len, const std::string &original_data)
    {

        if (isRLE)
        {
            isRLE_bitset[length_isRLE_bitset++] = 0;

            for (int j = 0; j < RLE_COUNT; ++j)
            {
                encoded_bitset[length_encoded_bitset++] = (i_len >> j) & 1;
            }
            i = i + i_len;
        }
        else
        {
            isRLE_bitset[length_isRLE_bitset++] = 1;

            for (int j = 0; j < 8; ++j)
            {
                encoded_bitset[length_encoded_bitset++] = (original_data[i] >> j) & 1;
            }
            ++i;
        }
    }

    void runLengthEncodeString(const std::string &input, boost::dynamic_bitset<> &encoded_bitset, boost::dynamic_bitset<> &isRLE_bitset, const std::string &original_data)
    {
        size_t length_encoded_bitset = 0;
        size_t length_isRLE_bitset = 0;

        int i = 0;
        int i_len;
        bool isRLE;
        while (i < input.size())
        {
            if (input[i] == '\0')
            {
                i_len = 1;
                isRLE = isContinuous(input, i, i_len);
                encoder(encoded_bitset, length_encoded_bitset, isRLE_bitset, length_isRLE_bitset, isRLE, i, i_len, original_data);
            }
            else
            {
                i_len = 0;
                encoder(encoded_bitset, length_encoded_bitset, isRLE_bitset, length_isRLE_bitset, false, i, i_len, original_data);
            }
        }

        encoded_bitset.resize(length_encoded_bitset);
        isRLE_bitset.resize(length_isRLE_bitset);
    }
}