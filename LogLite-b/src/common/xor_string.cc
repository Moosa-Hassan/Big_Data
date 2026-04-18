#include "xor_string.h"

namespace XORC
{
    std::string bitwiseXor(const std::string &a, const std::string &b)
    {
        std::string result;
        result.resize(a.length());
        for (size_t i = 0; i < a.length(); ++i)
            result[i] = a[i] ^ b[i];
        return result;
    }

    void bitwiseXor(const std::string &a, const std::string &b, std::string &result)
    {
        for (size_t i = 0; i < a.length(); ++i)
            result[i] = a[i] ^ b[i];
    }
}
