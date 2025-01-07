#ifndef RLE_H_
#define RLE_H_

#include <iostream>
#include <string>
#include <boost/dynamic_bitset.hpp>
#include <vector>
#include <immintrin.h>

#include "common/constants.h"

namespace XORC
{

    void runLengthEncodeString(const std::string &input, boost::dynamic_bitset<> &encoded_bitset, boost::dynamic_bitset<> &isRLE_bitset, const std::string &original_data);

}

#endif