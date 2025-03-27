
#ifndef SRC_BASE_MEMCPY_H_
#define SRC_BASE_MEMCPY_H_

#ifdef PBC_ENABLE_FAST_MEMCPY

#include "deps/memcpy/FastMemcpy.h"
#define pbc_memcpy(src,dst,size) memcpy_fast_sse(src,dst,size)

#else

#define pbc_memcpy(src,dst,size) memcpy(src,dst,size)

#endif

#endif // SRC_BASE_MEMCPY_H_
