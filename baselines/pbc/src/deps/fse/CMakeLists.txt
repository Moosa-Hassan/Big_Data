set(CMAKE_CXX_VISIBILITY_PRESET hidden)

FILE(GLOB fse_src *.h *.c
)

add_library(fse STATIC ${fse_src})
SET_TARGET_PROPERTIES(fse PROPERTIES LINKER_LANGUAGE C ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

disable_target_warnings(fse)
