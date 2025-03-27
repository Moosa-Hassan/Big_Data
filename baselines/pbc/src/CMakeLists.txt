# FILE(GLOB_RECURSE compress_lib compress/*.cpp)
# add_library(pbc STATIC ${compress_lib})
add_subdirectory(deps)

FILE(GLOB pbc_src
  compress/*.h
  compress/*.cc
  train/*.h
  train/*.cc
  compress-c.h
  compress-c.cc
)

ADD_LIBRARY(pbc STATIC ${pbc_src})
ADD_DEPENDENCIES(pbc fse pbc_fsst)
SET_TARGET_PROPERTIES(${PROJECT_NAME} PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

FILE(GLOB pbc-cli_src
  tools/pbc-cli.cc
)

ADD_EXECUTABLE(pbc-cli ${pbc-cli_src})
TARGET_LINK_LIBRARIES(pbc-cli -Wl,--start-group
  pbc
  fse
  ${LIBRARIES} -Wl,--end-group
)

FILE(GLOB_RECURSE pbc_test_src unit_test/test_main.cc unit_test/test_pbc.cc)

# remove # after c-interface test added.
# FILE(GLOB pbc_c_test unit_test/test_pbc_c.c)
# ADD_EXECUTABLE(pbc_c_test ${pbc_c_test})
# ADD_DEPENDENCIES(pbc_c_test pbc)
# TARGET_LINK_LIBRARIES(pbc_c_test -Wl,--start-group
#   pbc
#   fse
#   gtest
#   gflags
#   pthread
#   ${LIBRARIES} 
#   stdc++
#   -Wl,--end-group
# )
# SET_TARGET_PROPERTIES(pbc_c_test PROPERTIES LINKER_LANGUAGE C)



ADD_EXECUTABLE(pbc_test ${pbc_test_src})
ADD_DEPENDENCIES(pbc_test pbc)
TARGET_LINK_LIBRARIES(pbc_test -Wl,--start-group
  pbc
  fse
  gtest
  gflags
  pthread
  ${LIBRARIES} -Wl,--end-group
)
ADD_TEST (NAME pbc_test COMMAND pbc_test --dataset_path=${CMAKE_SOURCE_DIR}/dataset/)


# FILE(GLOB_RECURSE pbc_test_src unit_test/pbc_gtest.cc)
# ADD_EXECUTABLE(pbc_test ${pbc_test_src})
# ADD_DEPENDENCIES(pbc_test pbc)

# TARGET_LINK_LIBRARIES(pbc_test -Wl,--start-group
#   pbc
#   fse
#   gtest
#   pthread
#   ${LIBRARIES} -Wl,--end-group
# )
# ADD_TEST (NAME pbc_test COMMAND pbc_test)
