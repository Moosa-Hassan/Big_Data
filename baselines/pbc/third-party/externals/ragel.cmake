# Copyright (c) 2020 Ant Group. All rights reserved.

set(name ragel)
set(source_dir ${CMAKE_CURRENT_BINARY_DIR}/${name}/source)
set(MEMKIND_PREFIX "")
ExternalProject_Add(
    ${name}
    URL https://github.com/adrian-thurston/ragel/archive/refs/tags/7.0.4.tar.gz
    URL_HASH MD5=04bfa8473ea5a8bbab3d607a07103aea
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/${name}
    TMP_DIR ${BUILD_INFO_DIR}
    STAMP_DIR ${BUILD_INFO_DIR}
    DOWNLOAD_DIR ${DOWNLOAD_DIR}
    SOURCE_DIR ${source_dir}
    CONFIGURE_COMMAND
        ./autogen.sh
    COMMAND
        ${common_configure_envs}
		./configure --with-colm=${CMAKE_INSTALL_PREFIX} --disable-manual ${common_configure_args}
    BUILD_IN_SOURCE 1
    INSTALL_COMMAND make -s install -j${BUILDING_JOBS_NUM}
    LOG_CONFIGURE TRUE
    LOG_BUILD TRUE
    LOG_INSTALL TRUE
    DOWNLOAD_NO_PROGRESS 1
)

ExternalProject_Add_Step(${name} clean
    EXCLUDE_FROM_MAIN TRUE
    ALWAYS TRUE
    DEPENDEES configure
    COMMAND make clean -j
    COMMAND rm -f ${BUILD_INFO_DIR}/${name}-build
    WORKING_DIRECTORY ${source_dir}
)

ExternalProject_Add_StepTargets(${name} clean)
