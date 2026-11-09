# Part of the message-queue project.
# Copyright © message-queue Project Contributors.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.25)

project(message-queue VERSION 1.1.2 LANGUAGES CXX)

option(MESSAGE_QUEUE_BUILD_TESTS "Build tests" ${PROJECT_IS_TOP_LEVEL})
option(MESSAGE_QUEUE_INSTALL_CMAKE_CONFIG "Install CMake Config files" ${PROJECT_IS_TOP_LEVEL})
option(MESSAGE_QUEUE_USE_FETCH_CONTENT "Use FetchContent for internal dependencies" OFF)

if(NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 23)
endif()
set(CMAKE_CXX_EXTENSIONS OFF)
message(STATUS "message-queue: C++ standard: ${CMAKE_CXX_STANDARD}")

if(MESSAGE_QUEUE_USE_FETCH_CONTENT)
    include("${CMAKE_CURRENT_LIST_DIR}/tools/cmake/fetch_dependencies.cmake")
endif()

if(NOT TARGET Boost::headers)
    find_package(Boost REQUIRED COMPONENTS headers CONFIG)
endif()

if(NOT TARGET spdlog::spdlog)
    find_package(spdlog REQUIRED)
endif()

if(NOT TARGET fmt::fmt)
    find_package(fmt REQUIRED)
endif()

include(GNUInstallDirs)
include("${CMAKE_CURRENT_LIST_DIR}/tools/cmake/diagnostics.cmake")

add_library(message_queue STATIC)
add_library(message_queue::message_queue ALIAS message_queue)
target_compile_features(message_queue PUBLIC cxx_std_23)
message_queue_set_diagnostic_options(message_queue)
target_sources(
    message_queue
    PUBLIC FILE_SET HEADERS BASE_DIRS include FILES include/message_queue/message_queue.hpp
    PRIVATE src/message_queue.cpp
)
target_link_libraries(message_queue PUBLIC spdlog::spdlog PRIVATE Boost::headers fmt::fmt)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(RT_LIBRARY rt)
    if(RT_LIBRARY)
        target_link_libraries(message_queue PRIVATE ${RT_LIBRARY})
    endif()
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    set_target_properties(
        message_queue
        PROPERTIES COMPILE_PDB_NAME message_queue COMPILE_PDB_OUTPUT_DIRECTORY "${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}"
    )
endif()

if(MESSAGE_QUEUE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

install(TARGETS message_queue EXPORT message-queueTargets FILE_SET HEADERS DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    install(
        FILES "${CMAKE_BINARY_DIR}/${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/message_queue.pdb"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
endif()
if(MESSAGE_QUEUE_INSTALL_CMAKE_CONFIG)
    install(
        EXPORT message-queueTargets
        FILE message-queueTargets.cmake
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/message-queue"
        NAMESPACE message_queue::
    )
    include(CMakePackageConfigHelpers)
    configure_package_config_file(
        tools/cmake/message-queueConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/message-queueConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/message-queue"
    )
    install(
        FILES "${CMAKE_CURRENT_BINARY_DIR}/message-queueConfig.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/message-queue"
    )
endif()
