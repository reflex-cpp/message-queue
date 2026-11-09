# Part of the message-queue project.
# Copyright © message-queue Project Contributors.
# SPDX-License-Identifier: Apache-2.0

function(message_queue_set_diagnostic_options TARGET_NAME)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        target_compile_options(${TARGET_NAME} PRIVATE -W4 -external:W0)
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(COMMON_CLANG_WARNING_FLAGS
            -Wpedantic
            -Wshadow
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Wcast-align
            -Wconversion
            -Wdouble-promotion
            -Wformat=2
            -Wimplicit-fallthrough
        )
        if(CMAKE_CXX_SIMULATE_ID STREQUAL "MSVC")
            target_compile_options(${TARGET_NAME} PRIVATE -W4 ${COMMON_CLANG_WARNING_FLAGS})
        else()
            target_compile_options(${TARGET_NAME} PRIVATE -Wall -Wextra ${COMMON_CLANG_WARNING_FLAGS})
        endif()
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(${TARGET_NAME} PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    if(WIN32)
        target_compile_definitions(${TARGET_NAME} PRIVATE _CRT_SECURE_NO_WARNINGS _SCL_SECURE_NO_WARNINGS)
    endif()
endfunction()
