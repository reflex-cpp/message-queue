# Part of the message-queue project.
# Copyright © message-queue Project Contributors.
# SPDX-License-Identifier: Apache-2.0

message(STATUS "message-queue: using FetchContent")
include(FetchContent)

FetchContent_Declare(
    Catch2
    GIT_REPOSITORY https://github.com/catchorg/Catch2.git
    GIT_TAG
        b3fb4b9feafcd8d91c5cb510a4775143fdbef02f # v3.11.0
    OVERRIDE_FIND_PACKAGE
)
