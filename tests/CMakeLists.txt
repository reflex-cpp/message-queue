# Part of the message-queue project.
# Copyright © message-queue Project Contributors.
# SPDX-License-Identifier: Apache-2.0

find_package(Catch2 REQUIRED)

add_executable(message_queue_tests)
message_queue_set_diagnostic_options(message_queue_tests)
target_link_libraries(message_queue_tests PRIVATE Catch2::Catch2 message_queue::message_queue)
target_sources(message_queue_tests PRIVATE main.cpp socket_message_queue.test.cpp)

include(Catch)
catch_discover_tests(message_queue_tests)
