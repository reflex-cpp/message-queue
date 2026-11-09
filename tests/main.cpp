// Part of the message-queue project.
// Copyright © message-queue Project Contributors.
// SPDX-License-Identifier: Apache-2.0

#include <catch2/catch_session.hpp>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

int main(const int argc, char **const argv) {
    spdlog::set_default_logger(spdlog::stderr_color_mt("global"));
    spdlog::set_level(spdlog::level::warn);
    return Catch::Session().run(argc, argv);
}
