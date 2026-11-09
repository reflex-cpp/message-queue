// Part of the message-queue project.
// Copyright © message-queue Project Contributors.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include <spdlog/fwd.h>

namespace message_queue {

class MessageQueue {
public:
    struct message_s {
        struct id_s {
            std::uint32_t client_id = 0;
            std::uint32_t msg_id;

            bool operator==(const id_s &) const = default;
        };
        using id_t = id_s;
        using data_t = std::vector<std::byte>;
        using data_span_t = std::span<const std::byte>;
        data_t data;

        message_s() = default;
        message_s(const data_span_t &source) { data.assign(source.begin(), source.end()); }
        message_s(data_t source) : data(std::move(source)) {}

        static message_s from_str(id_t id, std::string_view str) {
            const data_span_t str_data{reinterpret_cast<const std::byte *>(str.data()), str.size()};
            return message_s(id, std::span{&str_data, 1});
        }

        message_s(id_t id, const std::span<const data_span_t> &datas);

        std::vector<data_span_t> get_datas() const;

        id_t get_id() const;
        void set_client_id(std::uint32_t client_id);

        std::string_view get_str() const {
            const auto first_span = get_datas().at(0);
            return std::string_view(reinterpret_cast<const char *>(first_span.data()), first_span.size());
        }

        void add_data_block(const data_span_t &source) {
            std::copy(source.begin(), source.end(), std::back_inserter(data));
        }
    };

    struct shared_memory_config_s {
        std::size_t max_num_msg_blocks = 100;
        std::size_t msg_block_size = 1 * 1024 * 1024;
        std::uint32_t id = 0;
    };

    enum class event_e {
        server_started,
        client_started,
        server_stopped,
        client_stopped,
        client_connected,
        client_cant_connect,
        client_disconnected,
        shared_memory_created,
    };

    using on_message_received_t = std::function<void(message_s msg)>;
    using on_event_received_t = std::function<void(event_e event)>;

    MessageQueue() = default;

    virtual ~MessageQueue() = default;

    virtual void start_server(std::uint16_t port) = 0;
    virtual void start_client(std::string_view host,
                              std::uint16_t port,
                              std::optional<shared_memory_config_s> shared_memory_config = std::nullopt) = 0;

    virtual void stop() = 0;
    virtual std::uint16_t port() = 0;

    virtual void set_message_handler(on_message_received_t on_message_received) = 0;
    virtual void set_event_handler(on_event_received_t on_event_received) = 0;

    virtual void broadcast_message(message_s msg) = 0;
    virtual void send_message(message_s msg) = 0;

    static std::unique_ptr<MessageQueue> create_socket_message_queue(std::shared_ptr<spdlog::logger> logger);
    static std::pair<std::unique_ptr<MessageQueue>, std::unique_ptr<MessageQueue>> create_local_message_queues(
            std::shared_ptr<spdlog::logger> logger);
};

} // namespace message_queue
