// Part of the message-queue project.
// Copyright © message-queue Project Contributors.
// SPDX-License-Identifier: Apache-2.0

#include <iostream>
#include <memory>
#include <mutex>
#include <semaphore>
#include <thread>

#include <catch2/catch_test_macros.hpp>
#include <message_queue/message_queue.hpp>
#include <spdlog/spdlog.h>

namespace message_queue::tests {

using namespace std::chrono_literals;

void test_message_queue(bool use_shared_memory) {
    std::unique_ptr<MessageQueue> server(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::unique_ptr<MessageQueue> client(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::binary_semaphore server_started{0};
    std::binary_semaphore server_stopped{0};
    std::binary_semaphore client_stopped{0};
    std::binary_semaphore client_connected_from_server{0};
    std::binary_semaphore client_disconnected_from_server{0};
    std::binary_semaphore client_connected{0};
    std::binary_semaphore client_disconnected{0};
    std::binary_semaphore server_msg_received{0};
    MessageQueue::message_s server_msg;
    std::binary_semaphore client_msg_received{0};
    MessageQueue::message_s client_msg;
    std::binary_semaphore shared_memory_created{0};
    MessageQueue::message_s::id_t msg_id{0, 0xA1A2A3A4};
    server->set_message_handler([&](MessageQueue::message_s &&msg) {
        spdlog::trace("server msg received");
        server_msg = std::move(msg);
        server_msg_received.release();

        server->send_message(MessageQueue::message_s::from_str(server_msg.get_id(), "world"));
    });
    client->set_message_handler([&](MessageQueue::message_s &&msg) {
        spdlog::trace("client msg received");
        client_msg = std::move(msg);
        client_msg_received.release();
    });
    server->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::client_connected:
                client_connected_from_server.release();
                spdlog::trace("+++server event received: connected");
                break;
            case MessageQueue::event_e::client_disconnected:
                client_disconnected_from_server.release();
                spdlog::trace("+++server event received: disconnected");
                break;
            case MessageQueue::event_e::server_started:
                server_started.release();
                spdlog::trace("+++server event received: server_started");
                break;
            case MessageQueue::event_e::client_started:
                spdlog::trace("+++server event received: client_started");
                break;
            case MessageQueue::event_e::server_stopped:
                server_stopped.release();
                spdlog::trace("+++server event received: server_stopped");
                break;
            case MessageQueue::event_e::client_stopped:
                spdlog::trace("+++server event received: client_stopped");
                break;
            case MessageQueue::event_e::shared_memory_created:
                spdlog::trace("+++server event received: shared_memory_created");
                break;
            default: break;
        }
    });
    client->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::client_connected:
                client_connected.release();
                spdlog::trace("+++client event received: connected");
                break;
            case MessageQueue::event_e::client_disconnected:
                client_disconnected.release();
                spdlog::trace("+++client event received: disconnected");
                break;
            case MessageQueue::event_e::server_started:
                spdlog::trace("+++client event received: server_started");
                break;
            case MessageQueue::event_e::client_started:
                spdlog::trace("+++client event received: client_started");
                break;
            case MessageQueue::event_e::server_stopped:
                spdlog::trace("+++client event received: server_stopped");
                break;
            case MessageQueue::event_e::client_stopped:
                client_stopped.release();
                spdlog::trace("+++client event received: client_stopped");
                break;
            case MessageQueue::event_e::shared_memory_created:
                shared_memory_created.release();
                spdlog::trace("+++client event received: shared_memory_created");
                break;
            default: break;
        }
    });
    server->start_server(0);
    {
        REQUIRE(server_started.try_acquire_for(1000ms));
        client->start_client("localhost",
                             server->port(),
                             use_shared_memory ? std::optional<MessageQueue::shared_memory_config_s>(
                                                         {.max_num_msg_blocks = 2, .msg_block_size = 16})
                                               : std::optional<MessageQueue::shared_memory_config_s>());
        REQUIRE(client_connected_from_server.try_acquire_for(8000ms));
        REQUIRE(client_connected.try_acquire_for(1000ms));
        if (use_shared_memory) {
            REQUIRE(shared_memory_created.try_acquire_for(3000ms));
        }
        {
            REQUIRE_THROWS(client->send_message(MessageQueue::message_s::from_str({1, 1}, "asd")));
            client->send_message(MessageQueue::message_s::from_str(msg_id, "Hello"));
        }
        REQUIRE(server_msg_received.try_acquire_for(1000ms));
        REQUIRE(server_msg.get_id().client_id != 0);
        REQUIRE(server_msg.get_id().msg_id == msg_id.msg_id);
        REQUIRE(server_msg.get_str() == "Hello");

        REQUIRE(client_msg_received.try_acquire_for(1000ms));
        REQUIRE(client_msg.get_id() == msg_id);
        REQUIRE(client_msg.get_str() == "world");

        server->stop();
        client->stop();
        REQUIRE(client_disconnected_from_server.try_acquire_for(1000ms));
        REQUIRE(client_disconnected.try_acquire_for(1000ms));

        REQUIRE(server_stopped.try_acquire_for(1000ms));
        REQUIRE(client_stopped.try_acquire_for(1000ms));
    }
}

TEST_CASE("client-server interaction") {
    for (size_t i = 0; i < 1; ++i) {
        test_message_queue(false);
    }
}
TEST_CASE("client-server interaction with shared memory") {
    for (size_t i = 0; i < 1; ++i) {
        test_message_queue(true);
    }
}
TEST_CASE("test client multiple reconnect") {
    std::unique_ptr<MessageQueue> server(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::unique_ptr<MessageQueue> client(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::binary_semaphore server_started{0};
    std::binary_semaphore server_stopped{0};
    std::binary_semaphore client_stopped{0};
    std::binary_semaphore client_connected{0};
    std::binary_semaphore client_connected_on_server{0};
    std::binary_semaphore client_disconnected{0};
    server->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::server_started: server_started.release(); break;
            case MessageQueue::event_e::client_connected: client_connected_on_server.release(); break;
            case MessageQueue::event_e::server_stopped: server_stopped.release(); break;
            default: break;
        }
    });
    client->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::client_connected: client_connected.release(); break;
            case MessageQueue::event_e::client_disconnected: client_disconnected.release(); break;
            case MessageQueue::event_e::client_stopped: client_stopped.release(); break;
            default: break;
        }
    });
    server->start_server(0);
    {
        REQUIRE(server_started.try_acquire_for(1000ms));
        REQUIRE_NOTHROW(client->start_client("localhost", server->port()));
        REQUIRE(client_connected_on_server.try_acquire_for(1000ms));
        server->stop();
        std::this_thread::sleep_for(1000ms);
        REQUIRE(server_stopped.try_acquire_for(1000ms));
        REQUIRE_THROWS(client->send_message(MessageQueue::message_s::from_str({1, 1}, "asd")));
        REQUIRE(client_disconnected.try_acquire_for(15000ms));
        REQUIRE(client_stopped.try_acquire_for(1000ms));
        try {
            client->start_client("localhost", server->port());
        } catch (const std::exception &) {
        }
        REQUIRE(client_stopped.try_acquire_for(10000ms));
        server->start_server(0);
        REQUIRE(server_started.try_acquire_for(1000ms));
        REQUIRE_NOTHROW(client->start_client("localhost", server->port()));
        REQUIRE(client_connected_on_server.try_acquire_for(1000ms));
        server->stop();
        REQUIRE(server_stopped.try_acquire_for(1000ms));
        REQUIRE_THROWS(client->send_message(MessageQueue::message_s::from_str({1, 1}, "asd")));
        REQUIRE(client_disconnected.try_acquire_for(15000ms));
        REQUIRE(client_stopped.try_acquire_for(1000ms));
        client->stop();
    }
}

TEST_CASE("server stop") {
    std::unique_ptr<MessageQueue> server(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::unique_ptr<MessageQueue> client(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::binary_semaphore server_started{0};
    std::binary_semaphore server_stopped{0};
    std::binary_semaphore client_stopped{0};
    std::binary_semaphore client_cant_connect_err{0};
    std::binary_semaphore client_connected{0};
    std::binary_semaphore client_connected_on_server{0};
    std::binary_semaphore client_disconnected{0};
    server->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::server_started: server_started.release(); break;
            case MessageQueue::event_e::client_connected: client_connected_on_server.release(); break;
            case MessageQueue::event_e::server_stopped: server_stopped.release(); break;
            default: break;
        }
    });
    client->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::client_connected: client_connected.release(); break;
            case MessageQueue::event_e::client_disconnected: client_disconnected.release(); break;
            case MessageQueue::event_e::client_stopped: client_stopped.release(); break;
            case MessageQueue::event_e::client_cant_connect: client_cant_connect_err.release(); break;
            default: break;
        }
    });
    server->start_server(0);
    {
        REQUIRE(server_started.try_acquire_for(1000ms));
        auto port = server->port();
        std::this_thread::sleep_for(1000ms);
        server->stop();
        server.reset();
        REQUIRE(server_stopped.try_acquire_for(1000ms));
        std::this_thread::sleep_for(1000ms);
        client->start_client("localhost", port);
        REQUIRE(client_cant_connect_err.try_acquire_for(10000ms));
        REQUIRE(client_stopped.try_acquire_for(1000ms));
    }
}

TEST_CASE("test local message queue") {
    auto client_server = MessageQueue::create_local_message_queues(spdlog::default_logger());
    bool message_received_first = false;
    bool message_received_second = false;
    client_server.first->set_message_handler([&](MessageQueue::message_s &&msg) {
        message_received_first = true;
        REQUIRE(msg.get_id().msg_id == 2);
        REQUIRE(msg.get_str() == "Hello");
    });
    client_server.second->set_message_handler([&](MessageQueue::message_s &&msg) {
        message_received_second = true;
        REQUIRE(msg.get_id().msg_id == 1);
        REQUIRE(msg.get_str() == "Hello");
    });
    client_server.first->send_message(MessageQueue::message_s::from_str({1, 1}, "Hello"));
    REQUIRE(message_received_second);
    client_server.second->send_message(MessageQueue::message_s::from_str({1, 2}, "Hello"));
    REQUIRE(message_received_first);
}

TEST_CASE("client send and stop at the same time") {
    std::unique_ptr<MessageQueue> server(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::unique_ptr<MessageQueue> client(MessageQueue::create_socket_message_queue(spdlog::default_logger()));
    std::binary_semaphore server_started{0};
    bool server_stopped = false;
    std::binary_semaphore client_stopped{0};
    std::binary_semaphore client_cant_connect_err{0};
    std::binary_semaphore client_connected{0};
    std::binary_semaphore client_connected_on_server{0};
    std::binary_semaphore client_disconnected_on_server{0};
    std::binary_semaphore client_disconnected{0};
    std::binary_semaphore half_messages_received{0};
    server->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::server_started: server_started.release(); break;
            case MessageQueue::event_e::client_connected: client_connected_on_server.release(); break;
            case MessageQueue::event_e::client_disconnected: client_disconnected_on_server.release(); break;
            case MessageQueue::event_e::server_stopped: server_stopped = true; break;
            default: break;
        }
    });
    client->set_event_handler([&](MessageQueue::event_e event) {
        switch (event) {
            case MessageQueue::event_e::client_connected: client_connected.release(); break;
            case MessageQueue::event_e::client_disconnected: client_disconnected.release(); break;
            case MessageQueue::event_e::client_stopped: client_stopped.release(); break;
            case MessageQueue::event_e::client_cant_connect: client_cant_connect_err.release(); break;
            default: break;
        }
    });
    uint32_t message_count = 1000;
    uint32_t messages_received = 0;
    server->set_message_handler([&](MessageQueue::message_s &&) {
        ++messages_received;
        if (messages_received == message_count / 2) {
            half_messages_received.release();
        }
    });
    server->start_server(0);
    {
        REQUIRE(server_started.try_acquire_for(1000ms));
        auto port = server->port();
        client->start_client("localhost", port);
        REQUIRE(client_connected.try_acquire_for(1000ms));
        for (uint32_t i = 0; i < message_count; ++i) {
            REQUIRE_NOTHROW(client->send_message(MessageQueue::message_s::from_str({0, i}, "asd")));
        }
        REQUIRE(half_messages_received.try_acquire_for(1000ms));
        client.reset();
        REQUIRE(client_disconnected_on_server.try_acquire_for(1000ms));
        server.reset();
        REQUIRE(server_stopped);
    }
}

} // namespace message_queue::tests