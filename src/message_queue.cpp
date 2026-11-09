// Part of the message-queue project.
// Copyright © message-queue Project Contributors.
// SPDX-License-Identifier: Apache-2.0

// #undef SPDLOG_ACTIVE_LEVEL
// #define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE

#include <message_queue/message_queue.hpp>

#include <deque>
#include <memory>
#include <mutex>
#include <semaphore>
#include <set>
#include <thread>
#include <utility>

#include <boost/asio.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/sync/named_condition.hpp>
#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "named_semaphore.hpp"


namespace message_queue {

namespace {

namespace asio = boost::asio;

template<typename T>
class Guarded {
public:
    template<typename... Args>
        requires std::constructible_from<T, Args...>
    constexpr explicit Guarded(Args &&...args) : m_value(std::forward<Args>(args)...) {}

    template<typename Fn>
    decltype(auto) lock(Fn &&fn) & {
        const auto guard = std::lock_guard{m_mutex};
        return std::invoke(std::forward<Fn>(fn), m_value);
    }

    template<typename Fn>
    decltype(auto) lock(Fn &&fn) const & {
        const auto guard = std::lock_guard{m_mutex};
        return std::invoke(std::forward<Fn>(fn), m_value);
    }

private:
    mutable std::mutex m_mutex{};
    T m_value;
};

using messages_t = std::deque<MessageQueue::message_s>;

template<class Semaphore>
class UniqueAcquire {
public:
    using semaphore_type = Semaphore;

    UniqueAcquire() noexcept = default;

    explicit UniqueAcquire(Semaphore &semaphore) : m_semaphore(std::addressof(semaphore)), m_owns(false) {
        m_semaphore->acquire();
        m_owns = true;
    }

    UniqueAcquire(UniqueAcquire &&other) noexcept : m_semaphore(other.m_semaphore), m_owns(other.m_owns) {
        other.m_semaphore = nullptr;
        other.m_owns = false;
    }

    UniqueAcquire &operator=(UniqueAcquire &&other) noexcept {
        if (this != std::addressof(other)) {
            if (m_owns) {
                m_semaphore->release();
            }

            m_semaphore = other.m_semaphore;
            m_owns = other.m_owns;
            other.m_semaphore = nullptr;
            other.m_owns = false;
        }
        return *this;
    }

    ~UniqueAcquire() noexcept {
        if (m_owns) {
            m_semaphore->release();
        }
    }

    UniqueAcquire(const UniqueAcquire &) = delete;
    UniqueAcquire &operator=(const UniqueAcquire &) = delete;

    void acquire() {
        validate();
        m_semaphore->acquire();
        m_owns = true;
    }

    [[nodiscard]] bool try_acquire() {
        validate();
        m_owns = m_semaphore->try_acquire();
        return m_owns;
    }

    void release() {
        if (!m_semaphore || !m_owns) {
            throw std::runtime_error("Operation not permitted");
        }

        m_semaphore->release();
        m_owns = false;
    }

    void swap(UniqueAcquire &other) noexcept {
        std::swap(m_semaphore, other.m_semaphore);
        std::swap(m_owns, other.m_owns);
    }

    [[nodiscard]] bool owns_acquire() const noexcept { return m_owns; }

    explicit operator bool() const noexcept { return m_owns; }

    [[nodiscard]] Semaphore *semaphore() const noexcept { return m_semaphore; }

private:
    Semaphore *m_semaphore = nullptr;
    bool m_owns = false;

    void validate() const {
        if (!m_semaphore) {
            throw std::runtime_error{"Operation not permitted"};
        }

        if (m_owns) {
            throw std::runtime_error{"Resource deadlock would occur"};
        }
    }
};

class SharedMemoryMessageQueue {
    class ShmRemove {
    public:
        explicit ShmRemove(const std::string_view filename) : m_filename{filename} {
            boost::interprocess::shared_memory_object::remove(m_filename.c_str());
        }

        ShmRemove(const ShmRemove &other) = delete;
        ShmRemove &operator=(const ShmRemove &other) = delete;

        ~ShmRemove() { boost::interprocess::shared_memory_object::remove(m_filename.c_str()); }

    private:
        std::string m_filename;
    };

    static std::string get_send_message_queue_name(const std::string &name) { return name + "_send"; }

    static std::string get_receive_message_queue_name(const std::string &name) { return name + "_receive"; }

    [[noreturn]] static void format_create_or_open_message_queue_error_and_throw(const std::exception &e,
                                                                                 std::string_view name,
                                                                                 spdlog::logger &logger,
                                                                                 bool create) {
#ifdef _WIN32
        const auto expected_shared_file_location = "C:/ProgramData/boost_interprocess";
#else
        const auto expected_shared_file_location = "/dev/shm";
#endif
        const auto error = fmt::format("Failed to {} message queue {}: {}. Expected file location: {}",
                                       create ? "create" : "open",
                                       name,
                                       e.what(),
                                       expected_shared_file_location);
        logger.error(error);
        throw std::runtime_error(error);
    }

    static auto try_create_message_queue(const std::string &name,
                                         spdlog::logger &logger,
                                         std::size_t max_num_msg,
                                         std::size_t msg_block_size) {
        try {
            return boost::interprocess::message_queue{boost::interprocess::create_only,
                                                      name.c_str(),
                                                      max_num_msg,
                                                      msg_block_size};
        } catch (const std::exception &e) {
            format_create_or_open_message_queue_error_and_throw(e, name, logger, true);
        }
    }

    static auto try_open_message_queue(const std::string &name, spdlog::logger &logger) {
        try {
            return boost::interprocess::message_queue{boost::interprocess::open_only, name.c_str()};
        } catch (const std::exception &e) {
            format_create_or_open_message_queue_error_and_throw(e, name, logger, false);
        }
    }

public:
    // constructor for server, deletes shared memory objects automatically
    SharedMemoryMessageQueue(std::shared_ptr<spdlog::logger> logger,
                             const std::string &name,
                             const std::size_t max_num_msg,
                             const std::size_t msg_block_size)
        : m_logger{std::move(logger)},
          m_send_queue_guard{get_send_message_queue_name(name)},
          m_receive_queue_guard{get_receive_message_queue_name(name)},
          m_send_queue(try_create_message_queue(get_receive_message_queue_name(name),
                                                *m_logger,
                                                max_num_msg,
                                                msg_block_size)),
          m_receive_queue(
                  try_create_message_queue(get_send_message_queue_name(name), *m_logger, max_num_msg, msg_block_size)),
          m_read_buffer(msg_block_size),
          m_send_semaphore(fmt::format("{}_send_semaphore", name), 0),
          m_receive_semaphore(fmt::format("{}_receive_semaphore", name), 0) {}

    // constructor for client, no queue guard is needed
    explicit SharedMemoryMessageQueue(std::shared_ptr<spdlog::logger> logger, const std::string &name)
        : m_logger{std::move(logger)},
          m_send_queue(try_open_message_queue(get_send_message_queue_name(name), *m_logger)),
          m_receive_queue(try_open_message_queue(get_receive_message_queue_name(name), *m_logger)),
          m_read_buffer(m_receive_queue.get_max_msg_size()),
          m_send_semaphore(fmt::format("{}_receive_semaphore", name)),
          m_receive_semaphore(fmt::format("{}_send_semaphore", name)) {}

    virtual ~SharedMemoryMessageQueue() { stop(); }

    void set_message_handler(MessageQueue::on_message_received_t on_message_received) {
        m_on_message_received_callback = on_message_received;
    }

    void start() {
        m_read_thread = std::jthread([this]() {
            SPDLOG_LOGGER_TRACE(m_logger, "shmem thread started");
            read_loop();
        });
        m_write_thread = std::jthread([this]() {
            SPDLOG_LOGGER_TRACE(m_logger, "shmem thread started");
            write_loop();
        });
    }

    void stop() {
        if (m_stopped)
            return;
        m_stopped = true;
        m_write_cv.notify_one();
        m_receive_semaphore.unlock();
        // m_receive_cv.notify_one();
        m_read_thread.join();
        m_write_thread.join();
    }

    virtual void send_message(MessageQueue::message_s msg) {
        std::unique_lock lock(m_write_mutex);
        m_messages_to_send.push_back(std::move(msg));
        m_write_cv.notify_one();
    }

private:
    void write_loop() {
        while (!m_stopped) {
            {
                std::unique_lock lock(m_write_mutex);
                m_write_cv.wait(lock, [this] { return !m_messages_to_send.empty() || m_stopped; });
                if (m_stopped)
                    return;
                m_write_buffer = std::move(m_messages_to_send.front().data);
                m_messages_to_send.pop_front();
                m_bytes_written = 0;
            }
            while (!m_write_buffer.empty()) {
                const auto bytes_to_write = std::min(m_write_buffer.size() - m_bytes_written, m_read_buffer.size());
                SPDLOG_LOGGER_TRACE(m_logger, "trying to send msg: {}", bytes_to_write);
                if (m_send_queue.try_send(m_write_buffer.data() + m_bytes_written, bytes_to_write, 0)) {
                    SPDLOG_LOGGER_TRACE(m_logger, "msg sent");
                    m_bytes_written += bytes_to_write;
                    if (bytes_to_write < m_read_buffer.size()) {
                        m_write_buffer.clear();
                        m_bytes_written = 0;
                    }
                    m_send_semaphore.unlock();
                    // boost::interprocess::scoped_lock lock(m_send_mutex);
                    // m_send_cv.notify_one();
                } else {
                    SPDLOG_LOGGER_TRACE(m_logger, "msg not sent");
                }
            }
        }
    }

    void read_loop() {
        while (!m_stopped) {
            m_receive_semaphore.lock();
            if (m_stopped)
                return;
            unsigned int priority;
            boost::interprocess::message_queue::size_type recvd_size;
            m_receive_queue.receive(m_read_buffer.data(), m_read_buffer.size(), recvd_size, priority);
            if (m_on_message_received_callback) {
                if (recvd_size > 0) {
                    m_received_message.add_data_block(std::span(m_read_buffer.data(), recvd_size));
                }
                if (recvd_size != m_read_buffer.size()) {
                    m_on_message_received_callback(std::move(m_received_message));
                }
            }
        }
    }

private:
    std::shared_ptr<spdlog::logger> m_logger;
    std::optional<ShmRemove> m_send_queue_guard;
    std::optional<ShmRemove> m_receive_queue_guard;

    boost::interprocess::message_queue m_send_queue;
    boost::interprocess::message_queue m_receive_queue;
    MessageQueue::message_s::data_t m_read_buffer;
    MessageQueue::message_s::data_t m_write_buffer;
    size_t m_bytes_written = 0;

    messages_t m_messages_to_send;
    std::atomic<bool> m_stopped = false;
    MessageQueue::on_message_received_t m_on_message_received_callback = nullptr;
    MessageQueue::message_s m_received_message;

    std::jthread m_read_thread;
    std::jthread m_write_thread;
    std::mutex m_write_mutex;
    std::condition_variable m_write_cv;

    NamedSemaphore m_send_semaphore;
    NamedSemaphore m_receive_semaphore;
};

class SocketMessageQueue;

enum class message_type_e : std::uint32_t {
    unknown,
    data,
    create_shared_memory_request,
    create_shared_memory_response,
};

constexpr std::string_view message_header = "MSQ2";

enum status_e {
    ok,
    failed,
};

struct create_shared_memory_message_request_s {
    static constexpr message_type_e type = message_type_e::create_shared_memory_request;

    MessageQueue::shared_memory_config_s config;
};

struct create_shared_memory_message_response_s {
    static constexpr message_type_e type = message_type_e::create_shared_memory_response;

    status_e status;
    std::uint32_t id;
    std::uint32_t number;
};

struct typed_message_s {
    message_type_e type;
    MessageQueue::message_s::data_t message{};

    template<typename T>
    static typed_message_s from(T msg) {
        typed_message_s result{.type = T::type};
        result.message.resize(sizeof(T));
        new (result.message.data()) T(std::move(msg));
        return result;
    }

    static typed_message_s from(MessageQueue::message_s &msg) {
        return typed_message_s{message_type_e::data, std::move(msg.data)};
    }

    template<typename T>
    T &to() {
        if (type != T::type) {
            throw std::runtime_error{"Bad type conversion"};
        }
        if (message.size() != sizeof(T)) {
            throw std::runtime_error{"Incorrect message size"};
        }
        return *reinterpret_cast<T *>(message.data());
    }

    MessageQueue::message_s to_data_message() {
        if (type != message_type_e::data) {
            throw std::runtime_error{"Bad type conversion"};
        }
        return {std::move(message)};
    }
};

class Session : public std::enable_shared_from_this<Session> {
public:
    using session_id_t = std::uint32_t;
    static constexpr size_t message_channel_max_messages = 1024;

    Session(std::shared_ptr<spdlog::logger> logger,
            asio::ip::tcp::socket socket,
            SocketMessageQueue &parent,
            session_id_t session_id)
        : m_logger{std::move(logger)},
          m_socket(std::move(socket)),
          m_parent(parent),
          m_session_id(session_id),
          m_message_channel(m_socket.get_executor(), message_channel_max_messages) {}

    ~Session() { stop(false); }

    session_id_t id() const { return m_session_id; }

    void start() {
        SPDLOG_LOGGER_TRACE(m_logger, "Starting session");
        join_to_parent();
        m_logger->info("Message queue connected to {}:{}",
                       m_socket.remote_endpoint().address().to_string(),
                       m_socket.remote_endpoint().port());
        send_event(MessageQueue::event_e::client_connected);

        asio::co_spawn(m_socket.get_executor(), [self = shared_from_this()] { return self->reader(); }, asio::detached);

        asio::co_spawn(m_socket.get_executor(), [self = shared_from_this()] { return self->writer(); }, asio::detached);
    }

    void send_message(typed_message_s msg) {
        SPDLOG_LOGGER_TRACE(m_logger, "session send_message size {}", msg.message.size());
        while (!m_message_channel.try_send(boost::system::error_code(), std::move(msg))) {
        }
    }

    void send_data_message(MessageQueue::message_s msg) {
        if (m_shared_memory_message_queue.lock([&](auto &shared_memory_message_queue) {
                if (shared_memory_message_queue) {
                    SPDLOG_LOGGER_TRACE(m_logger, "session sending via shared mem {}", msg.data.size());
                    shared_memory_message_queue->send_message(std::move(msg));
                    return false;
                }
                return true;
            })) {
            send_message(typed_message_s::from(msg));
        }
    }

    void stop(bool _leave_from_parent) {
        if (!m_stopped.exchange(true)) {
            SPDLOG_LOGGER_TRACE(m_logger, "Stopping session");
            if (_leave_from_parent) {
                leave_from_parent();
            }
            m_shared_memory_message_queue.lock([&](auto &shared_memory_message_queue) {
                if (shared_memory_message_queue) {
                    shared_memory_message_queue->stop();
                    SPDLOG_LOGGER_TRACE(m_logger, "session shmem thread joining");
                    shared_memory_message_queue.reset();
                }
            });
            try {
                // remote_endpoint may not be available
                m_logger->info("Message queue disconnected from {}:{}",
                               m_socket.remote_endpoint().address().to_string(),
                               m_socket.remote_endpoint().port());
            } catch (const std::exception &e) {
                m_logger->info("Message queue disconnected");
            }
            m_message_channel.close();
            boost::system::error_code ec;
            m_socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            m_socket.close(ec);
            m_logger->debug("Client disconnected");
            send_event(MessageQueue::event_e::client_disconnected);
        }
    }

private:
    asio::awaitable<void> reader() {
        std::array<char, message_header.length()> header_data{};
        try {
            for (;;) {
                co_await asio::async_read(m_socket,
                                          asio::buffer(header_data),
                                          asio::transfer_exactly(message_header.length()),
                                          asio::use_awaitable);
                std::string_view read_header(header_data.data(), message_header.length());
                if (read_header != message_header) {
                    m_logger->error("received message with wrong header, expected {}, received {}",
                                    message_header,
                                    read_header);
                    throw std::runtime_error(std::string("Wrong message header"));
                }
                typed_message_s msg;
                co_await asio::async_read(m_socket,
                                          asio::buffer(&msg.type, sizeof(msg.type)),
                                          asio::transfer_exactly(sizeof(msg.type)),
                                          asio::use_awaitable);
                size_t msg_length = 0;
                co_await asio::async_read(m_socket,
                                          asio::buffer(&msg_length, sizeof(msg_length)),
                                          asio::transfer_exactly(sizeof(msg_length)),
                                          asio::use_awaitable);
                msg.message.resize(msg_length);
                co_await asio::async_read(m_socket,
                                          asio::buffer(msg.message),
                                          asio::transfer_exactly(msg_length),
                                          asio::use_awaitable);
                SPDLOG_LOGGER_TRACE(m_logger, "reader msg {}", msg.message.size());

                on_message_received(msg);
            }
        } catch (const std::exception &e) {
            SPDLOG_LOGGER_TRACE(m_logger, "reader err: {}", e.what());
            stop(true);
        }
    }

    asio::awaitable<void> writer() {
        try {
            for (;;) {
                auto msg = co_await m_message_channel.async_receive(asio::use_awaitable);

                SPDLOG_LOGGER_TRACE(m_logger, "writer sending msg {}", msg.message.size());
                co_await asio::async_write(m_socket,
                                           asio::buffer(message_header.data(), message_header.length()),
                                           asio::use_awaitable);
                co_await asio::async_write(m_socket, asio::buffer(&msg.type, sizeof(msg.type)), asio::use_awaitable);

                auto msg_length = msg.message.size();
                co_await asio::async_write(m_socket,
                                           asio::buffer(&msg_length, sizeof(msg_length)),
                                           asio::use_awaitable);
                co_await asio::async_write(m_socket, asio::buffer(msg.message), asio::use_awaitable);
            }
        } catch (const std::exception &e) {
            SPDLOG_LOGGER_TRACE(m_logger, "writer err: {}", e.what());
            stop(true);
        }
    }


    void join_to_parent();

    void leave_from_parent();

    void on_message_received(typed_message_s &msg);
    void send_event(MessageQueue::event_e event);

    std::shared_ptr<spdlog::logger> m_logger;
    asio::ip::tcp::socket m_socket;
    SocketMessageQueue &m_parent;
    std::atomic<bool> m_stopped = false;
    Guarded<std::unique_ptr<SharedMemoryMessageQueue>> m_shared_memory_message_queue;
    session_id_t m_session_id;
    asio::experimental::concurrent_channel<void(boost::system::error_code, typed_message_s)> m_message_channel;
};


class SocketMessageQueue : public MessageQueue {
public:
    explicit SocketMessageQueue(std::shared_ptr<spdlog::logger> logger) : m_logger{std::move(logger)} {}
    ~SocketMessageQueue() override { SocketMessageQueue::stop(); }
    std::uint16_t port() override { return m_port; }

    void set_message_handler(on_message_received_t on_message_received) override {
        m_on_message_received_callback = on_message_received;
    }
    void set_event_handler(on_event_received_t on_event_received) override {
        m_on_event_received_callback = on_event_received;
    }

    void start_server(std::uint16_t port) override {
        UniqueAcquire start_stop_guard(m_start_stop_semaphore);
        {
            bool expected_running = false;
            if (!m_running.compare_exchange_weak(expected_running, true)) {
                throw std::runtime_error("Server already running");
            }
        }
        assert(!m_thread.joinable());
        m_is_server = true;
        m_port = port;
        m_thread = std::jthread([this, start_stop_guard = std::move(start_stop_guard)]() mutable {
            m_context.emplace(1);
            try {
                co_spawn(
                        *m_context,
                        [this, start_stop_guard = std::move(start_stop_guard)]() mutable {
                            return server_listener(*m_context, std::move(start_stop_guard));
                        },
                        asio::detached);
                SPDLOG_LOGGER_TRACE(m_logger, "server run");
                m_context->run();
                SPDLOG_LOGGER_TRACE(m_logger, "server end");
            } catch (const std::exception &e) {
                SPDLOG_LOGGER_TRACE(m_logger, "server exception: {}", e.what());
            }
            stop_sessions_and_clear_context();
            send_event(event_e::server_stopped);
        });
    }


    void start_client(const std::string_view host,
                      std::uint16_t port,
                      std::optional<shared_memory_config_s> shared_memory_config) override {
        UniqueAcquire start_stop_guard(m_start_stop_semaphore);
        {
            bool expected_running = false;
            if (!m_running.compare_exchange_weak(expected_running, true)) {
                throw std::runtime_error("Server already running");
            }
        }
        m_is_server = false;
        m_host = host;
        m_port = port;
        m_thread = std::jthread([this, shared_memory_config, start_stop_guard = std::move(start_stop_guard)]() mutable {
            m_context.emplace(1);
            bool connection_error = false;
            co_spawn(
                    *m_context,
                    [this, shared_memory_config, start_stop_guard = std::move(start_stop_guard)]() mutable {
                        return client_listener(*m_context, shared_memory_config, std::move(start_stop_guard));
                    },
                    [&](std::exception_ptr e) {
                        if (e) {
                            connection_error = true;
                        }
                    });
            SPDLOG_LOGGER_TRACE(m_logger, "client run");
            m_context->run();
            stop_sessions_and_clear_context();
            send_event(event_e::client_stopped);
            if (connection_error) {
                send_event(event_e::client_cant_connect);
            }
            SPDLOG_LOGGER_TRACE(m_logger, "client end");
        });
    }

    void stop_sessions_and_clear_context() {
        {
            auto sessions_to_close = m_sessions.lock([](auto &sessions) { return std::move(sessions); });
            for (const auto &session : sessions_to_close) {
                session->stop(false);
            }
        }
        {
            UniqueAcquire start_stop_guard(m_start_stop_semaphore);
            SPDLOG_LOGGER_TRACE(m_logger, "stop_sessions_and_clear_context called");
            m_context.reset();
            bool expected_running = true;
            if (!m_running.compare_exchange_weak(expected_running, false)) {
                assert(false && "Something wrong, m_running flag must be true");
            }
        }
    }

    void stop() override {
        UniqueAcquire start_stop_guard(m_start_stop_semaphore);
        if (m_running) {
            SPDLOG_LOGGER_TRACE(m_logger, "stop called");
            m_context->stop();
            start_stop_guard.release();
            m_thread.join();
        }
    }

    void join(std::shared_ptr<Session> session) {
        m_sessions.lock([&](auto &sessions) { sessions.insert(session); });
    }

    void leave(std::shared_ptr<Session> session) {
        m_sessions.lock([&](auto &sessions) { sessions.erase(session); });
    }

    void broadcast_message(message_s msg) override {
        if (!m_is_server) {
            throw std::runtime_error("Only server can broadcast messages");
        }

        msg.set_client_id(0);

        SPDLOG_LOGGER_TRACE(m_logger, "{} locking server");
        auto sessions = m_sessions.lock([&](auto &source) { return std::set(source); });
        SPDLOG_LOGGER_TRACE(m_logger, "server broadcasting msg {}", msg.data.size());

        for (const auto &session : sessions) {
            session->send_data_message(msg);
        }
    }

    void send_message(message_s msg) override {
        SPDLOG_LOGGER_TRACE(m_logger, "{} locking", m_is_server ? "server" : "client");
        auto sessions = m_sessions.lock([&](auto &source) { return std::set(source); });
        SPDLOG_LOGGER_TRACE(m_logger, "{} sending msg {}", m_is_server ? "server" : "client", msg.data.size());
        if (m_is_server) {
            for (const auto &session : sessions) {
                if (session->id() == msg.get_id().client_id) {
                    msg.set_client_id(0);
                    session->send_data_message(std::move(msg));
                    break;
                }
            }
        } else {
            if (msg.get_id().client_id != 0) {
                throw std::runtime_error("message client_id must be 0");
            }
            if (sessions.empty()) {
                throw std::runtime_error("Client not connected");
            }
            assert(sessions.size() == 1);
            for (const auto &session : sessions) {
                session->send_data_message(std::move(msg));
                break;
            }
        }
    }

    void on_message_received(message_s msg, Session::session_id_t id) {
        if (m_on_message_received_callback) {
            SPDLOG_LOGGER_TRACE(m_logger,
                                "{} on_message_received size {}",
                                m_is_server ? "server" : "client",
                                msg.data.size());
            assert(msg.get_id().client_id == 0);
            if (m_is_server) {
                msg.set_client_id(id);
            }
            m_on_message_received_callback(std::move(msg));
        }
    }

    void send_event(event_e event) {
        if (m_on_event_received_callback) {
            m_on_event_received_callback(event);
        }
    }

private:
    std::shared_ptr<spdlog::logger> m_logger;
    std::binary_semaphore m_start_stop_semaphore{1};
    std::optional<asio::io_context> m_context;
    std::jthread m_thread;
    std::atomic<bool> m_running = false;
    std::string m_host;
    std::uint16_t m_port;
    Guarded<std::set<std::shared_ptr<Session>>> m_sessions;
    Session::session_id_t m_last_session_id = 1;
    bool m_is_server = false;
    on_message_received_t m_on_message_received_callback = nullptr;
    on_event_received_t m_on_event_received_callback = nullptr;

    asio::awaitable<void> server_listener(asio::io_context &context,
                                          UniqueAcquire<std::binary_semaphore> &&start_stop_guard) {
        start_stop_guard.release();
        try {
            asio::ip::tcp::acceptor acceptor(context, {asio::ip::tcp::v6(), m_port});
            m_port = acceptor.local_endpoint().port();
            m_logger->info("Message queue server listening on port {}", m_port);
            send_event(event_e::server_started);
            while (true) {
                SPDLOG_LOGGER_TRACE(m_logger, "Server waiting connection");
                std::make_shared<Session>(m_logger,
                                          co_await acceptor.async_accept(asio::use_awaitable),
                                          *this,
                                          m_last_session_id++)
                        ->start();
                m_logger->debug("Server accepted connection");
            }
        } catch (const std::exception &e) {
            SPDLOG_LOGGER_TRACE(m_logger, "server listener err: {}", e.what());
            m_logger->error("Server error: {}", e.what());
        }
        SPDLOG_LOGGER_TRACE(m_logger, "server listener exit");
    }

    asio::awaitable<void> client_listener(asio::io_context &context,
                                          std::optional<shared_memory_config_s> shared_memory_config,
                                          UniqueAcquire<std::binary_semaphore> &&start_stop_guard) {
        start_stop_guard.release();
        try {
            asio::ip::tcp::resolver resolver(context);
            asio::ip::tcp::resolver::results_type endpoints = resolver.resolve(m_host, std::to_string(m_port));
            asio::ip::tcp::socket socket(context);
            SPDLOG_LOGGER_TRACE(m_logger, "Client connecting");
            send_event(event_e::client_started);
            co_await asio::async_connect(socket, endpoints, asio::use_awaitable);
            auto session = std::make_shared<Session>(m_logger, std::move(socket), *this, m_last_session_id++);
            session->start();
            if (shared_memory_config) {
                session->send_message(typed_message_s::from(
                        create_shared_memory_message_request_s{.config = shared_memory_config.value()}));
            }
            m_logger->debug("Client connected to {}:{}", m_host, m_port);
        } catch (const std::exception &e) {
            m_logger->error("Client can`t connect to {}:{}, {}", m_host, m_port, e.what());
            throw std::runtime_error(std::string{"Client can`t connect to server: "} + e.what());
        }
        SPDLOG_LOGGER_TRACE(m_logger, "client listener exit");
    }
};

void Session::join_to_parent() { m_parent.join(shared_from_this()); }

void Session::leave_from_parent() { m_parent.leave(shared_from_this()); }

void Session::on_message_received(typed_message_s &msg) {
    switch (msg.type) {
        case message_type_e::data: {
            SPDLOG_LOGGER_TRACE(m_logger, "session received data msg size {}", msg.message.size());
            m_parent.on_message_received(msg.to_data_message(), m_session_id);
            break;
        }
        case message_type_e::create_shared_memory_request: {
            if (m_shared_memory_message_queue.lock(
                        [&](auto &shared_memory_message_queue) { return shared_memory_message_queue.get(); })) {
                throw std::runtime_error(std::string("Shared memory already created"));
            }

            auto request = msg.to<create_shared_memory_message_request_s>();
            // static std::uint32_t pid = boost::interprocess::ipcdetail::get_current_process_id();
            static std::uint32_t id = request.config.id; // shmem file reuse (shmem file isn`t deleted on crash)
            static std::uint32_t last_shared_memory_id = 0;
            const auto shared_mem_file_id = last_shared_memory_id++;
            SPDLOG_LOGGER_TRACE(m_logger,
                                "create_shared_memory_request: {} {} {}",
                                shared_mem_file_id,
                                request.config.max_num_msg_blocks,
                                request.config.msg_block_size);
            m_shared_memory_message_queue.lock([&](auto &shared_memory_message_queue) {
                shared_memory_message_queue.reset(
                        new SharedMemoryMessageQueue(m_logger,
                                                     std::string{"message_queue_"} + std::to_string(id) +
                                                             std::string{"_"} + std::to_string(shared_mem_file_id),
                                                     request.config.max_num_msg_blocks,
                                                     request.config.msg_block_size));
                shared_memory_message_queue->set_message_handler([&](MessageQueue::message_s msg) {
                    SPDLOG_LOGGER_TRACE(m_logger, "server received shmem msg size {}", msg.data.size());
                    m_parent.on_message_received(std::move(msg), m_session_id);
                });
                shared_memory_message_queue->start();
            });

            send_event(MessageQueue::event_e::shared_memory_created);
            send_message(typed_message_s::from(
                    create_shared_memory_message_response_s{.status = ok, .id = id, .number = shared_mem_file_id}));
            break;
        }
        case message_type_e::create_shared_memory_response: {
            if (m_shared_memory_message_queue.lock(
                        [&](auto &shared_memory_message_queue) { return shared_memory_message_queue.get(); })) {
                throw std::runtime_error(std::string("Shared memory already created"));
            }

            auto response = msg.to<create_shared_memory_message_response_s>();
            SPDLOG_LOGGER_TRACE(m_logger, "create_shared_memory_response: {}", response.status);
            if (response.status == ok) {
                m_shared_memory_message_queue.lock([&](auto &shared_memory_message_queue) {
                    shared_memory_message_queue.reset(
                            new SharedMemoryMessageQueue(m_logger,
                                                         std::string{"message_queue_"} + std::to_string(response.id) +
                                                                 std::string{"_"} + std::to_string(response.number)));
                    shared_memory_message_queue->set_message_handler([&](MessageQueue::message_s msg) {
                        SPDLOG_LOGGER_TRACE(m_logger, "client received shmem msg size {}", msg.data.size());
                        m_parent.on_message_received(std::move(msg), m_session_id);
                    });
                    shared_memory_message_queue->start();
                });

                send_event(MessageQueue::event_e::shared_memory_created);
            } else {
                throw std::runtime_error(
                        std::string("Server return failed status of create_shared_memory_message_response_s"));
            }
            break;
        }
        default: throw std::runtime_error(std::string("Wrong message type"));
    }
}

void Session::send_event(MessageQueue::event_e event) { m_parent.send_event(event); }

class LocalMessageQueue : public MessageQueue {
public:
    explicit LocalMessageQueue(std::shared_ptr<spdlog::logger> logger)
        : m_logger{std::move(logger)},
          m_linked_message_queue(nullptr) {}

    void set_linked_message_queue(LocalMessageQueue *message_queue) { m_linked_message_queue = message_queue; }

    std::uint16_t port() override { return 0; }

    void set_message_handler(on_message_received_t on_message_received) override {
        m_on_message_received_callback = on_message_received;
    }
    void set_event_handler(on_event_received_t on_event_received) override {
        m_on_event_received_callback = on_event_received;
    }

    void start_server(std::uint16_t) override {}


    void start_client(const std::string_view, std::uint16_t, std::optional<shared_memory_config_s>) override {}

    void stop() override {}

    void broadcast_message(message_s msg) override { send_message(std::move(msg)); }

    void send_message(message_s msg) override {
        if (m_linked_message_queue) {
            m_linked_message_queue->on_message_received(std::move(msg));
        }
    }

    void on_message_received(message_s msg) {
        if (m_on_message_received_callback) {
            m_on_message_received_callback(std::move(msg));
        }
    }

    void send_event(event_e event) {
        if (m_on_event_received_callback) {
            m_on_event_received_callback(event);
        }
    }

private:
    std::shared_ptr<spdlog::logger> m_logger;
    LocalMessageQueue *m_linked_message_queue;
    on_message_received_t m_on_message_received_callback = nullptr;
    on_event_received_t m_on_event_received_callback = nullptr;
};

template<typename T>
void write_trivial(std::byte *&dest, T const &value, [[maybe_unused]] const std::byte *data_end) {
    static_assert(std::is_trivially_copyable_v<T>, "must be trivially copyable");
    assert(dest <= data_end && static_cast<size_t>(data_end - dest) >= sizeof(value));
    memcpy(dest, &value, sizeof(value));
    dest += sizeof(value);
}

template<typename T>
T read_trivial(const std::byte *&src, const std::byte *data_end) {
    static_assert(std::is_trivially_copyable_v<T>, "must be trivially copyable");
    T value;
    assert(src <= data_end);
    if (static_cast<size_t>(data_end - src) < sizeof(value))
        throw std::runtime_error{"Invalid message format"};
    memcpy(&value, src, sizeof(value));
    src += sizeof(value);
    return value;
}

} // namespace


MessageQueue::message_s::message_s(id_t id, const std::span<const data_span_t> &datas) {
    std::uint64_t size = sizeof(id) + sizeof(std::uint64_t);
    for (const auto &input_data : datas) {
        size += input_data.size() + sizeof(std::uint64_t);
    }
    data.resize(size);

    auto data_it = data.data();
    const auto data_end = data.data() + data.size();
    write_trivial<id_t>(data_it, id, data_end);
    write_trivial<std::uint64_t>(data_it, datas.size(), data_end);
    for (const auto &input_data : datas) {
        write_trivial<std::uint64_t>(data_it, input_data.size(), data_end);
        memcpy(data_it, input_data.data(), input_data.size());
        data_it += input_data.size();
    }
    assert(data_it == data_end);
}

std::vector<MessageQueue::message_s::data_span_t> MessageQueue::message_s::get_datas() const {
    auto data_it = data.data();
    const auto data_end = data.data() + data.size();
    read_trivial<id_t>(data_it, data_end);
    std::vector<data_span_t> datas;
    const auto datas_count = read_trivial<std::uint64_t>(data_it, data_end);
    if (datas_count >= datas.max_size())
        throw std::runtime_error{"Invalid message format"};
    datas.reserve(datas_count);
    for (std::uint64_t i = 0; i < datas_count; ++i) {
        const auto data_size = read_trivial<std::uint64_t>(data_it, data_end);
        const auto begin = data_it;
        if (static_cast<size_t>(data_end - begin) < data_size)
            throw std::runtime_error{"Invalid message format"};
        data_it += data_size;
        const auto end = data_it;
        datas.push_back({begin, end});
    }
    if (data_it != data_end)
        throw std::runtime_error{"Invalid message format"};
    return datas;
}

MessageQueue::message_s::id_t MessageQueue::message_s::get_id() const {
    auto data_it = data.data();
    return read_trivial<id_t>(data_it, data.data() + data.size());
}

void MessageQueue::message_s::set_client_id(std::uint32_t client_id) {
    auto id = get_id();
    id.client_id = client_id;
    auto data_it = data.data();
    write_trivial<id_t>(data_it, id, data.data() + data.size());
}

std::unique_ptr<MessageQueue> MessageQueue::create_socket_message_queue(std::shared_ptr<spdlog::logger> logger) {
    return std::unique_ptr<MessageQueue>{new SocketMessageQueue(logger)};
}

std::pair<std::unique_ptr<MessageQueue>, std::unique_ptr<MessageQueue>> MessageQueue::create_local_message_queues(
        std::shared_ptr<spdlog::logger> logger) {
    auto first = std::unique_ptr<LocalMessageQueue>{new LocalMessageQueue(logger)};
    auto second = std::unique_ptr<LocalMessageQueue>{new LocalMessageQueue(logger)};
    first->set_linked_message_queue(second.get());
    second->set_linked_message_queue(first.get());
    return {std::unique_ptr<MessageQueue>{first.release()}, std::unique_ptr<MessageQueue>{second.release()}};
}

} // namespace message_queue
