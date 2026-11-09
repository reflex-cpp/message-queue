#pragma once
#include <boost/predef.h>

#include <cassert>
#include <system_error>

#if BOOST_OS_WINDOWS
    #include <Windows.h>
#elif BOOST_OS_LINUX
    #include <cerrno>
    #include <semaphore.h>
#endif

namespace message_queue {

class NamedSemaphore {
    struct impl_tag {};

public:
    explicit NamedSemaphore(std::string name, std::optional<unsigned> value = {})
        : NamedSemaphore(impl_tag{}, std::move(name), value) {}

    NamedSemaphore(const NamedSemaphore &) = delete;
    NamedSemaphore(NamedSemaphore &&) = delete;

    ~NamedSemaphore() { destroy_impl(); }

    NamedSemaphore &operator=(const NamedSemaphore &) = delete;
    NamedSemaphore &operator=(NamedSemaphore &&) = delete;

    void lock() { lock_impl(); }
    bool try_lock() { return try_lock_impl(); }
    void unlock() { unlock_impl(); }

private:
#if BOOST_OS_WINDOWS
    HANDLE m_handle;

    explicit NamedSemaphore(impl_tag, std::string name, std::optional<unsigned> value)
        : m_handle{[](std::string name, std::optional<unsigned> value) {
              if (value.has_value() && value > static_cast<unsigned>(std::numeric_limits<LONG>::max())) {
                  throw std::system_error{make_error_code(std::errc::invalid_argument),
                                          "Initializing semaphore: Initial value out of allowed range"};
              }
              const auto handle = value.has_value()
                                          ? CreateSemaphoreW(nullptr,
                                                             value.has_value() ? static_cast<LONG>(*value) : 0,
                                                             std::numeric_limits<LONG>::max(),
                                                             std::wstring(name.begin(), name.end()).c_str())
                                          : OpenSemaphoreW(SEMAPHORE_ALL_ACCESS,
                                                           FALSE,
                                                           std::wstring(name.begin(), name.end()).c_str());
              if (!handle) {
                  const auto last_error = GetLastError();
                  throw std::system_error{static_cast<int>(last_error),
                                          std::system_category(),
                                          "Initializing semaphore"};
              }
              return handle;
          }(std::move(name), value)} {}

    void destroy_impl() noexcept {
        [[maybe_unused]] const auto success = CloseHandle(m_handle);
        assert(success);
    }

    void lock_impl() {
        if (WaitForSingleObject(m_handle, INFINITE) != WAIT_OBJECT_0) {
            const auto last_error = GetLastError();
            throw std::system_error{static_cast<int>(last_error), std::system_category(), "Locking semaphore"};
        }
    }

    bool try_lock_impl() {
        const auto result = WaitForSingleObject(m_handle, 0);
        switch (result) {
            case WAIT_OBJECT_0: return true;
            case WAIT_TIMEOUT: return false;
            default:
                const auto last_error = GetLastError();
                throw std::system_error{static_cast<int>(last_error), std::system_category(), "Locking semaphore"};
        }
    }

    void unlock_impl() {
        if (!ReleaseSemaphore(m_handle, 1, nullptr)) {
            const auto last_error = GetLastError();
            throw std::system_error{static_cast<int>(last_error), std::system_category(), "Locking semaphore"};
        }
    }

#elif BOOST_OS_LINUX
    sem_t *m_sem;

    explicit NamedSemaphore(impl_tag, std::string name, std::optional<unsigned> value) {
        if (value.has_value()) {
            m_sem = sem_open(name.c_str(), O_CREAT, 0777, *value);
        } else {
            m_sem = sem_open(name.c_str(), 0);
            sem_unlink(name.c_str());
        }
        if (m_sem == SEM_FAILED) {
            const auto last_error = errno;
            throw std::system_error{last_error, std::system_category(), "Initializing semaphore"};
        }
    }

    void destroy_impl() noexcept {
        [[maybe_unused]] const auto result = sem_close(m_sem);
        assert(!result);
    }

    void lock_impl() {
        while (sem_wait(m_sem)) {
            const auto last_error = errno;
            if (last_error == EINTR) {
                continue;
            }
            throw std::system_error{last_error, std::system_category(), "Locking semaphore"};
        }
    }

    bool try_lock_impl() {
        if (!sem_trywait(m_sem)) {
            return true;
        } else {
            const auto last_error = errno;
            if (last_error == EAGAIN) {
                return false;
            }
            throw std::system_error{last_error, std::system_category(), "Trying to lock semaphore"};
        }
    }

    void unlock_impl() {
        if (sem_post(m_sem)) {
            const auto last_error = errno;
            throw std::system_error{last_error, std::system_category(), "Unlocking semaphore"};
        }
    }
#else
    #error NamedSemaphore is not implemented for this platform
#endif
};

} // namespace message_queue