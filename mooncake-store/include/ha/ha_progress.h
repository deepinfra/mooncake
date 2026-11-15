#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>

namespace mooncake::ha {

// When this process last advanced its index: an OpLog entry applied, or a
// snapshot or restore chunk loaded. /standby_status reports the time since,
// so an operator can tell a long restore from one that is stuck.
inline int64_t ProgressNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline std::atomic<int64_t>& ProgressStampMs() {
    static std::atomic<int64_t> stamp{ProgressNowMs()};
    return stamp;
}

inline void NoteProgress() {
    ProgressStampMs().store(ProgressNowMs(), std::memory_order_relaxed);
}

inline uint64_t MillisSinceProgress() {
    const int64_t since =
        ProgressNowMs() - ProgressStampMs().load(std::memory_order_relaxed);
    return since > 0 ? static_cast<uint64_t>(since) : 0;
}

}  // namespace mooncake::ha
