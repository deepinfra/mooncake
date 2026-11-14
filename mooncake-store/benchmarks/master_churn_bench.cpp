// Fleet churn benchmark for the master.
//
// Imitates a fleet of store clients (one per engine rank) against one master
// and reports what the rest of the fleet sees while clients leave and join:
// heartbeat latency, data-plane RPC throughput, TCP accept latency, joins and
// live clients the master declared offline.
//
// Every client owns one memory segment that exists only as an address range,
// so the master can be filled to production key counts without any memory or
// transfer behind it. Heartbeats follow Client::StorageHeartbeatThreadMain:
// one Ping per second, ReMountSegment on NEED_REMOUNT, reconnect after three
// failed pings.
//
// Events are "<seconds after the fill>:<kill|join>:<count>", comma separated:
//   --events=30:kill:12,150:join:2
// kill stops the heartbeats of that many live clients without unmounting
// (a worker pod that went away); join starts that many new clients.

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_uint64(num_clients, 104, "Clients (ranks) mounted before the fill");
DEFINE_uint64(segment_size_gib, 128, "Segment size of every client, GiB");
DEFINE_uint64(value_size, 130048, "Object size in bytes");
DEFINE_uint64(fill_keys, 0, "Objects to create before the events start");
DEFINE_uint64(fill_threads, 32, "Threads used for the fill");
DEFINE_uint64(fill_batch, 128, "Objects per BatchPutStart during the fill");
DEFINE_uint64(load_threads, 8, "Data-plane threads after the fill");
DEFINE_uint64(load_period_us, 8000,
              "Period of one data-plane iteration per thread");
DEFINE_uint64(load_put_batch, 17, "Objects put per data-plane iteration");
DEFINE_uint64(load_exist_batch, 400,
              "Keys checked with BatchExistKey per data-plane iteration");
DEFINE_string(events, "", "Churn events, see the header of this file");
DEFINE_uint64(duration, 300, "Seconds to run after the fill");
DEFINE_uint64(ping_interval_ms, 1000, "Heartbeat interval");
DEFINE_uint64(mount_delay_ms, 2000,
              "Delay between a joining client's first ping and its "
              "MountSegment, as an engine allocates its segment in between");
DEFINE_string(report, "", "Write the per-second report to this file too");
DEFINE_string(key_prefix,
              "churn-bench@default@tp_rank:0@pcp0@dcp0@pp_rank:0@group:0-",
              "Key prefix; the default gives production-length keys");

namespace {

using Clock = std::chrono::steady_clock;
using mooncake::ErrorCode;

constexpr uint64_t kGiB = 1024ULL * 1024 * 1024;
constexpr uintptr_t kSegmentBase = 0x100000000ULL;

std::atomic<bool> g_stop{false};
Clock::time_point g_start;

double Seconds(Clock::time_point t) {
    return std::chrono::duration<double>(t - g_start).count();
}

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               Clock::now().time_since_epoch())
        .count();
}

void UnsetCpuAffinity() {
    cpu_set_t cpuset;
    memset(&cpuset, -1, sizeof(cpuset));
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

// Counters of the current report second, swapped out by the reporter.
struct Window {
    std::atomic<uint64_t> ping_ok{0};
    std::atomic<uint64_t> ping_fail{0};
    std::atomic<uint64_t> ping_max_us{0};
    std::atomic<uint64_t> put_items{0};
    std::atomic<uint64_t> put_failed_batches{0};
    std::atomic<uint64_t> exist_items{0};
    std::atomic<uint64_t> exist_failed_batches{0};
    std::atomic<uint64_t> rpc_max_us{0};
};

void StoreMax(std::atomic<uint64_t>& slot, uint64_t value) {
    uint64_t seen = slot.load(std::memory_order_relaxed);
    while (value > seen && !slot.compare_exchange_weak(
                               seen, value, std::memory_order_relaxed)) {
    }
}

struct Totals {
    std::atomic<uint64_t> keys_created{0};
    std::atomic<uint64_t> remount_started{0};
    std::atomic<uint64_t> remount_ok{0};
    std::atomic<uint64_t> remount_failed{0};
    std::atomic<uint64_t> mount_ok{0};
    std::atomic<uint64_t> mount_failed{0};
    std::atomic<uint64_t> reconnects{0};
    // A live client that had been OK and was told to remount: the master
    // declared it offline while it was heartbeating.
    std::atomic<uint64_t> declared_offline{0};
    std::atomic<int64_t> last_ping_ok_ns{0};
    std::atomic<uint64_t> max_fleet_ping_gap_us{0};
    std::atomic<uint64_t> max_client_silence_us{0};
    std::atomic<uint64_t> max_ping_us{0};
    std::atomic<uint64_t> max_rpc_us{0};
    std::atomic<int64_t> tcp_probe_ms{0};
    std::atomic<uint64_t> tcp_probe_fail{0};
    std::atomic<uint64_t> tcp_probe_fail_streak_max{0};
};

Window g_window;
Totals g_totals;

std::mutex g_join_mutex;
// Clients that joined after the fill.
std::vector<double> g_join_latency_s;   // start to first OK heartbeat
std::vector<double> g_mount_latency_s;  // MountSegment round trip
std::vector<double> g_first_remount_s;  // first ReMountSegment round trip

std::string HexKey(uint64_t stream, uint64_t index) {
    // 64 hex characters, like the block hashes engines key their KV by.
    static const char* digits = "0123456789abcdef";
    std::string out(64, '0');
    uint64_t state = stream * 0x9E3779B97F4A7C15ULL + index;
    for (int word = 0; word < 4; ++word) {
        state += 0x9E3779B97F4A7C15ULL;
        uint64_t z = state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        z ^= z >> 31;
        for (int nibble = 0; nibble < 16; ++nibble) {
            out[word * 16 + nibble] = digits[(z >> (nibble * 4)) & 0xF];
        }
    }
    return out;
}

std::string MakeKey(uint64_t stream, uint64_t index) {
    return FLAGS_key_prefix + HexKey(stream, index);
}

class SimClient {
   public:
    SimClient(size_t index, bool joined_late)
        : index_(index),
          joined_late_(joined_late),
          master_client_(mooncake::generate_uuid()) {
        segment_.id = mooncake::generate_uuid();
        segment_.name = "churn-seg-" + std::to_string(index);
        segment_.te_endpoint = segment_.name;
        segment_.base = kSegmentBase + index * FLAGS_segment_size_gib * kGiB;
        segment_.size = FLAGS_segment_size_gib * kGiB;
    }

    ~SimClient() { Kill(); }

    // Connects, starts heartbeating, then mounts the segment.
    bool Start() {
        join_start_ = Clock::now();
        auto ec = master_client_.Connect(FLAGS_master_server);
        if (ec != ErrorCode::OK) {
            LOG(ERROR) << "client " << index_
                       << ": connect failed: " << toString(ec);
            return false;
        }
        alive_.store(true);
        ping_thread_ = std::thread([this] { HeartbeatLoop(); });
        mount_thread_ = std::thread([this] { MountLoop(); });
        return true;
    }

    // A pod that went away: no more heartbeats, nothing unmounted.
    void Kill() {
        alive_.store(false);
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
        if (mount_thread_.joinable()) {
            mount_thread_.join();
        }
        if (remount_future_.valid()) {
            remount_future_.wait();
        }
    }

    bool alive() const { return alive_.load(); }
    bool ok() const { return ok_.load(); }
    size_t index() const { return index_; }

   private:
    void MountLoop() {
        UnsetCpuAffinity();
        // An engine allocates and registers its segment between creating the
        // store client and mounting.
        const auto ready_at =
            Clock::now() + std::chrono::milliseconds(FLAGS_mount_delay_ms);
        while (alive_.load() && Clock::now() < ready_at) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        while (alive_.load()) {
            const auto t0 = Clock::now();
            auto result = master_client_.MountSegment(segment_);
            const double took =
                std::chrono::duration<double>(Clock::now() - t0).count();
            if (result.has_value()) {
                mounted_.store(true);
                g_totals.mount_ok.fetch_add(1);
                if (joined_late_) {
                    std::lock_guard<std::mutex> lock(g_join_mutex);
                    g_mount_latency_s.push_back(took);
                    LOG(INFO) << "client " << index_ << ": mounted in " << took
                              << " s";
                }
                return;
            }
            // An engine does not retry: its rank fails to start. Counted, then
            // retried so the scenario keeps its fleet size.
            g_totals.mount_failed.fetch_add(1);
            LOG(ERROR) << "client " << index_ << ": MountSegment failed after "
                       << took << " s: " << toString(result.error());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void HeartbeatLoop() {
        UnsetCpuAffinity();
        int fail_count = 0;
        int64_t last_ok_ns = NowNs();
        const auto interval = std::chrono::milliseconds(FLAGS_ping_interval_ms);
        while (alive_.load()) {
            if (remount_future_.valid() &&
                remount_future_.wait_for(std::chrono::seconds(0)) ==
                    std::future_status::ready) {
                remount_future_ = std::future<void>();
            }

            const auto t0 = Clock::now();
            auto result = master_client_.Ping();
            const auto t1 = Clock::now();
            const uint64_t took_us =
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                    .count();

            if (!result.has_value()) {
                g_window.ping_fail.fetch_add(1);
                ++fail_count;
                if (fail_count >= 3) {
                    g_totals.reconnects.fetch_add(1);
                    auto ec = master_client_.Connect(FLAGS_master_server);
                    if (ec == ErrorCode::OK) {
                        fail_count = 0;
                    }
                }
                std::this_thread::sleep_for(interval);
                continue;
            }

            fail_count = 0;
            const int64_t now_ns = NowNs();
            g_window.ping_ok.fetch_add(1);
            StoreMax(g_window.ping_max_us, took_us);
            StoreMax(g_totals.max_ping_us, took_us);
            StoreMax(g_totals.max_client_silence_us,
                     static_cast<uint64_t>((now_ns - last_ok_ns) / 1000));
            last_ok_ns = now_ns;
            const int64_t fleet_last =
                g_totals.last_ping_ok_ns.exchange(now_ns);
            if (fleet_last != 0 && now_ns > fleet_last) {
                StoreMax(g_totals.max_fleet_ping_gap_us,
                         static_cast<uint64_t>((now_ns - fleet_last) / 1000));
            }

            if (result.value().client_status ==
                mooncake::ClientStatus::NEED_REMOUNT) {
                if (ok_.exchange(false)) {
                    g_totals.declared_offline.fetch_add(1);
                    LOG(WARNING) << "client " << index_
                                 << ": declared offline while heartbeating";
                }
                if (!remount_future_.valid()) {
                    StartRemount();
                }
            } else if (!ok_.exchange(true)) {
                if (!joined_.exchange(true)) {
                    const double took_s =
                        std::chrono::duration<double>(t1 - join_start_).count();
                    if (joined_late_) {
                        std::lock_guard<std::mutex> lock(g_join_mutex);
                        g_join_latency_s.push_back(took_s);
                        LOG(INFO) << "client " << index_ << ": joined in "
                                  << took_s << " s";
                    }
                }
            }
            std::this_thread::sleep_for(interval);
        }
    }

    void StartRemount() {
        g_totals.remount_started.fetch_add(1);
        const bool first = !remounted_once_.exchange(true);
        remount_future_ = std::async(std::launch::async, [this, first] {
            // Like the store client, remount what is mounted so far: nothing
            // on the first heartbeat of a client that has not mounted yet.
            std::vector<mooncake::Segment> segments;
            if (mounted_.load()) {
                segments.push_back(segment_);
            }
            const auto t0 = Clock::now();
            auto result = master_client_.ReMountSegment(segments);
            const double took =
                std::chrono::duration<double>(Clock::now() - t0).count();
            if (result.has_value()) {
                g_totals.remount_ok.fetch_add(1);
            } else {
                g_totals.remount_failed.fetch_add(1);
            }
            if (first && joined_late_) {
                std::lock_guard<std::mutex> lock(g_join_mutex);
                g_first_remount_s.push_back(took);
            }
        });
    }

    const size_t index_;
    const bool joined_late_;
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
    std::atomic<bool> alive_{false};
    std::atomic<bool> ok_{false};
    std::atomic<bool> joined_{false};
    std::atomic<bool> mounted_{false};
    std::atomic<bool> remounted_once_{false};
    Clock::time_point join_start_;
    std::thread ping_thread_;
    std::thread mount_thread_;
    std::future<void> remount_future_;
};

// Data-plane traffic from a client that owns no segment.
class LoadClient {
   public:
    explicit LoadClient(uint64_t stream)
        : stream_(stream), master_client_(mooncake::generate_uuid()) {}

    bool Connect() {
        return master_client_.Connect(FLAGS_master_server) == ErrorCode::OK;
    }

    // Puts `count` new objects; returns how many the master accepted.
    uint64_t Put(uint64_t count) {
        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> slices(count, {FLAGS_value_size});
        keys.reserve(count);
        for (uint64_t i = 0; i < count; ++i) {
            keys.push_back(MakeKey(stream_, next_key_++));
        }
        const auto t0 = Clock::now();
        auto started = master_client_.BatchPutStart(keys, slices, config_);
        std::vector<mooncake::ObjectMeta> metas;
        metas.reserve(count);
        for (size_t i = 0; i < keys.size(); ++i) {
            if (started[i].has_value()) {
                metas.emplace_back(mooncake::ObjectMeta{keys[i], std::nullopt});
            }
        }
        uint64_t done = 0;
        if (!metas.empty()) {
            auto ended = master_client_.BatchPutEnd(metas);
            for (auto& result : ended) {
                if (result.has_value()) {
                    ++done;
                }
            }
        }
        RecordRpc(t0);
        if (done == 0) {
            g_window.put_failed_batches.fetch_add(1);
        }
        g_window.put_items.fetch_add(done);
        g_totals.keys_created.fetch_add(done);
        return done;
    }

    // Half of the keys exist already, half do not, as a save path sees them.
    void Exist(uint64_t count) {
        if (count == 0) {
            return;
        }
        std::vector<std::string> keys;
        keys.reserve(count);
        std::uniform_int_distribution<uint64_t> pick(
            0, next_key_ == 0 ? 0 : next_key_ - 1);
        for (uint64_t i = 0; i < count; ++i) {
            if ((i & 1) == 0 && next_key_ > 0) {
                keys.push_back(MakeKey(stream_, pick(rng_)));
            } else {
                keys.push_back(
                    MakeKey(stream_ ^ 0x5555555555555555ULL, missing_key_++));
            }
        }
        const auto t0 = Clock::now();
        auto results = master_client_.BatchExistKey(keys);
        RecordRpc(t0);
        uint64_t answered = 0;
        for (auto& result : results) {
            if (result.has_value()) {
                ++answered;
            }
        }
        if (answered == 0) {
            g_window.exist_failed_batches.fetch_add(1);
        }
        g_window.exist_items.fetch_add(answered);
    }

   private:
    void RecordRpc(Clock::time_point t0) {
        const uint64_t took_us =
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() -
                                                                  t0)
                .count();
        StoreMax(g_window.rpc_max_us, took_us);
        StoreMax(g_totals.max_rpc_us, took_us);
    }

    const uint64_t stream_;
    mooncake::MasterClient master_client_;
    const mooncake::ReplicateConfig config_{};
    uint64_t next_key_{0};
    uint64_t missing_key_{0};
    std::mt19937_64 rng_{std::random_device{}()};
};

struct Event {
    double at_s;
    std::string kind;
    uint64_t count;
};

std::vector<Event> ParseEvents(const std::string& spec) {
    std::vector<Event> events;
    std::stringstream stream(spec);
    std::string item;
    while (std::getline(stream, item, ',')) {
        if (item.empty()) {
            continue;
        }
        Event event;
        char kind[16] = {0};
        unsigned long long count = 0;
        if (sscanf(item.c_str(), "%lf:%15[a-z]:%llu", &event.at_s, kind,
                   &count) != 3 ||
            (std::string(kind) != "kill" && std::string(kind) != "join")) {
            LOG(FATAL) << "bad event '" << item << "'";
        }
        event.kind = kind;
        event.count = count;
        events.push_back(event);
    }
    std::sort(events.begin(), events.end(),
              [](const Event& a, const Event& b) { return a.at_s < b.at_s; });
    return events;
}

// What a kubelet tcpSocket probe sees: can a connection be opened in 3 s?
void TcpProbeLoop() {
    UnsetCpuAffinity();
    const auto colon = FLAGS_master_server.rfind(':');
    const std::string host = FLAGS_master_server.substr(0, colon);
    const std::string port = FLAGS_master_server.substr(colon + 1);
    uint64_t streak = 0;
    while (!g_stop.load()) {
        const auto t0 = Clock::now();
        int64_t took_ms = -1;
        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* info = nullptr;
        if (getaddrinfo(host.c_str(), port.c_str(), &hints, &info) == 0 &&
            info != nullptr) {
            int fd = socket(info->ai_family, info->ai_socktype | SOCK_NONBLOCK,
                            info->ai_protocol);
            if (fd >= 0) {
                int rc = connect(fd, info->ai_addr, info->ai_addrlen);
                if (rc != 0 && errno == EINPROGRESS) {
                    pollfd waiter{fd, POLLOUT, 0};
                    if (poll(&waiter, 1, 3000) == 1) {
                        int err = 0;
                        socklen_t len = sizeof(err);
                        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
                        rc = err == 0 ? 0 : -1;
                    } else {
                        rc = -1;
                    }
                }
                if (rc == 0) {
                    took_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            Clock::now() - t0)
                            .count();
                }
                close(fd);
            }
            freeaddrinfo(info);
        }
        g_totals.tcp_probe_ms.store(took_ms);
        if (took_ms < 0) {
            g_totals.tcp_probe_fail.fetch_add(1);
            StoreMax(g_totals.tcp_probe_fail_streak_max, ++streak);
        } else {
            streak = 0;
        }
        std::this_thread::sleep_until(t0 + std::chrono::seconds(1));
    }
}

void HandleSignal(int) { g_stop.store(true); }

double Percentile(std::vector<double> values, double q) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(q * (values.size() - 1));
    return values[index];
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging("MasterChurnBench");
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    g_start = Clock::now();

    const auto events = ParseEvents(FLAGS_events);

    std::mutex clients_mutex;
    std::vector<std::unique_ptr<SimClient>> clients;
    auto add_clients = [&](uint64_t count, bool late) {
        std::vector<SimClient*> started;
        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            for (uint64_t i = 0; i < count; ++i) {
                clients.push_back(
                    std::make_unique<SimClient>(clients.size(), late));
                started.push_back(clients.back().get());
            }
        }
        for (auto* client : started) {
            client->Start();
        }
    };

    LOG(INFO) << "Starting " << FLAGS_num_clients << " clients...";
    add_clients(FLAGS_num_clients, /*late=*/false);
    // Let every client register and mount before the fill.
    for (int waited = 0; waited < 120 && !g_stop.load(); ++waited) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (g_totals.mount_ok.load() >= FLAGS_num_clients) {
            break;
        }
    }
    LOG(INFO) << "Mounted " << g_totals.mount_ok.load() << " segments";

    // Fill.
    if (FLAGS_fill_keys > 0) {
        LOG(INFO) << "Filling to " << FLAGS_fill_keys << " objects with "
                  << FLAGS_fill_threads << " threads...";
        std::vector<std::thread> fillers;
        for (uint64_t t = 0; t < FLAGS_fill_threads; ++t) {
            fillers.emplace_back([t] {
                UnsetCpuAffinity();
                LoadClient client(1000 + t);
                if (!client.Connect()) {
                    LOG(ERROR) << "fill client " << t << " cannot connect";
                    return;
                }
                while (!g_stop.load() &&
                       g_totals.keys_created.load() < FLAGS_fill_keys) {
                    if (client.Put(FLAGS_fill_batch) == 0) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(50));
                    }
                }
            });
        }
        uint64_t last = 0;
        while (!g_stop.load() &&
               g_totals.keys_created.load() < FLAGS_fill_keys) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            const uint64_t now = g_totals.keys_created.load();
            LOG(INFO) << "fill: " << now << " objects (" << (now - last) / 5
                      << "/s)";
            last = now;
        }
        for (auto& filler : fillers) {
            filler.join();
        }
        LOG(INFO) << "Fill done: " << g_totals.keys_created.load()
                  << " objects";
    }

    // Steady load, heartbeats and events.
    std::vector<std::thread> loaders;
    for (uint64_t t = 0; t < FLAGS_load_threads; ++t) {
        loaders.emplace_back([t] {
            UnsetCpuAffinity();
            LoadClient client(5000 + t);
            if (!client.Connect()) {
                LOG(ERROR) << "load client " << t << " cannot connect";
                return;
            }
            const auto period = std::chrono::microseconds(FLAGS_load_period_us);
            while (!g_stop.load()) {
                const auto t0 = Clock::now();
                client.Exist(FLAGS_load_exist_batch);
                if (FLAGS_load_put_batch > 0) {
                    client.Put(FLAGS_load_put_batch);
                }
                std::this_thread::sleep_until(t0 + period);
            }
        });
    }
    std::thread probe(TcpProbeLoop);

    std::ofstream report;
    if (!FLAGS_report.empty()) {
        report.open(FLAGS_report);
    }
    const std::string header =
        "t_s\tevent\talive\tok\tping_ok\tping_fail\tping_max_ms\tput_items\t"
        "put_fail_batches\texist_items\texist_fail_batches\trpc_max_ms\t"
        "tcp_probe_ms\tremount_started\tremount_ok\tremount_failed\t"
        "mount_ok\tmount_failed\tdeclared_offline";
    std::cout << header << std::endl;
    if (report.is_open()) {
        report << header << std::endl;
    }

    // The events are timed from here; the fleet-wide gap counts from here too.
    g_totals.last_ping_ok_ns.store(NowNs());
    g_totals.max_fleet_ping_gap_us.store(0);
    g_totals.max_client_silence_us.store(0);
    g_totals.max_ping_us.store(0);
    g_totals.max_rpc_us.store(0);
    const auto scenario_start = Clock::now();
    size_t next_event = 0;
    uint64_t zero_ping_seconds = 0;
    uint64_t zero_ping_streak = 0;
    uint64_t zero_ping_streak_max = 0;
    uint64_t zero_rpc_seconds = 0;
    for (uint64_t second = 1; second <= FLAGS_duration && !g_stop.load();
         ++second) {
        std::this_thread::sleep_until(scenario_start +
                                      std::chrono::seconds(second));
        std::string fired;
        while (next_event < events.size() &&
               events[next_event].at_s <= static_cast<double>(second)) {
            const auto& event = events[next_event++];
            fired += (fired.empty() ? "" : "+") + event.kind + ":" +
                     std::to_string(event.count);
            if (event.kind == "kill") {
                // Off the report thread: a kill joins heartbeat threads.
                std::vector<SimClient*> victims;
                {
                    std::lock_guard<std::mutex> lock(clients_mutex);
                    for (auto& client : clients) {
                        if (victims.size() >= event.count) {
                            break;
                        }
                        if (client->alive()) {
                            victims.push_back(client.get());
                        }
                    }
                }
                std::thread([victims] {
                    for (auto* victim : victims) {
                        victim->Kill();
                    }
                }).detach();
            } else {
                std::thread([&add_clients, count = event.count] {
                    add_clients(count, /*late=*/true);
                }).detach();
            }
        }

        size_t alive = 0;
        size_t ok = 0;
        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            for (auto& client : clients) {
                if (client->alive()) {
                    ++alive;
                    if (client->ok()) {
                        ++ok;
                    }
                }
            }
        }
        const uint64_t ping_ok = g_window.ping_ok.exchange(0);
        const uint64_t ping_fail = g_window.ping_fail.exchange(0);
        const uint64_t ping_max_us = g_window.ping_max_us.exchange(0);
        const uint64_t put_items = g_window.put_items.exchange(0);
        const uint64_t put_fail = g_window.put_failed_batches.exchange(0);
        const uint64_t exist_items = g_window.exist_items.exchange(0);
        const uint64_t exist_fail = g_window.exist_failed_batches.exchange(0);
        const uint64_t rpc_max_us = g_window.rpc_max_us.exchange(0);
        if (ping_ok == 0) {
            ++zero_ping_seconds;
            zero_ping_streak_max =
                std::max(zero_ping_streak_max, ++zero_ping_streak);
        } else {
            zero_ping_streak = 0;
        }
        if (put_items == 0 && exist_items == 0) {
            ++zero_rpc_seconds;
        }
        std::ostringstream line;
        line << second << '\t' << (fired.empty() ? "-" : fired) << '\t' << alive
             << '\t' << ok << '\t' << ping_ok << '\t' << ping_fail << '\t'
             << std::fixed << std::setprecision(1) << ping_max_us / 1000.0
             << '\t' << put_items << '\t' << put_fail << '\t' << exist_items
             << '\t' << exist_fail << '\t' << rpc_max_us / 1000.0 << '\t'
             << g_totals.tcp_probe_ms.load() << '\t'
             << g_totals.remount_started.load() << '\t'
             << g_totals.remount_ok.load() << '\t'
             << g_totals.remount_failed.load() << '\t'
             << g_totals.mount_ok.load() << '\t' << g_totals.mount_failed.load()
             << '\t' << g_totals.declared_offline.load();
        std::cout << line.str() << std::endl;
        if (report.is_open()) {
            report << line.str() << std::endl;
        }
    }

    g_stop.store(true);
    for (auto& loader : loaders) {
        loader.join();
    }
    probe.join();

    std::vector<double> joins;
    std::vector<double> mounts;
    std::vector<double> remounts;
    {
        std::lock_guard<std::mutex> lock(g_join_mutex);
        joins = g_join_latency_s;
        mounts = g_mount_latency_s;
        remounts = g_first_remount_s;
    }
    std::ostringstream summary;
    summary << std::fixed << std::setprecision(2);
    summary << "SUMMARY"
            << " keys_created=" << g_totals.keys_created.load()
            << " zero_ping_seconds=" << zero_ping_seconds
            << " zero_ping_streak_max_s=" << zero_ping_streak_max
            << " zero_rpc_seconds=" << zero_rpc_seconds
            << " max_fleet_ping_gap_s="
            << g_totals.max_fleet_ping_gap_us.load() / 1e6
            << " max_client_silence_s="
            << g_totals.max_client_silence_us.load() / 1e6
            << " max_ping_s=" << g_totals.max_ping_us.load() / 1e6
            << " max_rpc_s=" << g_totals.max_rpc_us.load() / 1e6
            << " declared_offline=" << g_totals.declared_offline.load()
            << " tcp_probe_fail=" << g_totals.tcp_probe_fail.load()
            << " tcp_probe_fail_streak_max="
            << g_totals.tcp_probe_fail_streak_max.load()
            << " mount_failed=" << g_totals.mount_failed.load()
            << " remount_failed=" << g_totals.remount_failed.load()
            << " reconnects=" << g_totals.reconnects.load()
            << " late_joins=" << joins.size()
            << " join_p50_s=" << Percentile(joins, 0.5)
            << " join_max_s=" << Percentile(joins, 1.0)
            << " late_mount_max_s=" << Percentile(mounts, 1.0)
            << " late_first_remount_max_s=" << Percentile(remounts, 1.0);
    std::cout << summary.str() << std::endl;
    if (report.is_open()) {
        report << summary.str() << std::endl;
    }

    // Heartbeat threads may sit in an RPC that the master never answers.
    std::cout.flush();
    _exit(0);
}
