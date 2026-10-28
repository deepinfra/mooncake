// KV event snapshot: MasterService::GetKvSnapshotPage and
// GET /kv_events/snapshot. Built only with ENABLE_KV_EVENTS.

#include <endian.h>
#include <gtest/gtest.h>
#include <msgpack.hpp>
#include <zmq.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <ylt/coro_http/coro_http_client.hpp>

#include "common/network.h"
#include "master_admin_service.h"
#include "master_service_test_fixture.h"
#include "rpc_service.h"

namespace mooncake::test {
namespace {

// Object key -> media holding it.
using ObjectMedia = std::map<std::string, std::set<std::string>>;

std::string MakeIpcEndpoint() {
    static std::atomic<uint64_t> counter{0};
    return "ipc:///tmp/kv_snapshot_test_" + std::to_string(getpid()) + "_" +
           std::to_string(counter.fetch_add(1));
}

std::string MsgpackString(const msgpack::object& value) {
    return value.type == msgpack::type::STR
               ? std::string(value.via.str.ptr, value.via.str.size)
               : std::string{};
}

const msgpack::object* FindMapValue(const msgpack::object& map,
                                    const std::string& field) {
    if (map.type != msgpack::type::MAP) {
        return nullptr;
    }
    for (uint32_t i = 0; i < map.via.map.size; ++i) {
        const auto& key = map.via.map.ptr[i].key;
        if (key.type == msgpack::type::STR &&
            std::string(key.via.str.ptr, key.via.str.size) == field) {
            return &map.via.map.ptr[i].val;
        }
    }
    return nullptr;
}

struct ObjectEvent {
    std::string type;
    std::string object_key;
    std::string medium;
};

struct EventBatch {
    uint64_t sequence;
    std::vector<ObjectEvent> events;
};

void Apply(ObjectMedia& state, const EventBatch& batch) {
    for (const auto& event : batch.events) {
        if (event.type == "stored") {
            state[event.object_key].insert(event.medium);
        } else if (event.type == "removed") {
            auto it = state.find(event.object_key);
            if (it != state.end()) {
                it->second.erase(event.medium);
                if (it->second.empty()) {
                    state.erase(it);
                }
            }
        } else if (event.type == "cleared") {
            state.clear();
        }
    }
}

// Every batch one SUB socket receives, in order.
class EventLog {
   public:
    explicit EventLog(const std::string& endpoint) {
        context_ = zmq_ctx_new();
        socket_ = zmq_socket(context_, ZMQ_SUB);
        int timeout_ms = 20;
        zmq_setsockopt(socket_, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms));
        int hwm = 0;
        zmq_setsockopt(socket_, ZMQ_RCVHWM, &hwm, sizeof(hwm));
        zmq_connect(socket_, endpoint.c_str());
        zmq_setsockopt(socket_, ZMQ_SUBSCRIBE, "", 0);
        thread_ = std::thread([this] { Run(); });
        // A SUB socket misses what is published before its subscription
        // reaches the publisher.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    ~EventLog() {
        stop_ = true;
        thread_.join();
        zmq_close(socket_);
        zmq_ctx_destroy(context_);
    }

    std::vector<EventBatch> Batches() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return batches_;
    }

    size_t Count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return batches_.size();
    }

    size_t Malformed() const { return malformed_.load(); }

   private:
    void Run() {
        while (!stop_) {
            std::vector<std::string> frames;
            if (!Receive(frames)) {
                continue;
            }
            if (frames.size() != 3 || frames[1].size() != sizeof(uint64_t)) {
                ++malformed_;
                continue;
            }
            uint64_t sequence_be;
            std::memcpy(&sequence_be, frames[1].data(), sizeof(sequence_be));
            EventBatch batch{be64toh(sequence_be), {}};
            auto handle = msgpack::unpack(frames[2].data(), frames[2].size());
            const auto& events = handle.get().via.array.ptr[1];
            for (uint32_t i = 0; i < events.via.array.size; ++i) {
                const auto& event = events.via.array.ptr[i];
                const auto* type = FindMapValue(event, "event_type");
                const auto* key = FindMapValue(event, "object_key");
                const auto* medium = FindMapValue(event, "medium");
                batch.events.push_back({type ? MsgpackString(*type) : "",
                                        key ? MsgpackString(*key) : "",
                                        medium ? MsgpackString(*medium) : ""});
            }
            std::lock_guard<std::mutex> lock(mutex_);
            batches_.push_back(std::move(batch));
        }
    }

    bool Receive(std::vector<std::string>& frames) {
        while (true) {
            zmq_msg_t message;
            zmq_msg_init(&message);
            if (zmq_msg_recv(&message, socket_, 0) < 0) {
                zmq_msg_close(&message);
                return false;
            }
            const char* data = static_cast<const char*>(zmq_msg_data(&message));
            frames.emplace_back(data, data + zmq_msg_size(&message));
            const bool more = zmq_msg_more(&message);
            zmq_msg_close(&message);
            if (!more) {
                return true;
            }
        }
    }

    void* context_{nullptr};
    void* socket_{nullptr};
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<size_t> malformed_{0};
    mutable std::mutex mutex_;
    std::vector<EventBatch> batches_;
};

std::string GroupId(const std::string& key) {
    return "vllm-mooncake-store:model@" + key;
}

ReplicateConfig GroupConfig(const std::string& key) {
    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{GroupId(key)};
    return config;
}

// Puts and removes keys of `pool` at random until `stop`.
void Churn(MasterService& service, const UUID& client_id,
           const std::vector<std::string>& pool, const std::atomic<bool>& stop,
           uint32_t seed) {
    std::mt19937 rng(seed);
    while (!stop.load(std::memory_order_relaxed)) {
        const auto& key = pool[rng() % pool.size()];
        if (rng() % 3 == 0) {
            (void)service.Remove(key, TenantId::Default());
            continue;
        }
        if (service.PutStart(client_id, key, TenantId::Default(), 1024,
                             GroupConfig(key))) {
            (void)service.PutEnd(client_id, key, TenantId::Default(),
                                 ReplicaType::MEMORY);
        }
    }
}

struct Snapshot {
    uint64_t first_sequence{0};
    uint64_t last_sequence{0};
    size_t pages{0};
    std::vector<MasterService::KvSnapshotObject> objects;
};

Snapshot ReadSnapshot(const MasterService& service, size_t max_objects,
                      const TenantId& tenant_id = TenantId::Default(),
                      bool with_keys = true) {
    Snapshot snapshot;
    std::optional<size_t> shard = 0;
    while (shard) {
        auto page = service.GetKvSnapshotPage(tenant_id, *shard, max_objects,
                                              with_keys);
        EXPECT_TRUE(page.has_value());
        if (!page) {
            break;
        }
        if (snapshot.pages++ == 0) {
            snapshot.first_sequence = page->next_sequence;
        }
        snapshot.last_sequence = page->next_sequence;
        if (page->next_shard) {
            EXPECT_GT(*page->next_shard, *shard);
            // Only the last page may stop short of max_objects.
            EXPECT_GE(page->objects.size(), max_objects);
        }
        for (auto& object : page->objects) {
            snapshot.objects.push_back(std::move(object));
        }
        shard = page->next_shard;
    }
    return snapshot;
}

ObjectMedia StateOf(const Snapshot& snapshot) {
    ObjectMedia state;
    for (const auto& object : snapshot.objects) {
        EXPECT_FALSE(state.contains(object.object_key))
            << object.object_key << " is on two pages";
        state[object.object_key] = {object.media.begin(), object.media.end()};
    }
    return state;
}

size_t Differences(const ObjectMedia& a, const ObjectMedia& b) {
    size_t differences = 0;
    for (const auto& [key, media] : a) {
        auto it = b.find(key);
        differences += it == b.end() || it->second != media;
    }
    for (const auto& [key, media] : b) {
        differences += !a.contains(key);
    }
    return differences;
}

}  // namespace

class KvSnapshotTest : public MasterServiceTest {
   protected:
    // What the master holds now, read key by key.
    ObjectMedia Truth(MasterService& service,
                      const std::vector<std::string>& pool) {
        ObjectMedia state;
        for (const auto& key : pool) {
            auto media = KvMediaForKey(service, key);
            if (media && !media->empty()) {
                state[key] = {media->begin(), media->end()};
            }
        }
        return state;
    }

    static MasterServiceConfig EventsConfig(const std::string& endpoint) {
        MasterServiceConfig config = MasterServiceConfig::builder().build();
        config.enable_kv_events = true;
        config.kv_events_bind_endpoint = endpoint;
        config.kv_events_backend_id = "kv-snapshot-test";
        return config;
    }
};

TEST_F(KvSnapshotTest, PagesListEveryObjectWithAMediumOnce) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    std::map<std::string, std::string> expected;  // key -> group id
    for (int i = 0; i < 3000; ++i) {
        const std::string key = "listed_" + std::to_string(i);
        PutCompletedObject(service, context.client_id, key, GroupConfig(key));
        expected[key] = GroupId(key);
    }
    for (int i = 0; i < 300; ++i) {
        const std::string key = "listed_" + std::to_string(i * 7);
        ASSERT_TRUE(service.Remove(key, TenantId::Default()).has_value());
        expected.erase(key);
    }
    // An object still being written has no medium yet.
    for (int i = 0; i < 200; ++i) {
        const std::string key = "unfinished_" + std::to_string(i);
        ASSERT_TRUE(service
                        .PutStart(context.client_id, key, TenantId::Default(),
                                  1024, GroupConfig(key))
                        .has_value());
    }

    for (size_t max_objects : {1, 97, 5000}) {
        const auto snapshot = ReadSnapshot(service, max_objects);
        EXPECT_EQ(snapshot.first_sequence, 0u) << "KV events are off";
        std::map<std::string, std::string> listed;
        for (const auto& object : snapshot.objects) {
            listed[object.object_key] = object.group_id;
            EXPECT_EQ(object.media, std::vector<std::string>{"cpu"});
            EXPECT_EQ(object.media, KvMediaForKey(service, object.object_key));
        }
        EXPECT_EQ(listed, expected) << "max_objects=" << max_objects;
        EXPECT_EQ(snapshot.objects.size(), expected.size());
    }
    EXPECT_EQ(ReadSnapshot(service, 1 << 20).pages, 1u);

    std::vector<std::string> groups;
    for (const auto& object :
         ReadSnapshot(service, 97, TenantId::Default(), false).objects) {
        EXPECT_TRUE(object.object_key.empty());
        groups.push_back(object.group_id);
    }
    std::vector<std::string> expected_groups;
    for (const auto& [key, group] : expected) {
        expected_groups.push_back(group);
    }
    std::sort(groups.begin(), groups.end());
    std::sort(expected_groups.begin(), expected_groups.end());
    EXPECT_EQ(groups, expected_groups);

    EXPECT_EQ(service.GetKvSnapshotPage(TenantId::Default(), 1024, 10).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(service.GetKvSnapshotPage(TenantId::Default(), 0, 0).error(),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(KvSnapshotTest, PagesHoldOnlyTheRequestedTenant) {
    const TenantId tenant_a("kv_snapshot_tenant_a");
    MasterService service(MakeStrictTenantConfig(
        {std::string(TenantId::kDefaultValue), tenant_a.value()}));
    const auto context = PrepareSimpleSegment(service);
    PutCompletedObject(service, context.client_id, "shared", GroupConfig("s"));
    PutCompletedObject(service, context.client_id, "default_only",
                       GroupConfig("d"));
    PutCompletedObject(service, context.client_id, "shared", tenant_a,
                       GroupConfig("s"));
    PutCompletedObject(service, context.client_id, "tenant_only", tenant_a,
                       GroupConfig("t"));

    auto keys = [](const Snapshot& snapshot) {
        std::set<std::string> keys;
        for (const auto& object : snapshot.objects) {
            keys.insert(object.object_key);
        }
        return keys;
    };
    EXPECT_EQ(keys(ReadSnapshot(service, 1)),
              (std::set<std::string>{"shared", "default_only"}));
    EXPECT_EQ(keys(ReadSnapshot(service, 1, tenant_a)),
              (std::set<std::string>{"shared", "tenant_only"}));
}

// The contract: pages plus every event batch from the first page's
// next_sequence on end at the master's state, while writers change objects
// during the read.
TEST_F(KvSnapshotTest, EventsFromTheFirstPageSequenceBringPagesUpToDate) {
    constexpr int kRounds = 6;
    constexpr int kWriters = 4;
    size_t late_fence_misses = 0;
    for (int round = 0; round < kRounds; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        const auto endpoint = MakeIpcEndpoint();
        MasterService service(EventsConfig(endpoint));
        // Smaller than the key pool, so eviction removes objects as well.
        const auto context = PrepareSimpleSegment(
            service, "churn_segment", kDefaultSegmentBase, 3 * 1024 * 1024);
        EventLog log(endpoint);
        std::vector<std::string> pool;
        for (int i = 0; i < 4000; ++i) {
            pool.push_back("churn_" + std::to_string(i));
        }

        std::atomic<bool> stop{false};
        std::vector<std::thread> writers;
        for (int w = 0; w < kWriters; ++w) {
            writers.emplace_back(Churn, std::ref(service), context.client_id,
                                 std::cref(pool), std::cref(stop),
                                 round * kWriters + w);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        // Small pages spread the read over time, while writers run.
        const auto snapshot = ReadSnapshot(service, 2 + round * 3);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        stop = true;
        for (auto& writer : writers) {
            writer.join();
        }

        // Background eviction goes on after the writers stop: wait until the
        // master is quiet and everything it published has arrived.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        uint64_t published = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            published = service.GetKvEventStats().published_batches;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            if (published == service.GetKvEventStats().published_batches &&
                log.Count() == published) {
                break;
            }
        }
        const auto truth = Truth(service, pool);
        const auto stats = service.GetKvEventStats();
        const auto batches = log.Batches();
        ASSERT_EQ(stats.published_batches, published) << "never went quiet";
        ASSERT_EQ(stats.dropped_events, 0u);
        ASSERT_EQ(log.Malformed(), 0u);
        ASSERT_EQ(batches.size(), stats.published_batches);
        ASSERT_FALSE(batches.empty());
        ASSERT_EQ(batches.front().sequence, 1u)
            << "the subscriber missed the first batches";
        for (size_t i = 1; i < batches.size(); ++i) {
            ASSERT_EQ(batches[i].sequence, batches[i - 1].sequence + 1);
        }

        ASSERT_FALSE(truth.empty());
        // The stream alone, from an empty index, must reach the state: else
        // the events, not the snapshot, are wrong.
        ObjectMedia replayed;
        for (const auto& batch : batches) {
            Apply(replayed, batch);
        }
        ASSERT_EQ(Differences(replayed, truth), 0u);

        ObjectMedia state = StateOf(snapshot);
        const size_t stale_pages = Differences(state, truth);
        size_t applied = 0;
        for (const auto& batch : batches) {
            if (batch.sequence >= snapshot.first_sequence) {
                Apply(state, batch);
                ++applied;
            }
        }
        EXPECT_EQ(Differences(state, truth), 0u)
            << "first_sequence=" << snapshot.first_sequence
            << " pages=" << snapshot.pages << " applied=" << applied;
        // The writers changed what the pages hold, so the events had work.
        EXPECT_GT(stale_pages, 0u);
        EXPECT_GT(applied, 0u);

        // A fence read at the last page instead loses changes made earlier.
        ObjectMedia late = StateOf(snapshot);
        for (const auto& batch : batches) {
            if (batch.sequence >= snapshot.last_sequence) {
                Apply(late, batch);
            }
        }
        late_fence_misses += Differences(late, truth);
        LOG(INFO) << "round " << round << ": " << snapshot.pages << " pages, "
                  << snapshot.objects.size() << " objects, " << batches.size()
                  << " batches (" << applied << " from the fence), "
                  << stale_pages << " stale objects in pages, "
                  << Differences(late, truth) << " wrong with a late fence";
    }
    // Not a property of the code under test; shows this test can fail.
    EXPECT_GT(late_fence_misses, 0u);
}

// The HTTP view of the same pages.
class KvSnapshotHttpTest : public MasterServiceTest {
   protected:
    struct Page {
        std::optional<uint64_t> next_sequence;
        std::optional<uint64_t> next_shard;
        // object key, group id (nil -> none), media
        std::vector<
            std::tuple<std::optional<std::string>, std::optional<std::string>,
                       std::vector<std::string>>>
            objects;
    };

    static std::pair<int, std::string> Get(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto result =
            client.get("http://127.0.0.1:" + std::to_string(port) + path);
        return {result.status, std::string(result.resp_body)};
    }

    static Page Decode(const std::string& body) {
        Page page;
        auto handle = msgpack::unpack(body.data(), body.size());
        const auto& map = handle.get();
        EXPECT_EQ(map.type, msgpack::type::MAP);
        const auto* sequence = FindMapValue(map, "next_sequence");
        const auto* shard = FindMapValue(map, "next_shard");
        const auto* objects = FindMapValue(map, "objects");
        EXPECT_TRUE(sequence && shard && objects);
        if (sequence->type != msgpack::type::NIL) {
            page.next_sequence = sequence->as<uint64_t>();
        }
        if (shard->type != msgpack::type::NIL) {
            page.next_shard = shard->as<uint64_t>();
        }
        for (uint32_t i = 0; i < objects->via.array.size; ++i) {
            const auto& object = objects->via.array.ptr[i];
            EXPECT_EQ(object.via.array.size, 3u);
            const auto& key = object.via.array.ptr[0];
            const auto& group = object.via.array.ptr[1];
            page.objects.emplace_back(
                key.type == msgpack::type::NIL
                    ? std::nullopt
                    : std::optional<std::string>(MsgpackString(key)),
                group.type == msgpack::type::NIL
                    ? std::nullopt
                    : std::optional<std::string>(MsgpackString(group)),
                object.via.array.ptr[2].as<std::vector<std::string>>());
        }
        return page;
    }

    static std::shared_ptr<WrappedMasterService> MakeService(bool kv_events) {
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 5000;
        config.enable_metric_reporting = false;
        config.client_active_ttl_sec = 3600;
        config.enable_kv_events = kv_events;
        config.kv_events_bind_endpoint = MakeIpcEndpoint();
        config.kv_events_backend_id = "kv-snapshot-http-test";
        return std::make_shared<WrappedMasterService>(config);
    }
};

TEST_F(KvSnapshotHttpTest, PagesListTheObjectsAndTheFence) {
    auto service = MakeService(true);
    Segment segment = MakeSegment("http_snapshot_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 50; ++i) {
        const std::string key = "http_" + std::to_string(i);
        ASSERT_TRUE(service->PutStart(client_id, key, 1024, GroupConfig(key))
                        .has_value());
        ASSERT_TRUE(service
                        ->PutEnd(client_id, ObjectMeta{key, std::nullopt},
                                 ReplicaType::MEMORY)
                        .has_value());
        expected[key] = GroupId(key);
    }
    // No group id goes out as nil.
    ReplicateConfig plain;
    plain.replica_num = 1;
    ASSERT_TRUE(
        service->PutStart(client_id, "http_plain", 1024, plain).has_value());
    ASSERT_TRUE(service
                    ->PutEnd(client_id, ObjectMeta{"http_plain", std::nullopt},
                             ReplicaType::MEMORY)
                    .has_value());

    const int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    for (const char* object_keys : {"", "&object_keys=1", "&object_keys=0"}) {
        SCOPED_TRACE(object_keys);
        std::map<std::string, std::string> listed;
        std::vector<std::string> groups;
        std::optional<uint64_t> shard = 0;
        size_t pages = 0;
        while (shard) {
            auto [status, body] =
                Get(port, "/kv_events/snapshot?max_objects=7&shard=" +
                              std::to_string(*shard) + object_keys);
            ASSERT_EQ(status, 200) << body;
            const auto page = Decode(body);
            ASSERT_TRUE(page.next_sequence.has_value());
            EXPECT_GE(*page.next_sequence, 1u);
            for (const auto& [key, group, media] : page.objects) {
                EXPECT_EQ(media, std::vector<std::string>{"cpu"});
                if (std::string(object_keys) == "&object_keys=0") {
                    EXPECT_FALSE(key.has_value());
                    if (group) {
                        groups.push_back(*group);
                    }
                } else if (key && *key == "http_plain") {
                    EXPECT_FALSE(group.has_value());
                } else {
                    ASSERT_TRUE(key && group);
                    listed[*key] = *group;
                }
            }
            shard = page.next_shard;
            ++pages;
        }
        EXPECT_GT(pages, 1u);
        if (std::string(object_keys) == "&object_keys=0") {
            std::vector<std::string> expected_groups;
            for (const auto& [key, group] : expected) {
                expected_groups.push_back(group);
            }
            std::sort(groups.begin(), groups.end());
            EXPECT_EQ(groups, expected_groups);
        } else {
            EXPECT_EQ(listed, expected);
        }
    }

    for (const char* bad :
         {"?shard=x", "?shard=-1", "?shard=1024", "?shard=1%20",
          "?max_objects=0", "?max_objects=1048577", "?object_keys=2",
          "?max_objects=1e3"}) {
        EXPECT_EQ(Get(port, std::string("/kv_events/snapshot") + bad).first,
                  400)
            << bad;
    }
    admin.Stop();
}

TEST_F(KvSnapshotHttpTest, NoSequenceWithoutKvEvents) {
    auto service = MakeService(false);
    const int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);
    auto [status, body] = Get(port, "/kv_events/snapshot");
    ASSERT_EQ(status, 200);
    const auto page = Decode(body);
    EXPECT_FALSE(page.next_sequence.has_value());
    EXPECT_FALSE(page.next_shard.has_value());
    EXPECT_TRUE(page.objects.empty());

    admin.SetServiceAvailable(false);
    EXPECT_EQ(Get(port, "/kv_events/snapshot").first, 503);
    admin.Stop();
}

// What a snapshot of a production-sized store costs: walk time, bytes, page
// latency, and the latency it adds to puts. Run by hand:
//   KV_SNAPSHOT_BENCH_OBJECTS=4000000 kv_snapshot_test \
//     --gtest_also_run_disabled_tests --gtest_filter='*ProductionScale*'
TEST_F(KvSnapshotHttpTest, DISABLED_ProductionScale) {
    const char* env = std::getenv("KV_SNAPSHOT_BENCH_OBJECTS");
    const size_t objects = env ? std::strtoull(env, nullptr, 10) : 4000000;
    using Clock = std::chrono::steady_clock;
    auto ms = [](Clock::duration d) {
        return std::chrono::duration<double, std::milli>(d).count();
    };

    auto service = MakeService(true);
    Segment segment = MakeSegment("bench_segment", kDefaultSegmentBase,
                                  (objects + 100000) * 2048);
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service->MountSegment(segment, client_id).has_value());
    // Key shapes of vLLM's MooncakeStoreConnector.
    auto hex = [](size_t i) {
        char buffer[65];
        std::snprintf(buffer, sizeof(buffer), "%064zx", i * 0x9e3779b97f4a7c15);
        return std::string(buffer);
    };
    auto object_key = [&](size_t i) {
        return "deepseek-ai/DeepSeek-V3.2@tp_rank:0@pcp0@dcp0@pp_rank:0@"
               "group:0@" +
               hex(i);
    };
    const auto fill_start = Clock::now();
    std::vector<std::thread> fillers;
    for (size_t t = 0; t < 16; ++t) {
        fillers.emplace_back([&, t] {
            ReplicateConfig config;
            config.replica_num = 1;
            for (size_t i = t; i < objects; i += 16) {
                config.group_ids = std::vector<std::string>{
                    "vllm-mooncake-store:deepseek-ai/DeepSeek-V3.2@" + hex(i)};
                const auto key = object_key(i);
                if (service->PutStart(client_id, key, 1024, config)) {
                    (void)service->PutEnd(client_id,
                                          ObjectMeta{key, std::nullopt},
                                          ReplicaType::MEMORY);
                }
            }
        });
    }
    for (auto& filler : fillers) {
        filler.join();
    }
    std::cout << "filled " << objects << " objects in "
              << ms(Clock::now() - fill_start) / 1000 << " s" << std::endl;

    // A one-object page reads one shard, so its time bounds that shard's
    // lock hold.
    for (bool with_keys : {true, false}) {
        std::vector<double> shard_ms;
        std::optional<size_t> shard = 0;
        while (shard) {
            const auto start = Clock::now();
            auto page = service->GetKvSnapshotPageForAdmin("default", *shard, 1,
                                                           with_keys);
            shard_ms.push_back(ms(Clock::now() - start));
            ASSERT_TRUE(page.has_value());
            shard = page->next_shard;
        }
        std::sort(shard_ms.begin(), shard_ms.end());
        std::cout << "one shard per page"
                  << (with_keys ? " with keys" : " group ids only") << ": "
                  << shard_ms.size() << " pages, p50 "
                  << shard_ms[shard_ms.size() / 2] << " ms, p99 "
                  << shard_ms[shard_ms.size() * 99 / 100] << " ms, max "
                  << shard_ms.back() << " ms" << std::endl;
    }

    const int port = getFreeTcpPort();
    MasterAdminServer admin(static_cast<uint16_t>(port), false);
    ASSERT_TRUE(admin.Start());
    admin.SetRuntimeState(ha::MasterRuntimeState::kServing);
    admin.SetServiceDelegate(service);
    admin.SetServiceAvailable(true);

    struct Walk {
        size_t objects{0}, pages{0}, bytes{0};
        double total_ms{0}, slowest_page_ms{0}, decode_ms{0};
    };
    auto walk = [&](size_t max_objects, bool keys) {
        Walk result;
        const auto start = Clock::now();
        std::optional<uint64_t> shard = 0;
        while (shard) {
            const auto page_start = Clock::now();
            auto [status, body] =
                Get(port, "/kv_events/snapshot?max_objects=" +
                              std::to_string(max_objects) +
                              "&shard=" + std::to_string(*shard) +
                              (keys ? "" : "&object_keys=0"));
            result.slowest_page_ms =
                std::max(result.slowest_page_ms, ms(Clock::now() - page_start));
            EXPECT_EQ(status, 200);
            const auto decode_start = Clock::now();
            const auto page = Decode(body);
            result.decode_ms += ms(Clock::now() - decode_start);
            result.objects += page.objects.size();
            result.bytes += body.size();
            ++result.pages;
            shard = page.next_shard;
        }
        result.total_ms = ms(Clock::now() - start);
        return result;
    };

    // Puts of fresh keys, one at a time, as a worker's store client does.
    std::atomic<bool> walking{false}, stop{false};
    std::mutex latency_mutex;
    std::vector<double> idle_us, busy_us;
    std::thread writer([&] {
        ReplicateConfig config;
        config.replica_num = 1;
        for (size_t i = objects; !stop; ++i) {
            const auto key = object_key(i);
            const bool during_walk = walking;
            const auto start = Clock::now();
            if (service->PutStart(client_id, key, 1024, config)) {
                (void)service->PutEnd(client_id, ObjectMeta{key, std::nullopt},
                                      ReplicaType::MEMORY);
            }
            (void)service->Remove(key);
            const double us = ms(Clock::now() - start) * 1000;
            std::lock_guard<std::mutex> lock(latency_mutex);
            (during_walk ? busy_us : idle_us).push_back(us);
        }
    });
    std::this_thread::sleep_for(std::chrono::seconds(3));

    for (const auto& [max_objects, keys] : std::vector<std::pair<size_t, bool>>{
             {65536, false}, {65536, true}, {16384, false}, {262144, false}}) {
        walking = true;
        const auto result = walk(max_objects, keys);
        walking = false;
        // The writer's object may be listed while it exists.
        EXPECT_GE(result.objects, objects);
        EXPECT_LE(result.objects, objects + 1);
        std::cout << "max_objects=" << max_objects
                  << (keys ? " with keys" : " group ids only") << ": "
                  << result.pages << " pages, " << result.bytes / 1e6
                  << " MB, total " << result.total_ms << " ms, slowest page "
                  << result.slowest_page_ms << " ms, client decode "
                  << result.decode_ms << " ms" << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    stop = true;
    writer.join();

    auto report = [](const char* name, std::vector<double> us) {
        if (us.empty()) {
            return;
        }
        std::sort(us.begin(), us.end());
        auto at = [&](double q) {
            return us[std::min(us.size() - 1, size_t(q * us.size()))];
        };
        std::cout << name << ": " << us.size() << " put+remove, p50 " << at(0.5)
                  << " us, p99 " << at(0.99) << " us, p99.9 " << at(0.999)
                  << " us, max " << us.back() << " us" << std::endl;
    };
    report("idle", idle_us);
    report("during walks", busy_us);
    admin.Stop();
}

}  // namespace mooncake::test
