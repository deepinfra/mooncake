// Leader-takeover probe for the late-write hazard.
//
//   start: opens --count writes on the current leader (PutStart, never
//          PutEnd) and prints the memory range each one was given, one
//          "endpoint address size" line per range. These are the writes
//          still in flight when the leader goes.
//   check: on the new leader, opens --count writes of the same size and
//          counts how many were given a range that overlaps one printed by
//          `start` (--ranges file), then revokes them. Any overlap is a
//          range a late write from the old term could still land in.
//   remount: mounts a segment of its own (no memory behind it: only the
//          master's allocation is checked), writes objects there and leaves
//          one write open, then pings until the leader changes. On the new
//          leader it remounts as a client would, then dies. A restarted
//          client mounts the same endpoint once the old one is offboarded,
//          writes objects, and remounts as its first ping asks it to. It
//          then counts new allocations on the segment that overlap the
//          restarted client's objects (`overlapping`) or the old term's
//          open write (`overlapping_open`).

#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "master_client.h"
#include "types.h"

DEFINE_string(master_server, "172.31.250.10:50051", "Master address");
DEFINE_string(mode, "start", "start | check");
DEFINE_uint64(count, 256, "Writes to open");
DEFINE_uint64(value_size, 16384, "Bytes per write");
DEFINE_string(ranges, "", "check: the ranges `start` printed");
DEFINE_string(key_prefix, "late-write-probe", "Key prefix");
DEFINE_string(segment_endpoint, "172.31.250.99:1",
              "remount: the probe segment's endpoint");
DEFINE_uint64(segment_mb, 16, "remount: the probe segment's size");
DEFINE_uint64(wait_s, 300, "remount: how long to wait for a new leader");

namespace {

struct Range {
    std::string endpoint;
    uint64_t address{0};
    uint64_t size{0};
};

std::vector<Range> MemoryRanges(
    const std::vector<mooncake::Replica::Descriptor>& replicas) {
    std::vector<Range> out;
    for (const auto& replica : replicas) {
        if (!replica.is_memory_replica()) {
            continue;
        }
        const auto& buffer = replica.get_memory_descriptor().buffer_descriptor;
        out.push_back(
            {buffer.transport_endpoint_, buffer.buffer_address_, buffer.size_});
    }
    return out;
}

bool Overlaps(uint64_t address, uint64_t size,
              const std::vector<std::pair<uint64_t, uint64_t>>& live) {
    for (const auto& [start, length] : live) {
        if (address < start + length && start < address + size) {
            return true;
        }
    }
    return false;
}

// Writes `count` objects on the probe segment; appends their ranges to
// `live`. With `end` false the writes stay open.
size_t WriteOnSegment(mooncake::MasterClient& client,
                      const mooncake::ReplicateConfig& config,
                      const std::vector<size_t>& slices,
                      const std::string& prefix, size_t count, bool end,
                      std::vector<std::pair<uint64_t, uint64_t>>& live) {
    size_t on_segment = 0;
    for (size_t i = 0; i < count; ++i) {
        const std::string key = prefix + "-" + std::to_string(i);
        auto started = client.PutStart(key, slices, config);
        if (!started) {
            std::cerr << "put_start " << key << " "
                      << mooncake::toString(started.error()) << std::endl;
            continue;
        }
        for (const auto& range : MemoryRanges(*started)) {
            if (range.endpoint == FLAGS_segment_endpoint) {
                live.emplace_back(range.address, range.size);
                ++on_segment;
            }
        }
        if (end) {
            (void)client.PutEnd({key, std::nullopt},
                                mooncake::ReplicaType::MEMORY);
        }
    }
    return on_segment;
}

int Remount(mooncake::MasterClient& client, mooncake::ReplicateConfig config,
            const std::vector<size_t>& slices) {
    mooncake::Segment segment;
    segment.id = mooncake::generate_uuid();
    segment.name = FLAGS_segment_endpoint;
    segment.te_endpoint = FLAGS_segment_endpoint;
    segment.base = 0x7e0000000000ULL;
    segment.size = FLAGS_segment_mb << 20;
    segment.protocol = "tcp";
    if (!client.MountSegment(segment)) {
        std::cerr << "mount failed" << std::endl;
        return 2;
    }
    config.preferred_segment = segment.name;
    config.preferred_segments = {segment.name};
    std::vector<std::pair<uint64_t, uint64_t>> live;
    const std::string prefix = FLAGS_key_prefix;
    std::cout << "old_term_objects="
              << WriteOnSegment(client, config, slices, prefix + "-old", 64,
                                true, live)
              << " open="
              << WriteOnSegment(client, config, slices, prefix + "-open", 1,
                                false, live)
              << std::endl;
    auto first = client.Ping();
    const auto view = first ? first->view_version_id : 0;
    std::cout << "VIEW " << view << " READY" << std::endl;

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_wait_s);
    bool remounted = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto ping = client.Ping();
        if (ping && ping->view_version_id != view &&
            ping->client_status == mooncake::ClientStatus::NEED_REMOUNT) {
            remounted = static_cast<bool>(client.ReMountSegment({segment}));
            if (remounted) {
                std::cout << "REMOUNTED view=" << ping->view_version_id
                          << std::endl;
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!remounted) {
        std::cerr << "no new leader" << std::endl;
        return 3;
    }
    const std::vector<std::pair<uint64_t, uint64_t>> open(live.end() - 1,
                                                          live.end());

    // The client dies; a restarted one takes the same endpoint.
    mooncake::MasterClient restarted(mooncake::generate_uuid());
    if (restarted.Connect(FLAGS_master_server) != mooncake::ErrorCode::OK) {
        std::cerr << "restarted connect failed" << std::endl;
        return 2;
    }
    mooncake::Segment again = segment;
    again.id = mooncake::generate_uuid();
    const auto mounted_by =
        std::chrono::steady_clock::now() + std::chrono::seconds(FLAGS_wait_s);
    bool mounted = false;
    while (!mounted && std::chrono::steady_clock::now() < mounted_by) {
        mounted = static_cast<bool>(restarted.MountSegment(again));
        if (!mounted) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
    if (!mounted) {
        std::cerr << "restarted mount failed" << std::endl;
        return 3;
    }
    std::vector<std::pair<uint64_t, uint64_t>> fresh;
    std::cout << "RESTARTED_MOUNTED this_term_objects="
              << WriteOnSegment(restarted, config, slices, prefix + "-new", 64,
                                true, fresh)
              << std::endl;
    auto ping = restarted.Ping();
    const bool needs_remount =
        ping && ping->client_status == mooncake::ClientStatus::NEED_REMOUNT;
    const bool remounted_again =
        static_cast<bool>(restarted.ReMountSegment({again}));
    std::cout << "RESTARTED_REMOUNTED need_remount=" << needs_remount
              << " ok=" << remounted_again << std::endl;
    size_t allocated = 0;
    size_t overlapping = 0;
    size_t overlapping_open = 0;
    // Enough to fill the segment, held until the end: a range the
    // allocator forgot comes back.
    std::vector<std::string> held;
    const size_t fill = (FLAGS_segment_mb << 20) / FLAGS_value_size + 64;
    for (size_t i = 0; i < fill; ++i) {
        const std::string key = prefix + "-check-" + std::to_string(i);
        auto started = restarted.PutStart(key, slices, config);
        if (!started) {
            continue;
        }
        for (const auto& range : MemoryRanges(*started)) {
            if (range.endpoint != FLAGS_segment_endpoint) {
                continue;
            }
            ++allocated;
            overlapping += Overlaps(range.address, range.size, fresh);
            overlapping_open += Overlaps(range.address, range.size, open);
        }
        held.push_back(key);
        if (i % 64 == 0) {
            (void)restarted.Ping();
        }
    }
    for (const auto& key : held) {
        (void)restarted.PutRevoke(key, mooncake::ReplicaType::MEMORY);
    }
    std::cout << "allocated=" << allocated << " overlapping=" << overlapping
              << " overlapping_open=" << overlapping_open << std::endl;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    mooncake::MasterClient client(mooncake::generate_uuid());
    if (client.Connect(FLAGS_master_server) != mooncake::ErrorCode::OK) {
        std::cerr << "connect failed" << std::endl;
        return 2;
    }
    mooncake::ReplicateConfig config;
    config.replica_num = 1;
    const std::vector<size_t> slices{FLAGS_value_size};

    if (FLAGS_mode == "remount") {
        return Remount(client, config, slices);
    }
    if (FLAGS_mode == "start") {
        size_t opened = 0;
        for (uint64_t i = 0; i < FLAGS_count; ++i) {
            auto started = client.PutStart(
                FLAGS_key_prefix + "-" + std::to_string(i), slices, config);
            if (!started) {
                continue;
            }
            for (const auto& range : MemoryRanges(*started)) {
                std::cout << range.endpoint << " " << range.address << " "
                          << range.size << std::endl;
            }
            ++opened;
        }
        std::cerr << "opened " << opened << " writes" << std::endl;
        return opened == 0 ? 1 : 0;
    }

    std::map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> open;
    std::ifstream in(FLAGS_ranges);
    std::string line;
    while (std::getline(in, line)) {
        std::istringstream fields(line);
        Range range;
        if (fields >> range.endpoint >> range.address >> range.size) {
            open[range.endpoint].emplace_back(range.address, range.size);
        }
    }
    size_t allocated = 0;
    size_t overlapping = 0;
    for (uint64_t i = 0; i < FLAGS_count; ++i) {
        const std::string key =
            FLAGS_key_prefix + "-check-" + std::to_string(i);
        auto started = client.PutStart(key, slices, config);
        if (!started) {
            continue;
        }
        ++allocated;
        for (const auto& range : MemoryRanges(*started)) {
            auto ranges = open.find(range.endpoint);
            if (ranges == open.end()) {
                continue;
            }
            for (const auto& [address, size] : ranges->second) {
                if (range.address < address + size &&
                    address < range.address + range.size) {
                    ++overlapping;
                    std::cout << "OVERLAP " << key << " " << range.endpoint
                              << " " << range.address << std::endl;
                    break;
                }
            }
        }
        (void)client.PutRevoke(key, mooncake::ReplicaType::MEMORY);
    }
    std::cout << "allocated=" << allocated << " overlapping=" << overlapping
              << std::endl;
    return 0;
}
