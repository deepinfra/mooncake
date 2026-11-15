// Follower mode: the standby applies the leader's OpLog straight into a
// MasterService index, so promotion activates that index instead of
// rebuilding one from a standby store.

#include <glog/logging.h>

#include <limits>
#include <mutex>
#include <shared_mutex>

#include "allocator.h"
#include "master_service.h"

namespace mooncake {

std::shared_ptr<DummyBufferAllocator> MasterService::FollowerPlaceholderLocked(
    const std::string& endpoint, const std::string& segment_name) {
    auto& slot = standby_allocator_keepalive_[endpoint];
    auto placeholder = std::dynamic_pointer_cast<DummyBufferAllocator>(slot);
    if (!placeholder) {
        placeholder =
            std::make_shared<DummyBufferAllocator>(segment_name, endpoint);
        slot = placeholder;
    }
    return placeholder;
}

tl::expected<std::vector<Replica>, ErrorCode>
MasterService::BuildFollowerReplicas(const StandbyObjectMetadata& metadata) {
    std::vector<Replica> replicas;
    replicas.reserve(metadata.replicas.size());
    std::lock_guard<std::mutex> lock(follower_mutex_);
    for (const auto& desc : metadata.replicas) {
        follower_max_replica_id_ = std::max(follower_max_replica_id_, desc.id);
        if (desc.is_memory_replica()) {
            const auto& buffer = desc.get_memory_descriptor().buffer_descriptor;
            if (buffer.size_ != metadata.size || buffer.size_ == 0 ||
                buffer.buffer_address_ >
                    std::numeric_limits<uintptr_t>::max() - buffer.size_) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto placeholder = FollowerPlaceholderLocked(
                buffer.transport_endpoint_, buffer.transport_endpoint_);
            auto restored = std::make_unique<AllocatedBuffer>(
                std::static_pointer_cast<BufferAllocatorBase>(placeholder),
                buffer);
            if (desc.status != ReplicaStatus::REMOVED &&
                desc.status != ReplicaStatus::FAILED) {
                follower_orphaned_replicas_.fetch_add(
                    placeholder->Track(*restored), std::memory_order_relaxed);
            }
            replicas.push_back(
                Replica(desc.id, std::move(restored), desc.status));
        } else if (desc.is_nof_replica()) {
            const auto& buffer = desc.get_nof_descriptor().buffer_descriptor;
            if (buffer.size_ != metadata.size || buffer.size_ == 0) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto placeholder = FollowerPlaceholderLocked(
                buffer.transport_endpoint_, buffer.transport_endpoint_);
            replicas.push_back(Replica(
                desc.id,
                std::make_unique<AllocatedBuffer>(
                    std::static_pointer_cast<BufferAllocatorBase>(placeholder),
                    buffer),
                desc.status, ReplicaType::NOF_SSD));
        } else if (desc.is_disk_replica()) {
            const auto& disk = desc.get_disk_descriptor();
            if (disk.object_size != metadata.size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            replicas.push_back(Replica(desc.id, disk.file_path,
                                       disk.object_size, desc.status));
        } else if (desc.is_local_disk_replica()) {
            const auto& local = desc.get_local_disk_descriptor();
            if (local.object_size != metadata.size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            auto& record = follower_owner_records_[local.client_id];
            if (!record) {
                record = std::make_shared<ClientLivenessRecord>(
                    ClientLivenessRecord::Clock::now());
            }
            replicas.push_back(
                Replica(desc.id, local.client_id, local.object_size,
                        local.transport_endpoint, desc.status, record));
        } else {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }
    return replicas;
}

tl::expected<void, ErrorCode> MasterService::FollowerPut(
    const std::string& tenant_id, const std::string& key,
    const StandbyObjectMetadata& metadata) {
    if (lifecycle_state_.load(std::memory_order_acquire) !=
        RestoreLifecycleState::kFollowing) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const TenantId tenant(enable_multi_tenants_
                              ? tenant_id
                              : std::string(TenantId::kDefaultValue));
    if (!tenant.IsValid()) {
        follower_rejected_entries_.fetch_add(1, std::memory_order_relaxed);
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(
        this, {tenant, key},
        MetadataAccessorRW::CreateKey{.cleanup_invalid = false});
    if (accessor.IsPublished()) {
        accessor.Erase();
    }
    auto replicas = BuildFollowerReplicas(metadata);
    if (!replicas) {
        // Erasing is always safe: the worst case is a miss.
        follower_rejected_entries_.fetch_add(1, std::memory_order_relaxed);
        LOG(WARNING) << "action=follower_reject_entry, tenant="
                     << tenant.value() << ", key=" << key
                     << ", error=" << toString(replicas.error());
        return {};
    }
    if (replicas->empty()) {
        return {};
    }
    accessor.Publish(metadata.client_id, std::chrono::system_clock::now(),
                     metadata.size, std::move(*replicas), std::nullopt,
                     metadata.hard_pinned.value_or(false), metadata.data_type,
                     metadata.group_id, tenant, key);
    auto& published = accessor.Get();
    published.MutablePart(0).SetChecksum(metadata.object_checksum);
    published.session_id = metadata.session_id.value_or("");
    return {};
}

tl::expected<void, ErrorCode> MasterService::FollowerRemove(
    const std::string& tenant_id, const std::string& key) {
    if (lifecycle_state_.load(std::memory_order_acquire) !=
        RestoreLifecycleState::kFollowing) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const TenantId tenant(enable_multi_tenants_
                              ? tenant_id
                              : std::string(TenantId::kDefaultValue));
    if (!tenant.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this, {tenant, key});
    if (accessor.IsPublished()) {
        accessor.Erase();
    }
    return {};
}

void MasterService::FollowerSegmentMount(const SegmentMountOp& op) {
    if (!op.is_memory_segment || op.transport_endpoint.empty()) {
        return;
    }
    std::unique_lock<std::shared_mutex> exclusive(master_state_mutex_);
    std::lock_guard<std::mutex> lock(follower_mutex_);
    auto& segment = follower_segments_[op.transport_endpoint];
    // A different segment id on the same endpoint is a new client process:
    // nothing restored onto the old one may point into its memory.
    if (segment.segment_id && op.segment_id &&
        *segment.segment_id != op.segment_id.value()) {
        follower_orphaned_replicas_.fetch_add(
            FollowerPlaceholderLocked(op.transport_endpoint, op.segment_name)
                ->OrphanAll(),
            std::memory_order_relaxed);
    }
    segment.info = StandbySegmentInfo{
        .segment_name = op.segment_name,
        .transport_endpoint = op.transport_endpoint,
        .capacity = op.capacity,
        .is_memory_segment = true,
        .file_path = {},
    };
    if (op.segment_id) {
        segment.segment_id = op.segment_id.value();
    }
    auto placeholder =
        FollowerPlaceholderLocked(op.transport_endpoint, op.segment_name);
    if (!op.segment_name.empty() && op.segment_name != op.transport_endpoint) {
        standby_allocator_keepalive_[op.segment_name] = placeholder;
    }
}

void MasterService::FollowerSegmentUnmount(const SegmentUnmountOp& op) {
    std::unique_lock<std::shared_mutex> exclusive(master_state_mutex_);
    std::lock_guard<std::mutex> lock(follower_mutex_);
    auto segment = follower_segments_.find(op.transport_endpoint);
    if (segment != follower_segments_.end() && segment->second.segment_id &&
        op.segment_id && *segment->second.segment_id != op.segment_id.value()) {
        // An unmount of an older incarnation; the live one stays.
        return;
    }
    auto kept = standby_allocator_keepalive_.find(op.transport_endpoint);
    if (kept != standby_allocator_keepalive_.end()) {
        if (auto placeholder =
                std::dynamic_pointer_cast<DummyBufferAllocator>(kept->second)) {
            follower_orphaned_replicas_.fetch_add(placeholder->OrphanAll(),
                                                  std::memory_order_relaxed);
        }
        standby_allocator_keepalive_.erase(kept);
    }
    if (segment != follower_segments_.end()) {
        const std::string& name = segment->second.info.segment_name;
        if (!name.empty() && name != op.transport_endpoint) {
            standby_allocator_keepalive_.erase(name);
        }
        follower_segments_.erase(segment);
    }
}

std::vector<StandbySegmentInfo> MasterService::FollowerSegments() const {
    std::lock_guard<std::mutex> lock(follower_mutex_);
    std::vector<StandbySegmentInfo> segments;
    segments.reserve(follower_segments_.size());
    for (const auto& [endpoint, segment] : follower_segments_) {
        segments.push_back(segment.info);
    }
    return segments;
}

ReplicaID MasterService::FollowerMaxReplicaId() const {
    std::lock_guard<std::mutex> lock(follower_mutex_);
    return follower_max_replica_id_;
}

uint64_t MasterService::FollowerOrphanedReplicas() const {
    return follower_orphaned_replicas_.load(std::memory_order_relaxed);
}

uint64_t MasterService::FollowerRejectedEntries() const {
    return follower_rejected_entries_.load(std::memory_order_relaxed);
}

}  // namespace mooncake
