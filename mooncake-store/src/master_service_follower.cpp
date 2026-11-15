// Follower mode: the standby applies the leader's OpLog straight into a
// MasterService index, so promotion activates that index instead of
// rebuilding one from a standby store.

#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <thread>

#include "allocator.h"
#include "ha/ha_types.h"
#include "ha/handoff_record.h"
#include "ha/kv/ha_kv_backend_factory.h"
#include "ha/oplog/oplog_batch_storage.h"
#include "master_metric_manager.h"
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
    MetadataAccessorRW accessor(this, {tenant, key},
                                MetadataAccessorRW::AsIs{});
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

void MasterService::FollowerClear() {
    std::vector<std::pair<TenantId, route::ObjectRef>> objects;
    VisitNamespaces(
        [&objects](const TenantId& tenant_id, metadata::Namespace& ns) {
            for (auto object : ns.objects.ReadCursor()) {
                objects.emplace_back(tenant_id, object.ref());
            }
        });
    std::unique_lock<std::shared_mutex> exclusive(master_state_mutex_);
    for (const auto& [tenant_id, ref] : objects) {
        MetadataAccessorRW accessor(this, tenant_id, ref);
        if (accessor.IsPublished()) {
            accessor.Erase();
        }
    }
    std::lock_guard<std::mutex> lock(follower_mutex_);
    follower_segments_.clear();
    standby_allocator_keepalive_.clear();
    follower_owner_records_.clear();
    follower_max_replica_id_ = 0;
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

namespace {

// A followed object as the standby store would hold it. Orphaned replicas
// are left out: they own nothing a reader could use.
StandbyObjectMetadata FollowedObjectMetadata(const ObjectMetadata& metadata) {
    StandbyObjectMetadata out;
    out.client_id = metadata.client_id;
    out.size = metadata.TotalLength();
    out.group_id = metadata.group_id;
    out.data_type = metadata.data_type;
    out.hard_pinned = metadata.IsHardPinned();
    out.session_id = metadata.session_id;
    out.object_checksum = metadata.GetPart(0).checksum();
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (replica.is_memory_replica() && replica.has_invalid_mem_handle()) {
            continue;
        }
        out.replicas.push_back(replica.get_descriptor());
    }
    return out;
}

}  // namespace

std::optional<StandbyObjectMetadata> MasterService::FollowerGet(
    const std::string& tenant_id, const std::string& key) {
    const TenantId tenant(enable_multi_tenants_
                              ? tenant_id
                              : std::string(TenantId::kDefaultValue));
    if (!tenant.IsValid()) {
        return std::nullopt;
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    MetadataAccessorRW accessor(this, {tenant, key},
                                MetadataAccessorRW::AsIs{});
    if (!accessor.IsPublished()) {
        return std::nullopt;
    }
    return FollowedObjectMetadata(accessor.Get());
}

size_t MasterService::FollowerKeyCount(const std::string& tenant_id) const {
    const metadata::Namespace* ns = namespaces_.Lookup(TenantId(tenant_id));
    return ns == nullptr ? 0 : ns->objects.ObjectCount();
}

FollowerTraversalCursor MasterService::FollowerBeginTraversal() const {
    FollowerTraversalCursor cursor;
    VisitNamespaces(
        [&cursor](const TenantId& tenant_id, metadata::Namespace& /*ns*/) {
            cursor.tenants.push_back(tenant_id.value());
        });
    return cursor;
}

bool MasterService::FollowerCopyNextChunk(
    size_t count, FollowerTraversalCursor& cursor,
    std::vector<StandbyObjectEntry>& out) const {
    out.clear();
    if (count == 0) {
        return false;
    }
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    while (cursor.tenant < cursor.tenants.size() && out.size() < count) {
        const std::string& tenant = cursor.tenants[cursor.tenant];
        const metadata::Namespace* ns = namespaces_.Lookup(TenantId(tenant));
        if (ns == nullptr ||
            cursor.stripe >= route::ObjectRoute::kStripeCount) {
            ++cursor.tenant;
            cursor.stripe = 0;
            cursor.skip = 0;
            continue;
        }
        size_t seen = 0;
        bool stripe_done = true;
        for (auto object :
             ns->objects.ReadCursor(cursor.stripe, cursor.stripe + 1)) {
            if (seen++ < cursor.skip) {
                continue;
            }
            if (out.size() == count) {
                stripe_done = false;
                break;
            }
            out.push_back(
                StandbyObjectEntry{tenant, object.key(),
                                   FollowedObjectMetadata(object.metadata())});
        }
        if (stripe_done) {
            ++cursor.stripe;
            cursor.skip = 0;
        } else {
            // The object at seen - 1 was not taken; resume there.
            cursor.skip = seen - 1;
        }
    }
    return true;
}

namespace {
int64_t SystemNowMs(std::chrono::system_clock::time_point now) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               now.time_since_epoch())
        .count();
}
}  // namespace

bool MasterService::InheritedLeaseHeld(
    std::chrono::system_clock::time_point now) const {
    return SystemNowMs(now) <
           inherited_lease_floor_ms_.load(std::memory_order_relaxed);
}

bool MasterService::LeaseExpired(
    const ObjectMetadata& metadata,
    std::chrono::system_clock::time_point now) const {
    return metadata.IsLeaseExpired(now) && !InheritedLeaseHeld(now);
}

bool MasterService::LeaseExpired(const ObjectMetadata& metadata) const {
    return LeaseExpired(metadata, std::chrono::system_clock::now());
}

bool MasterService::MemoryEvictionPaused() const {
    return InheritedLeaseHeld(std::chrono::system_clock::now());
}

tl::expected<void, ErrorCode> MasterService::ActivateFollower(
    ViewVersionId view_version, const DurablePrefix& applied,
    const std::vector<StandbySegmentInfo>& segments,
    const WeightMetadataSnapshot& weights) {
    RestoreLifecycleState expected = RestoreLifecycleState::kFollowing;
    if (!lifecycle_state_.compare_exchange_strong(
            expected, RestoreLifecycleState::kRecovering,
            std::memory_order_acq_rel)) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    const auto fail = [this](ErrorCode error) {
        lifecycle_state_.store(RestoreLifecycleState::kFailed,
                               std::memory_order_release);
        LOG(ERROR) << "action=activate_follower, error=" << toString(error);
        return tl::make_unexpected(error);
    };
    view_version_ = view_version;
    if (enable_oplog_) {
        const auto backend_type =
            ha::ParseHABackendType(follower_ha_backend_type_);
        if (!backend_type || follower_ha_backend_connstring_.empty()) {
            return fail(ErrorCode::INVALID_PARAMS);
        }
        auto backend = CreateHaKvBackend(ha::HABackendSpec{
            .type = *backend_type,
            .connstring = follower_ha_backend_connstring_,
            .cluster_namespace = cluster_id_,
        });
        if (!backend) {
            return fail(backend.error());
        }
        // The writer goes on from the durable prefix, so apply must have
        // reached it: nothing the old leader logged may be missing here.
        DurablePrefix durable;
        OpLogBatchStorage storage(cluster_id_, **backend);
        if (const ErrorCode err = storage.ReadDurablePrefix(durable);
            err != ErrorCode::OK && err != ErrorCode::ETCD_KEY_NOT_EXIST) {
            return fail(err);
        }
        if (durable.last_seq != applied.last_seq ||
            durable.batch_id != applied.batch_id) {
            LOG(ERROR) << "action=activate_follower, durable_seq="
                       << durable.last_seq
                       << ", applied_seq=" << applied.last_seq;
            return fail(ErrorCode::INCOMPLETE_OPLOG_CATCH_UP);
        }
        if (const ErrorCode err = InitializeBatchOpLogWriter(
                std::move(*backend),
                /*require_fenced_writer=*/view_version_ > 0);
            err != ErrorCode::OK) {
            return fail(err);
        }
    }

    ArmPromotionQuarantine(applied.last_seq);
    {
        std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
        std::unique_lock<std::shared_mutex> exclusive(master_state_mutex_);
        std::lock_guard<std::mutex> lock(follower_mutex_);
        std::unordered_map<std::string, StandbySegmentInfo> memory_segments;
        for (const auto& [endpoint, segment] : follower_segments_) {
            memory_segments.emplace(endpoint, segment.info);
        }
        for (const auto& segment : segments) {
            if (segment.is_memory_segment) {
                memory_segments.try_emplace(segment.transport_endpoint,
                                            segment);
            }
        }
        standby_memory_segments_.clear();
        standby_accounted_memory_bytes_.clear();
        for (const auto& [endpoint, info] : memory_segments) {
            standby_memory_segments_.push_back(info);
            auto kept = standby_allocator_keepalive_.find(endpoint);
            if (kept == standby_allocator_keepalive_.end()) {
                continue;
            }
            auto placeholder =
                std::dynamic_pointer_cast<DummyBufferAllocator>(kept->second);
            if (placeholder && placeholder->TrackedBytes() > 0) {
                standby_accounted_memory_bytes_[info.segment_name] +=
                    placeholder->TrackedBytes();
            }
        }
        invalid_replica_endpoints_.clear();
        for (const auto& [alias, allocator] : standby_allocator_keepalive_) {
            invalid_replica_endpoints_.insert(alias);
        }
        standby_restored_at_ = std::chrono::steady_clock::now();
        for (auto& [client_id, record] : follower_owner_records_) {
            if (client_liveness_records_.emplace(client_id, record).second) {
                MasterMetricManager::instance()
                    .client_liveness_record_created();
            }
        }
        const ReplicaID desired_next_id = follower_max_replica_id_ + 1;
        ReplicaID current_next_id = Replica::next_id_.load();
        while (current_next_id < desired_next_id &&
               !Replica::next_id_.compare_exchange_weak(current_next_id,
                                                        desired_next_id)) {
        }
    }
    NotifyPolicyRestored();
    if (!weight_manager_.RestoreSnapshot(weights)) {
        return fail(ErrorCode::INVALID_PARAMS);
    }
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().inc_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }
    if (follower_kv_event_config_) {
        kv_event_publisher_ =
            std::make_unique<KvEventPublisher>(*follower_kv_event_config_);
        kv_track_tenant_epochs_ = KvEventsEnabled();
    }
    inherited_lease_floor_ms_.store(
        SystemNowMs(std::chrono::system_clock::now()) +
            static_cast<int64_t>(default_kv_lease_ttl_),
        std::memory_order_relaxed);
    lifecycle_state_.store(RestoreLifecycleState::kReady,
                           std::memory_order_release);
    LOG(INFO) << "action=activate_follower, objects=" << GetKeyCount()
              << ", segments=" << standby_memory_segments_.size()
              << ", applied_seq=" << applied.last_seq
              << ", orphaned_replicas=" << FollowerOrphanedReplicas()
              << ", rejected_entries=" << FollowerRejectedEntries();
    return {};
}

std::vector<AllocatedBuffer::Descriptor> MasterService::BeginHandoffDrain(
    std::chrono::milliseconds timeout) {
    handoff_draining_.store(true, std::memory_order_release);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const auto in_flight = [this] {
        std::vector<std::pair<TenantId, std::string>> keys;
        VisitNamespaces(
            [&keys](const TenantId& tenant_id, metadata::Namespace& ns) {
                for (auto& key : ns.InFlightKeys()) {
                    keys.emplace_back(tenant_id, std::move(key));
                }
            });
        return keys;
    };
    auto keys = in_flight();
    while (!keys.empty() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        keys = in_flight();
    }
    std::vector<AllocatedBuffer::Descriptor> ranges;
    std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
    for (const auto& [tenant_id, key] : keys) {
        MetadataAccessorRW accessor(this, {tenant_id, key},
                                    MetadataAccessorRW::AsIs{});
        if (!accessor.IsPublished()) {
            continue;
        }
        accessor.Get().VisitReplicas(
            [](const Replica& replica) {
                return replica.is_memory_replica() && replica.is_processing();
            },
            [&ranges](const Replica& replica) {
                ranges.push_back(replica.get_descriptor()
                                     .get_memory_descriptor()
                                     .buffer_descriptor);
            });
    }
    LOG(INFO) << "action=handoff_drain, writes_in_flight=" << keys.size()
              << ", ranges_in_flight=" << ranges.size();
    return ranges;
}

ErrorCode MasterService::WriteHandoffRecord(
    const std::vector<AllocatedBuffer::Descriptor>& in_flight) {
    if (!batch_oplog_kv_backend_) {
        return ErrorCode::OK;
    }
    DurablePrefix durable;
    OpLogBatchStorage storage(cluster_id_, *batch_oplog_kv_backend_);
    if (const ErrorCode err = storage.ReadDurablePrefix(durable);
        err != ErrorCode::OK && err != ErrorCode::ETCD_KEY_NOT_EXIST) {
        return err;
    }
    const ha::HandoffRecord record{.view_version = view_version_,
                                   .durable_last_seq = durable.last_seq,
                                   .in_flight = in_flight};
    const auto key = ha::HandoffKey(cluster_id_);
    if (key.empty()) {
        return ErrorCode::INVALID_PARAMS;
    }
    const ErrorCode err = batch_oplog_kv_backend_->Put(
        key, struct_pack::serialize<std::string>(record));
    LOG(INFO) << "action=write_handoff_record, view=" << view_version_
              << ", durable_seq=" << durable.last_seq
              << ", ranges_in_flight=" << in_flight.size()
              << ", error=" << toString(err);
    return err;
}

void MasterService::ArmPromotionQuarantine(uint64_t applied_last_seq) {
    PromotionQuarantine quarantine;
    quarantine.until =
        std::chrono::steady_clock::now() + put_start_release_timeout_sec_;
    const auto record =
        batch_oplog_kv_backend_
            ? ha::ReadCleanHandoff(*batch_oplog_kv_backend_, cluster_id_,
                                   view_version_, applied_last_seq)
            : std::nullopt;
    const bool clean = record.has_value();
    if (record) {
        for (const auto& range : record->in_flight) {
            quarantine.ranges[range.transport_endpoint_].emplace_back(
                range.buffer_address_, range.size_);
        }
    }
    quarantine.all_free = !clean;
    LOG(INFO) << "action=arm_promotion_quarantine, clean_handoff=" << clean
              << ", ranges=" << quarantine.ranges.size()
              << ", for_s=" << put_start_release_timeout_sec_.count();
    std::unique_lock<std::shared_mutex> exclusive(master_state_mutex_);
    promotion_quarantine_ = std::move(quarantine);
}

void MasterService::HoldQuarantined(
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers,
    std::chrono::steady_clock::time_point until) {
    std::lock_guard<std::mutex> lock(quarantine_mutex_);
    for (auto& buffer : buffers) {
        quarantine_holds_.push_back({until, std::move(buffer)});
    }
}

void MasterService::ReleaseExpiredQuarantine() {
    std::vector<std::unique_ptr<AllocatedBuffer>> expired;
    {
        std::lock_guard<std::mutex> lock(quarantine_mutex_);
        if (quarantine_holds_.empty()) {
            return;
        }
        const auto now = std::chrono::steady_clock::now();
        auto keep = std::partition(
            quarantine_holds_.begin(), quarantine_holds_.end(),
            [now](const QuarantineHold& hold) { return hold.until > now; });
        for (auto it = keep; it != quarantine_holds_.end(); ++it) {
            expired.push_back(std::move(it->buffer));
        }
        quarantine_holds_.erase(keep, quarantine_holds_.end());
    }
    if (!expired.empty()) {
        std::shared_lock<std::shared_mutex> shared_lock(master_state_mutex_);
        LOG(INFO) << "action=release_promotion_quarantine, holds="
                  << expired.size();
        expired.clear();
    }
}

uint64_t MasterService::PromotionQuarantineBytes() const {
    std::lock_guard<std::mutex> lock(quarantine_mutex_);
    uint64_t bytes = 0;
    for (const auto& hold : quarantine_holds_) {
        bytes += hold.buffer ? hold.buffer->size() : 0;
    }
    return bytes;
}

uint64_t MasterService::FollowerOrphanedReplicas() const {
    return follower_orphaned_replicas_.load(std::memory_order_relaxed);
}

uint64_t MasterService::FollowerRejectedEntries() const {
    return follower_rejected_entries_.load(std::memory_order_relaxed);
}

}  // namespace mooncake
