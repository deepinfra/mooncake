#include "ha/snapshot/object/snapshot_object_store.h"

#include "ha/replication/replicated_kv_backend.h"
#include "ha/snapshot/object/backends/ha_kv/ha_kv_snapshot_object_store.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#ifdef HAVE_AWS_SDK
#include "ha/snapshot/object/backends/s3/s3_snapshot_object_store.h"
#endif

namespace mooncake {

std::unique_ptr<SnapshotObjectStore> SnapshotObjectStore::Create(
    SnapshotObjectStoreType type) {
    switch (type) {
#ifdef HAVE_AWS_SDK
        case SnapshotObjectStoreType::S3:
            return std::make_unique<S3SnapshotObjectStore>();
#else
        case SnapshotObjectStoreType::S3:
            throw std::runtime_error(
                "S3 snapshot object store requested but AWS SDK is not "
                "available. Please rebuild with HAVE_AWS_SDK or use the "
                "'local' object store.");
#endif
        case SnapshotObjectStoreType::LOCAL_FILE:
            return std::make_unique<LocalFileSnapshotObjectStore>();
        case SnapshotObjectStoreType::HA_KV: {
            auto backend = ha::replication::RegisteredReplicatedBackend();
            if (!backend) {
                throw std::invalid_argument(
                    "the ha_kv snapshot object store needs HA replication");
            }
            return std::make_unique<HaKvSnapshotObjectStore>(backend);
        }
        default:
            throw std::invalid_argument("Unknown snapshot object store type");
    }
}

}  // namespace mooncake
