#include "ha/kv/sqlite_ha_kv_backend.h"

namespace mooncake {

tl::expected<std::shared_ptr<SqliteHaKvBackend>, ErrorCode>
SqliteHaKvBackend::Open(std::string_view connstring) {
    auto store = ha::sqlite::SqliteHaStore::Open(connstring);
    if (!store) {
        return tl::make_unexpected(store.error());
    }
    return std::make_shared<SqliteHaKvBackend>(std::move(store.value()));
}

ErrorCode SqliteHaKvBackend::Get(std::string_view key, std::string& value) {
    return store_->Get(key, value);
}

ErrorCode SqliteHaKvBackend::Put(std::string_view key, std::string_view value) {
    return store_->Put(key, value);
}

ErrorCode SqliteHaKvBackend::Range(std::string_view begin_key,
                                   std::string_view end_key, size_t limit,
                                   std::vector<KvPair>& kvs) {
    return store_->Range(begin_key, end_key, limit, kvs);
}

ErrorCode SqliteHaKvBackend::DeleteRange(std::string_view begin_key,
                                         std::string_view end_key) {
    return store_->DeleteRange(begin_key, end_key);
}

ErrorCode SqliteHaKvBackend::Txn(const KvTxn& txn) { return store_->Txn(txn); }

}  // namespace mooncake
