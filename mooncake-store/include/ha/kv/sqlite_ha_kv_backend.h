#pragma once

#include <memory>
#include <string_view>

#include <ylt/util/tl/expected.hpp>

#include "ha/common/sqlite/sqlite_ha_store.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

class SqliteHaKvBackend : public HaKvBackend {
   public:
    static tl::expected<std::shared_ptr<SqliteHaKvBackend>, ErrorCode> Open(
        std::string_view connstring);

    explicit SqliteHaKvBackend(std::unique_ptr<ha::sqlite::SqliteHaStore> store)
        : store_(std::move(store)) {}

    ErrorCode Get(std::string_view key, std::string& value) override;
    ErrorCode Put(std::string_view key, std::string_view value) override;
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override;
    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override;
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn& txn) override;

   private:
    std::unique_ptr<ha::sqlite::SqliteHaStore> store_;
};

}  // namespace mooncake
