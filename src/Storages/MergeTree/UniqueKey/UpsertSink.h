#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Columns/IColumn.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/Context_fwd.h>
#include <Common/RWLock.h>

namespace DB
{
class StorageMergeTree;
namespace Upsert
{
class Sink final : public SinkToStorage
{
public:
    Sink(StorageMergeTree & storage_, StorageMetadataPtr metadata_, ContextPtr context_);
    ~Sink() override;
    String getName() const override;
    void setHasDependentMaterializedViews(bool has_dependent_views) override;
    void onStart() override;
    void onFinish() override;
    void onException(std::exception_ptr exception) override;
    void consume(Chunk & chunk) override;
private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata;
    ContextPtr context;
    RWLockImpl::LockHolder schema_lock;
    UInt64 unit = 0;
    bool synchronously_commit_for_views = false;
    bool has_dependent_views = false;
    struct Impl;
    std::unique_ptr<Impl> impl;
    void finishFront();
    bool consumeBlock(Block block, size_t first_row, const String & client_token, IColumn::Filter * forwarded);
};
}
}
