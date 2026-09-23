#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Interpreters/Context_fwd.h>

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
    void onStart() override;
    void onFinish() override;
    void onException(std::exception_ptr exception) override;
    void consume(Chunk & chunk) override;
    void setHasDependentMaterializedViews(bool has_views) override;
private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata;
    ContextPtr context;
    UInt64 unit = 0;
    struct Impl;
    std::unique_ptr<Impl> impl;
    void finishFront();
};
}
}
