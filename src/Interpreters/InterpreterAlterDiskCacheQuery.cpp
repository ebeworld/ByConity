#include <Interpreters/InterpreterAlterDiskCacheQuery.h>

#include <Catalog/DataModelPartWrapper_fwd.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <CloudServices/CnchPartsHelper.h>
#include <MergeTreeCommon/MergeTreeMetaBase.h>
#include <Parsers/ASTAlterDiskCacheQuery.h>
#include <Storages/StorageCnchMergeTree.h>
#include "Common/tests/gtest_global_context.h"

namespace DB
{
InterpreterAlterDiskCacheQuery::InterpreterAlterDiskCacheQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

BlockIO InterpreterAlterDiskCacheQuery::execute()
{
    const auto & query = query_ptr->as<ASTAlterDiskCacheQuery &>();
    if (query.type == ASTAlterDiskCacheQuery::Type::DROP)
    {
        throw Exception("Drop disk cache is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// apply settings
    if (query.settings_ast)
    {
        InterpreterSetQuery(query.settings_ast, getContext()).executeForCurrentContext();
    }

    StoragePtr table = DatabaseCatalog::instance().getTable({query.database, query.table}, getContext());
    auto * storage = dynamic_cast<StorageCnchMergeTree *>(table.get());
    if (!storage)
        throw Exception("Preload only support CnchMergeTree engine", ErrorCodes::LOGICAL_ERROR);

    ServerDataPartsVector parts;
    if (query.partition)
    {
        String partition_id = storage->getPartitionIDFromQuery(query.partition, getContext());
        parts = getContext()->getCnchCatalog()->getServerDataPartsInPartitions(table, {partition_id}, getContext()->getTimestamp(), nullptr);
    }
    else
    {
        parts = storage->getAllParts(getContext());
    }
    parts = CnchPartsHelper::calcVisibleParts(parts, false);

    storage->sendPreloadTasks(getContext(), std::move(parts), query.sync, getContext()->getSettings().parts_preload_level);

    return {};
}
}
