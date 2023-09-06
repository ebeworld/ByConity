#pragma once

#include <Optimizer/Equivalences.h>
#include <QueryPlan/SimplePlanRewriter.h>
#include <Optimizer/Rewriter/Rewriter.h>
#include <Optimizer/SymbolTransformMap.h>
#include <Optimizer/Utils.h>
#include <Parsers/ASTTableColumnReference.h>
#include <QueryPlan/Assignment.h>

namespace DB
{
/**
  * MaterializedViewRewriter is based on "Optimizing Queries Using Materialized Views:
  * A Practical, Scalable Solution" by Goldstein and Larson.
  */
class NBAcceleration : public Rewriter
{
public:
    void rewrite(QueryPlan & plan, ContextMutablePtr context) const override;
    String name() const override { return "NBAccleration"; }

private:
};

class NBAccelerationVisitor : public SimplePlanRewriter<Void>
{
public:
    explicit NBAccelerationVisitor(ContextMutablePtr context_, CTEInfo & cte_info_)
        : SimplePlanRewriter(context_, cte_info_)
    {
    }

private:
    PlanNodePtr visitTableScanNode(TableScanNode & node, Void & require) override;
};

}
