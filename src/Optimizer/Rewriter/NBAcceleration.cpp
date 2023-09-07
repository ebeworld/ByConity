#include <Optimizer/Rewriter/NBAcceleration.h>

#include <QueryPlan/IQueryPlanStep.h>
#include <QueryPlan/NBAccelerationStep.h>
#include <QueryPlan/PlanNode.h>
#include <QueryPlan/ProjectionStep.h>
#include <QueryPlan/Void.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Interpreters/RequiredSourceColumnsVisitor.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Optimizer/PredicateUtils.h>
#include <Optimizer/SymbolsExtractor.h>
#include <QueryPlan/SymbolMapper.h>


namespace DB
{
    void NBAcceleration::rewrite(QueryPlan & plan, ContextMutablePtr context) const
    {
        if (!context->getSettingsRef().enable_nb_acceleration )
            return;
        NBAccelerationVisitor visitor{context, plan.getCTEInfo()};
        Void v;
        auto result = VisitorUtil::accept(plan.getPlanNode(), visitor, v);
        plan.update(result);
    }

    PlanNodePtr NBAccelerationVisitor::visitTableScanNode(TableScanNode & node, Void & require)
    {
        return std::make_shared<NBAccelerationNode>(context->nextNodeId(), std::make_shared<NBAccelerationStep>(node.getStep()));
    }
}
