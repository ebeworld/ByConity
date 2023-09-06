/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <QueryPlan/NBAccelerationStep.h>


namespace DB
{
    NBAccelerationStep::NBAccelerationStep(std::shared_ptr<TableScanStep> step): 
            ISourceStep(step->getTableOutputStream(), {}), 
            step_(step) 
    {
        
    }

    void NBAccelerationStep::serialize(WriteBuffer & buffer) const
    {
        step_->serialize(buffer);
    }

    QueryPlanStepPtr NBAccelerationStep::deserialize(ReadBuffer & buffer, ContextPtr context)
    {
        auto step = std::dynamic_pointer_cast<TableScanStep>(TableScanStep::deserialize(buffer, context));    
        return std::make_shared<NBAccelerationStep>(step);
    }

    QueryPlanStepPtr NBAccelerationStep::copy(ContextPtr context) const
    {
        auto step = std::dynamic_pointer_cast<TableScanStep>(step_->copy(context));
        return std::make_shared<NBAccelerationStep>(step);
    }

    void NBAccelerationStep::initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings & build_context)
    {
        step_->initializePipeline(pipeline, build_context);
    }

}
