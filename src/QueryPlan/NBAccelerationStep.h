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

#pragma once
#include <QueryPlan/TableScanStep.h>

namespace DB
{

/// NB Acceleration Step
class NBAccelerationStep : public ISourceStep
{
public:
    explicit NBAccelerationStep(std::shared_ptr<TableScanStep> step);

    String getName() const override { return "NBAcceleration"; }

    Type getType() const override { return Type::NBAcceleration; }

    void serialize(WriteBuffer & buffer) const override;

    static QueryPlanStepPtr deserialize(ReadBuffer & buffer, ContextPtr context);

    QueryPlanStepPtr copy(ContextPtr context) const override;

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;

    std::shared_ptr<TableScanStep> getStep() const { return step_; }
  
private:
    std::shared_ptr<TableScanStep> step_;
};

}


