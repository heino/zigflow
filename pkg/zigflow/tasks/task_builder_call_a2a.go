/*
 * Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
 *
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

package tasks

import (
	"encoding/json"
	"fmt"

	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/zigflow/activities"
	"go.temporal.io/sdk/worker"
)

func NewCallA2ATaskBuilder(
	temporalWorker worker.Worker,
	task *model.CallFunction,
	taskName string,
	doc *model.Workflow,
	emitter *cloudevents.Events,
	taskOpts *TaskOpts,
) (*CallA2ATaskBuilder, error) {
	if task.Call != "a2a" {
		return nil, fmt.Errorf("unsupported call task %q for A2A builder", task.Call)
	}
	return &CallA2ATaskBuilder{
		doc:            doc,
		eventEmitter:   emitter,
		name:           taskName,
		task:           task,
		taskOpts:       taskOpts,
		temporalWorker: temporalWorker,
	}, nil
}

type CallA2ATaskBuilder struct {
	builder[*model.CallFunction]
}

var callA2AActivity = &activities.CallA2A{}

func (t *CallA2ATaskBuilder) Validate() error {
	data, err := json.Marshal(t.task.With)
	if err != nil {
		return fmt.Errorf("marshal A2A arguments: %w", err)
	}
	args, err := activities.DecodeA2AArguments(data)
	if err != nil {
		return err
	}
	return activities.ValidateA2AArguments(&args)
}

func (t *CallA2ATaskBuilder) Build() (TemporalWorkflowFunc, error) {
	return t.buildActivityFunc(callA2AActivity.CallA2AActivity, legacyCallA2AActivityName), nil
}
