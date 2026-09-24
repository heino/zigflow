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

package activities

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
	"github.com/a2aproject/a2a-go/a2aclient/agentcard"
	"github.com/google/uuid"
	swUtil "github.com/open-workflow-specification/sdk-go/v4/impl/utils"
	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/zigflow/zigflow/pkg/utils"
	"github.com/zigflow/zigflow/pkg/zigflow/metadata"
	"go.temporal.io/sdk/activity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func init() {
	Registry = append(Registry, &CallA2A{})
}

// See: https://a2a-protocol.org/v0.3.0/specification/#356-method-mapping-reference-table
const (
	a2aJSONNull                             = "null"
	a2aMethodMessageSend                    = "message/send"
	a2aMethodMessageStream                  = "message/stream"
	a2aMethodTaskGet                        = "tasks/get"
	a2aMethodTaskList                       = "tasks/list" // gRPC/REST only"
	a2aMethodTaskCancel                     = "tasks/cancel"
	a2aMethodTaskResubscribe                = "tasks/resubscribe"
	a2aMethodPushConfigSet                  = "tasks/pushNotificationConfig/set"
	a2aMethodPushConfigGet                  = "tasks/pushNotificationConfig/get"
	a2aMethodPushConfigList                 = "tasks/pushNotificationConfig/list"
	a2aMethodPushConfigDelete               = "tasks/pushNotificationConfig/delete"
	a2aMethodAuthenticatedExtendedAgentCard = "agent/getAuthenticatedExtendedCard"
)

var supportedA2AMethods = map[string]struct{}{
	a2aMethodMessageSend:                    {},
	a2aMethodMessageStream:                  {},
	a2aMethodTaskGet:                        {},
	a2aMethodTaskList:                       {},
	a2aMethodTaskCancel:                     {},
	a2aMethodTaskResubscribe:                {},
	a2aMethodPushConfigSet:                  {},
	a2aMethodPushConfigGet:                  {},
	a2aMethodPushConfigList:                 {},
	a2aMethodPushConfigDelete:               {},
	a2aMethodAuthenticatedExtendedAgentCard: {},
}

// A2AArguments is the OWS A2A call's with object. AgentCard and Server are
// kept as raw values because OWS permits external-resource endpoint forms.
type A2AArguments struct {
	Method     string          `json:"method"`
	AgentCard  json.RawMessage `json:"agentCard,omitempty"`
	Server     json.RawMessage `json:"server,omitempty"`
	Parameters json.RawMessage `json:"parameters,omitempty"`
}

type a2aResource struct {
	Endpoint string `json:"endpoint"`
}

var a2aArgumentFields = map[string]struct{}{
	"method": {}, "agentCard": {}, "server": {}, "parameters": {},
}

// CallA2A executes OWS A2A calls in a Temporal activity. Network access and
// Agent Card discovery are deliberately outside workflow code, preserving
// replay determinism.
type CallA2A struct{}

func (c *CallA2A) CallA2AActivity(ctx context.Context, task *model.CallFunction, input any, state *utils.State) (any, error) {
	logger := activity.GetLogger(ctx)
	stopHeartbeat := metadata.StartActivityHeartbeat(ctx, task.GetBase())
	defer stopHeartbeat()

	state = state.Clone().AddActivityInfo(ctx)
	args, err := ParseA2AArguments(task, state)
	if err != nil {
		return nil, err
	}

	info := activity.GetInfo(ctx)
	client, err := newA2AClient(ctx, args, info.StartToCloseTimeout)
	if err != nil {
		return nil, fmt.Errorf("create A2A client: %w", err)
	}
	defer func() {
		if closeErr := client.Destroy(); closeErr != nil {
			logger.Debug("Error closing A2A client", "error", closeErr)
		}
	}()

	result, err := c.call(ctx, client, args, &info)
	if err != nil {
		return nil, fmt.Errorf("A2A %s: %w", args.Method, err)
	}

	return a2aResultJSON(result)
}

func (c *CallA2A) call(ctx context.Context, client *a2aclient.Client, args *A2AArguments, info *activity.Info) (any, error) {
	switch args.Method {
	case a2aMethodMessageSend:
		return callA2AMessage(ctx, client, args, info, false)
	case a2aMethodMessageStream:
		return callA2AMessage(ctx, client, args, info, true)
	case a2aMethodTaskGet:
		return callA2AParameters(args.Parameters, func(params *a2a.TaskQueryParams) (any, error) { return client.GetTask(ctx, params) })
	case a2aMethodTaskList:
		return callA2AParameters(args.Parameters, func(params *a2a.ListTasksRequest) (any, error) { return client.ListTasks(ctx, params) })
	case a2aMethodTaskCancel:
		return callA2AParameters(args.Parameters, func(params *a2a.TaskIDParams) (any, error) { return client.CancelTask(ctx, params) })
	case a2aMethodTaskResubscribe:
		return callA2AParameters(args.Parameters, func(params *a2a.TaskIDParams) (any, error) {
			return collectA2AEvents(client.ResubscribeToTask(ctx, params))
		})
	case a2aMethodPushConfigSet:
		return callA2AParameters(args.Parameters, func(params *a2a.TaskPushConfig) (any, error) { return client.SetTaskPushConfig(ctx, params) })
	case a2aMethodPushConfigGet:
		return callA2AParameters(args.Parameters, func(params *a2a.GetTaskPushConfigParams) (any, error) {
			return client.GetTaskPushConfig(ctx, params)
		})
	case a2aMethodPushConfigList:
		return callA2AParameters(args.Parameters, func(params *a2a.ListTaskPushConfigParams) (any, error) {
			return client.ListTaskPushConfig(ctx, params)
		})
	case a2aMethodPushConfigDelete:
		return callA2AParameters(args.Parameters, func(params *a2a.DeleteTaskPushConfigParams) (any, error) {
			return nil, client.DeleteTaskPushConfig(ctx, params)
		})
	case a2aMethodAuthenticatedExtendedAgentCard:
		return client.GetAgentCard(ctx)
	default:
		return nil, fmt.Errorf("unsupported A2A method %q", args.Method)
	}
}

func callA2AMessage(ctx context.Context, client *a2aclient.Client, args *A2AArguments, info *activity.Info, streaming bool) (any, error) {
	var params a2a.MessageSendParams
	if err := decodeA2AParameters(args.Parameters, &params); err != nil {
		return nil, err
	}
	if params.Message == nil {
		return nil, fmt.Errorf("A2A %q requires parameters.message", args.Method)
	}
	defaultA2AMessage(&params, info)
	if streaming {
		return collectA2AEvents(client.SendStreamingMessage(ctx, &params))
	}
	return client.SendMessage(ctx, &params)
}

func callA2AParameters[T any](raw json.RawMessage, call func(*T) (any, error)) (any, error) {
	var params T
	if err := decodeA2AParameters(raw, &params); err != nil {
		return nil, err
	}
	return call(&params)
}

func newA2AClient(ctx context.Context, args *A2AArguments, timeout time.Duration) (*a2aclient.Client, error) {
	httpClient := &http.Client{Timeout: timeout}
	opts := []a2aclient.FactoryOption{
		a2aclient.WithDefaultsDisabled(),
		a2aclient.WithJSONRPCTransport(httpClient),
		a2aclient.WithGRPCTransport(grpcTLSOption()),
		a2aclient.WithTransport(a2a.TransportProtocolHTTPJSON, a2aclient.TransportFactoryFn(func(
			_ context.Context, endpoint string, _ *a2a.AgentCard,
		) (a2aclient.Transport, error) {
			return newA2AHTTPJSONTransport(endpoint, httpClient), nil
		})),
	}
	if len(args.AgentCard) > 0 && string(args.AgentCard) != a2aJSONNull {
		endpoint, err := a2aEndpoint(args.AgentCard, "agentCard")
		if err != nil {
			return nil, err
		}
		// The OWS endpoint names the Agent Card itself. An empty resolver path
		// preserves that exact URL instead of assuming the well-known location.
		card, err := agentcard.NewResolver(httpClient).Resolve(ctx, endpoint, agentcard.WithPath(""))
		if err != nil {
			return nil, fmt.Errorf("resolve Agent Card: %w", err)
		}
		if err := validateA2AAgentCard(card); err != nil {
			return nil, err
		}
		return a2aclient.NewFromCard(ctx, card, opts...)
	}

	endpoint, err := a2aEndpoint(args.Server, "server")
	if err != nil {
		return nil, err
	}
	return a2aclient.NewFromEndpoints(ctx, []a2a.AgentInterface{{
		Transport: a2a.TransportProtocolJSONRPC,
		URL:       endpoint,
	}}, opts...)
}

func validateA2AAgentCard(card *a2a.AgentCard) error {
	if len(card.Security) > 0 {
		return fmt.Errorf("A2A Agent Card requires client authentication, which Zigflow does not yet support")
	}
	return nil
}

func grpcTLSOption() grpc.DialOption {
	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}))
}

func a2aEndpoint(raw json.RawMessage, field string) (string, error) {
	if len(raw) == 0 || string(raw) == a2aJSONNull {
		return "", fmt.Errorf("A2A %s is required", field)
	}
	var endpoint string
	if err := json.Unmarshal(raw, &endpoint); err == nil && endpoint != "" {
		return endpoint, nil
	}
	var resource a2aResource
	if err := json.Unmarshal(raw, &resource); err != nil || resource.Endpoint == "" {
		return "", fmt.Errorf("A2A %s must be an endpoint string or an object with endpoint", field)
	}
	return resource.Endpoint, nil
}

// ParseA2AArguments resolves runtime expressions in the OWS parameters before
// decoding them into the A2A SDK request types.
func ParseA2AArguments(task *model.CallFunction, state *utils.State) (*A2AArguments, error) {
	data, err := json.Marshal(task.With)
	if err != nil {
		return nil, fmt.Errorf("marshal A2A arguments: %w", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal A2A arguments: %w", err)
	}
	parsed, err := utils.TraverseAndEvaluateObj(model.NewObjectOrRuntimeExpr(swUtil.DeepClone(raw)), nil, state)
	if err != nil {
		return nil, fmt.Errorf("interpolate A2A arguments: %w", err)
	}
	data, err = json.Marshal(parsed)
	if err != nil {
		return nil, fmt.Errorf("marshal interpolated A2A arguments: %w", err)
	}
	args, err := DecodeA2AArguments(data)
	if err != nil {
		return nil, err
	}
	if err := ValidateA2AArguments(&args); err != nil {
		return nil, err
	}
	return &args, nil
}

// DecodeA2AArguments rejects fields that are not part of the OWS 1.0.3 A2A
// call contract. This keeps Zigflow's temporary schema bridge from accepting
// runtime-specific DSL extensions.
func DecodeA2AArguments(data []byte) (A2AArguments, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return A2AArguments{}, fmt.Errorf("decode A2A arguments: %w", err)
	}
	for field := range raw {
		if _, ok := a2aArgumentFields[field]; !ok {
			return A2AArguments{}, fmt.Errorf("unsupported A2A argument %q", field)
		}
	}
	var args A2AArguments
	if err := json.Unmarshal(data, &args); err != nil {
		return A2AArguments{}, fmt.Errorf("decode A2A arguments: %w", err)
	}
	return args, nil
}

// ValidateA2AArguments checks the OWS A2A call shape without requiring a
// network request. It is shared by load-time and activity-time validation.
func ValidateA2AArguments(args *A2AArguments) error {
	if args == nil {
		return fmt.Errorf("A2A arguments are required")
	}
	if _, ok := supportedA2AMethods[args.Method]; !ok {
		return fmt.Errorf("unsupported A2A method %q", args.Method)
	}
	if len(args.AgentCard) == 0 || string(args.AgentCard) == a2aJSONNull {
		if _, err := a2aEndpoint(args.Server, "server"); err != nil {
			return fmt.Errorf("A2A requires agentCard or server: %w", err)
		}
	} else if _, err := a2aEndpoint(args.AgentCard, "agentCard"); err != nil {
		return err
	}
	return nil
}

func decodeA2AParameters(raw json.RawMessage, target any) error {
	if len(raw) == 0 || string(raw) == a2aJSONNull {
		raw = []byte("{}")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode A2A parameters: %w", err)
	}
	return nil
}

// defaultA2AMessage applies the defaults mandated by OWS. The generated UUID
// is derived from stable Temporal activity identity, so an activity retry
// retains the A2A message identity instead of accidentally starting a second
// agent task.
func defaultA2AMessage(params *a2a.MessageSendParams, info *activity.Info) {
	if params.Message.ID == "" {
		params.Message.ID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(info.WorkflowExecution.ID+":"+info.ActivityID)).String()
	}
	if params.Message.Role == "" {
		params.Message.Role = a2a.MessageRoleUser
	}
}

func collectA2AEvents(events func(func(a2a.Event, error) bool)) ([]a2a.Event, error) {
	result := make([]a2a.Event, 0)
	for event, err := range events {
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, nil
}

func a2aResultJSON(result any) (any, error) {
	data, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("marshal A2A result: %w", err)
	}
	var output any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&output); err != nil {
		return nil, fmt.Errorf("decode A2A result: %w", err)
	}
	return output, nil
}
