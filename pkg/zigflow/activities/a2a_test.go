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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/google/uuid"
	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

const (
	a2aTestProtocolVersionKey    = "protocolVersion"
	a2aTestProtocolVersion       = "0.3.0"
	a2aTestNameKey               = "name"
	a2aTestDescriptionKey        = "description"
	a2aTestURLKey                = "url"
	a2aTestPreferredTransportKey = "preferredTransport"
	a2aTestVersionKey            = "version"
	a2aTestVersion               = "1.0.0"
	a2aTestCapabilitiesKey       = "capabilities"
	a2aTestDefaultInputModesKey  = "defaultInputModes"
	a2aTestDefaultOutputModesKey = "defaultOutputModes"
	a2aTestSkillsKey             = "skills"
	a2aTestIDKey                 = "id"
	a2aTestKindKey               = "kind"
	a2aTestMessageIDKey          = "messageId"
	a2aTestRoleKey               = "role"
	a2aTestAgentRole             = "agent"
	a2aTestPartsKey              = "parts"
	a2aTestTextKey               = "text"
	a2aTestTextKind              = "text"
	a2aTestMessageKind           = "message"
)

func a2aTestAgentCard(name, description, endpoint, transport string) map[string]any {
	return map[string]any{
		a2aTestProtocolVersionKey:    a2aTestProtocolVersion,
		a2aTestNameKey:               name,
		a2aTestDescriptionKey:        description,
		a2aTestURLKey:                endpoint,
		a2aTestPreferredTransportKey: transport,
		a2aTestVersionKey:            a2aTestVersion,
		a2aTestCapabilitiesKey:       map[string]any{},
		a2aTestDefaultInputModesKey:  []string{"text/plain"},
		a2aTestDefaultOutputModesKey: []string{"text/plain"},
		a2aTestSkillsKey:             []any{},
	}
}

func TestCallA2AActivityDiscoversCardAndDefaultsMessage(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cards/report-agent.json":
			assert.Equal(t, http.MethodGet, r.Method)
			_ = json.NewEncoder(w).Encode(a2aTestAgentCard("report-agent", "creates reports", server.URL+"/rpc", "JSONRPC"))
		case "/rpc":
			assert.Equal(t, http.MethodPost, r.Method)
			var req struct {
				ID     string `json:"id"`
				Method string `json:"method"`
				Params struct {
					Message struct {
						ID    string `json:"messageId"`
						Role  string `json:"role"`
						Parts []struct {
							Kind string `json:"kind"`
							Text string `json:"text"`
						} `json:"parts"`
					} `json:"message"`
				} `json:"params"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			assert.Equal(t, a2aMethodMessageSend, req.Method)
			assert.Equal(t, "user", req.Params.Message.Role)
			assert.NotEmpty(t, req.Params.Message.ID)
			assert.NoError(t, uuid.Validate(req.Params.Message.ID))
			require.Len(t, req.Params.Message.Parts, 1)
			assert.Equal(t, a2aTestTextKind, req.Params.Message.Parts[0].Kind)
			assert.Equal(t, "Generate the Q1 sales report.", req.Params.Message.Parts[0].Text)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc":    "2.0",
				a2aTestIDKey: req.ID,
				"result": map[string]any{
					a2aTestKindKey:      a2aTestMessageKind,
					a2aTestMessageIDKey: "agent-reply",
					a2aTestRoleKey:      a2aTestAgentRole,
					a2aTestPartsKey: []map[string]any{{
						a2aTestKindKey: a2aTestTextKind,
						a2aTestTextKey: "Report complete.",
					}},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	// A2A's Agent Card specifies the final RPC URL. The card is deliberately
	// served at a non-well-known URL to verify OWS externalResource handling.
	task := &model.CallFunction{
		Call: "a2a",
		With: map[string]any{
			"method": "message/send",
			"agentCard": map[string]any{
				"endpoint": server.URL + "/cards/report-agent.json",
			},
			"parameters": map[string]any{
				"message": map[string]any{
					a2aTestPartsKey: []map[string]any{{
						a2aTestKindKey: a2aTestTextKind,
						a2aTestTextKey: "Generate the Q1 sales report.",
					}},
				},
			},
		},
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	call := (&CallA2A{}).CallA2AActivity
	env.RegisterActivity(call)
	result, err := env.ExecuteActivity(call, task, nil, utils.NewState())
	require.NoError(t, err)
	var output map[string]any
	require.NoError(t, result.Get(&output))
	assert.Equal(t, a2aTestMessageKind, output[a2aTestKindKey])
	assert.Equal(t, a2aTestAgentRole, output[a2aTestRoleKey])
}

func TestCallA2AActivitySupportsHTTPJSON(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/card" {
			_ = json.NewEncoder(w).Encode(a2aTestAgentCard("rest-agent", "uses HTTP+JSON", server.URL+"/v1", "HTTP+JSON"))
			return
		}
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/message:send", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var params struct {
			Message struct {
				Role string `json:"role"`
			} `json:"message"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&params))
		assert.Equal(t, "user", params.Message.Role)
		_ = json.NewEncoder(w).Encode(map[string]any{
			a2aTestKindKey:      a2aTestMessageKind,
			a2aTestMessageIDKey: "rest-reply",
			a2aTestRoleKey:      a2aTestAgentRole,
			a2aTestPartsKey: []map[string]any{{
				a2aTestKindKey: a2aTestTextKind,
				a2aTestTextKey: "REST transport works.",
			}},
		})
	}))
	defer server.Close()

	task := &model.CallFunction{Call: "a2a", With: map[string]any{
		"method": "message/send",
		"agentCard": map[string]any{
			"endpoint": server.URL + "/card",
		},
		"parameters": map[string]any{"message": map[string]any{
			a2aTestPartsKey: []map[string]any{{a2aTestKindKey: a2aTestTextKind, a2aTestTextKey: "Hello REST agent."}},
		}},
	}}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	call := (&CallA2A{}).CallA2AActivity
	env.RegisterActivity(call)
	result, err := env.ExecuteActivity(call, task, nil, utils.NewState())
	require.NoError(t, err)
	var output map[string]any
	require.NoError(t, result.Get(&output))
	assert.Equal(t, a2aTestMessageKind, output[a2aTestKindKey])
	assert.Equal(t, a2aTestAgentRole, output[a2aTestRoleKey])
}

func TestA2AHTTPJSONTransportMethodMappings(t *testing.T) {
	const taskID = "task-1"
	const configID = "config-1"
	message := map[string]any{
		a2aTestKindKey:      a2aTestMessageKind,
		a2aTestMessageIDKey: "agent-message",
		a2aTestRoleKey:      a2aTestAgentRole,
		a2aTestPartsKey:     []map[string]any{{a2aTestKindKey: a2aTestTextKind, a2aTestTextKey: "Done."}},
	}
	task := map[string]any{
		a2aTestKindKey: "task",
		a2aTestIDKey:   taskID,
		"status": map[string]any{
			"state": "completed",
		},
	}
	config := map[string]any{
		"taskId": taskID,
		"pushNotificationConfig": map[string]any{
			"id":  configID,
			"url": "https://worker.example.com/callback",
		},
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/message:send":
			assert.Equal(t, http.MethodPost, r.Method)
			_ = json.NewEncoder(w).Encode(message)
		case "/v1/message:stream", "/v1/tasks/task-1:subscribe":
			assert.Equal(t, http.MethodPost, r.Method)
			w.Header().Set("Content-Type", "text/event-stream")
			payload := `data: {"message":{"kind":"message","messageId":"agent-message",` +
				`"role":"agent","parts":[{"kind":"text","text":"Done."}]}}` + "\n\n"
			_, _ = w.Write([]byte(payload))
		case "/v1/tasks/task-1":
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "4", r.URL.Query().Get("historyLength"))
			_ = json.NewEncoder(w).Encode(task)
		case "/v1/tasks":
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Equal(t, "context-1", r.URL.Query().Get("contextId"))
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{task}, "total_size": 1})
		case "/v1/tasks/task-1:cancel":
			assert.Equal(t, http.MethodPost, r.Method)
			var body map[string]string
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, "tasks/task-1", body["name"])
			_ = json.NewEncoder(w).Encode(task)
		case "/v1/tasks/task-1/pushNotificationConfigs":
			switch r.Method {
			case http.MethodPost:
				var body map[string]json.RawMessage
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.NotEmpty(t, body["config"])
				_ = json.NewEncoder(w).Encode(config)
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode([]any{config})
			default:
				http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			}
		case "/v1/tasks/task-1/pushNotificationConfigs/config-1":
			switch r.Method {
			case http.MethodGet:
				_ = json.NewEncoder(w).Encode(config)
			case http.MethodDelete:
				w.WriteHeader(http.StatusNoContent)
			default:
				http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			}
		case "/v1/card":
			assert.Equal(t, http.MethodGet, r.Method)
			_ = json.NewEncoder(w).Encode(a2aTestAgentCard("rest-agent", "uses HTTP+JSON", server.URL+"/v1", "HTTP+JSON"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	transport := newA2AHTTPJSONTransport(server.URL+"/v1", server.Client())
	params := &a2a.MessageSendParams{Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.TextPart{Text: "Hello."})}
	result, err := transport.SendMessage(t.Context(), params)
	require.NoError(t, err)
	assert.IsType(t, &a2a.Message{}, result)

	var streamEvents []a2a.Event
	for event, err := range transport.SendStreamingMessage(t.Context(), params) {
		require.NoError(t, err)
		streamEvents = append(streamEvents, event)
	}
	require.Len(t, streamEvents, 1)

	historyLength := 4
	gotTask, err := transport.GetTask(t.Context(), &a2a.TaskQueryParams{ID: taskID, HistoryLength: &historyLength})
	require.NoError(t, err)
	assert.Equal(t, taskID, string(gotTask.ID))
	list, err := transport.ListTasks(t.Context(), &a2a.ListTasksRequest{ContextID: "context-1"})
	require.NoError(t, err)
	require.Len(t, list.Tasks, 1)
	_, err = transport.CancelTask(t.Context(), &a2a.TaskIDParams{ID: taskID})
	require.NoError(t, err)

	var resubscribeEvents []a2a.Event
	for event, err := range transport.ResubscribeToTask(t.Context(), &a2a.TaskIDParams{ID: taskID}) {
		require.NoError(t, err)
		resubscribeEvents = append(resubscribeEvents, event)
	}
	require.Len(t, resubscribeEvents, 1)

	pushConfig := &a2a.TaskPushConfig{TaskID: taskID, Config: a2a.PushConfig{ID: configID, URL: "https://worker.example.com/callback"}}
	_, err = transport.SetTaskPushConfig(t.Context(), pushConfig)
	require.NoError(t, err)
	_, err = transport.GetTaskPushConfig(t.Context(), &a2a.GetTaskPushConfigParams{TaskID: taskID, ConfigID: configID})
	require.NoError(t, err)
	configs, err := transport.ListTaskPushConfig(t.Context(), &a2a.ListTaskPushConfigParams{TaskID: taskID})
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.NoError(t, transport.DeleteTaskPushConfig(t.Context(), &a2a.DeleteTaskPushConfigParams{TaskID: taskID, ConfigID: configID}))
	card, err := transport.GetAgentCard(t.Context())
	require.NoError(t, err)
	assert.Equal(t, a2a.TransportProtocolHTTPJSON, card.PreferredTransport)
}

func TestValidateA2AArguments(t *testing.T) {
	tests := []struct {
		name    string
		args    A2AArguments
		wantErr string
	}{
		{
			name: "server endpoint is supported",
			args: A2AArguments{
				Method: a2aMethodTaskGet,
				Server: json.RawMessage(`"https://agent.example.com/a2a"`),
			},
		},
		{
			name: "unknown method is rejected",
			args: A2AArguments{
				Method: "not/a2a-method",
				Server: json.RawMessage(`"https://agent.example.com/a2a"`),
			},
			wantErr: "unsupported A2A method",
		},
		{
			name:    "agent target is required",
			args:    A2AArguments{Method: a2aMethodTaskGet},
			wantErr: "requires agentCard or server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateA2AArguments(&tt.args)
			if tt.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestValidateA2AAgentCardRejectsAuthenticationRequirements(t *testing.T) {
	err := validateA2AAgentCard(&a2a.AgentCard{Security: []a2a.SecurityRequirements{{"apiKey": {}}}})
	assert.ErrorContains(t, err, "requires client authentication")
	assert.NoError(t, validateA2AAgentCard(&a2a.AgentCard{}))
}

func TestCallA2ARejectsMessageMethodWithoutMessage(t *testing.T) {
	call := &CallA2A{}
	info := activity.Info{}
	_, err := call.call(context.Background(), nil, &A2AArguments{Method: a2aMethodMessageSend}, &info)
	assert.ErrorContains(t, err, "requires parameters.message")
}
