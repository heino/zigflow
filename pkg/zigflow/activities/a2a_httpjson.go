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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/a2aproject/a2a-go/a2a"
	"github.com/a2aproject/a2a-go/a2aclient"
)

// a2aHTTPJSONTransport implements the HTTP+JSON mappings in A2A 0.3. The
// upstream Go SDK declares HTTP+JSON but currently ships only JSON-RPC and
// gRPC transports, so Zigflow supplies this adapter around its public types.
type a2aHTTPJSONTransport struct {
	baseURL    string
	httpClient *http.Client
}

var _ a2aclient.Transport = (*a2aHTTPJSONTransport)(nil)

func newA2AHTTPJSONTransport(baseURL string, httpClient *http.Client) *a2aHTTPJSONTransport {
	return &a2aHTTPJSONTransport{baseURL: strings.TrimRight(baseURL, "/"), httpClient: httpClient}
}

func (t *a2aHTTPJSONTransport) SendMessage(ctx context.Context, params *a2a.MessageSendParams) (a2a.SendMessageResult, error) {
	data, err := t.request(ctx, http.MethodPost, "v1/message:send", params, nil)
	if err != nil {
		return nil, err
	}
	event, err := a2a.UnmarshalEventJSON(data)
	if err != nil {
		return nil, fmt.Errorf("HTTP+JSON message/send returned an invalid result: %w", err)
	}
	switch result := event.(type) {
	case *a2a.Task:
		return result, nil
	case *a2a.Message:
		return result, nil
	default:
		return nil, fmt.Errorf("HTTP+JSON message/send returned %T, expected Task or Message", event)
	}
}

func (t *a2aHTTPJSONTransport) SendStreamingMessage(ctx context.Context, params *a2a.MessageSendParams) iter.Seq2[a2a.Event, error] {
	return t.stream(ctx, "v1/message:stream", params)
}

func (t *a2aHTTPJSONTransport) GetTask(ctx context.Context, params *a2a.TaskQueryParams) (*a2a.Task, error) {
	query := url.Values{}
	if params.HistoryLength != nil {
		query.Set("historyLength", strconv.Itoa(*params.HistoryLength))
	}
	var result a2a.Task
	if err := t.decode(ctx, http.MethodGet, "v1/tasks/"+url.PathEscape(string(params.ID)), nil, query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) ListTasks(ctx context.Context, params *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	query := url.Values{}
	if params.ContextID != "" {
		query.Set("contextId", params.ContextID)
	}
	if params.Status != "" {
		query.Set("status", string(params.Status))
	}
	if params.PageSize != 0 {
		query.Set("pageSize", strconv.Itoa(params.PageSize))
	}
	if params.PageToken != "" {
		query.Set("pageToken", params.PageToken)
	}
	if params.HistoryLength != 0 {
		query.Set("historyLength", strconv.Itoa(params.HistoryLength))
	}
	if params.LastUpdatedAfter != nil {
		query.Set("lastUpdatedAfter", params.LastUpdatedAfter.Format(time.RFC3339))
	}
	if params.IncludeArtifacts {
		query.Set("includeArtifacts", "true")
	}
	var result a2a.ListTasksResponse
	if err := t.decode(ctx, http.MethodGet, "v1/tasks", nil, query, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) CancelTask(ctx context.Context, params *a2a.TaskIDParams) (*a2a.Task, error) {
	var result a2a.Task
	endpoint := "v1/tasks/" + url.PathEscape(string(params.ID)) + ":cancel"
	body := map[string]string{"name": "tasks/" + string(params.ID)}
	if err := t.decode(ctx, http.MethodPost, endpoint, body, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) ResubscribeToTask(ctx context.Context, params *a2a.TaskIDParams) iter.Seq2[a2a.Event, error] {
	return t.stream(ctx, "v1/tasks/"+url.PathEscape(string(params.ID))+":subscribe", map[string]string{"name": "tasks/" + string(params.ID)})
}

func (t *a2aHTTPJSONTransport) GetTaskPushConfig(ctx context.Context, params *a2a.GetTaskPushConfigParams) (*a2a.TaskPushConfig, error) {
	var result a2a.TaskPushConfig
	endpoint := "v1/tasks/" + url.PathEscape(string(params.TaskID)) + "/pushNotificationConfigs/" + url.PathEscape(params.ConfigID)
	if err := t.decode(ctx, http.MethodGet, endpoint, nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) ListTaskPushConfig(
	ctx context.Context, params *a2a.ListTaskPushConfigParams,
) ([]*a2a.TaskPushConfig, error) {
	var result []*a2a.TaskPushConfig
	endpoint := "v1/tasks/" + url.PathEscape(string(params.TaskID)) + "/pushNotificationConfigs"
	if err := t.decode(ctx, http.MethodGet, endpoint, nil, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (t *a2aHTTPJSONTransport) SetTaskPushConfig(ctx context.Context, params *a2a.TaskPushConfig) (*a2a.TaskPushConfig, error) {
	var result a2a.TaskPushConfig
	endpoint := "v1/tasks/" + url.PathEscape(string(params.TaskID)) + "/pushNotificationConfigs"
	if err := t.decode(ctx, http.MethodPost, endpoint, map[string]any{"config": params}, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) DeleteTaskPushConfig(ctx context.Context, params *a2a.DeleteTaskPushConfigParams) error {
	endpoint := "v1/tasks/" + url.PathEscape(string(params.TaskID)) + "/pushNotificationConfigs/" + url.PathEscape(params.ConfigID)
	_, err := t.request(ctx, http.MethodDelete, endpoint, nil, nil)
	return err
}

func (t *a2aHTTPJSONTransport) GetAgentCard(ctx context.Context) (*a2a.AgentCard, error) {
	var result a2a.AgentCard
	if err := t.decode(ctx, http.MethodGet, "v1/card", nil, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (t *a2aHTTPJSONTransport) Destroy() error { return nil }

func (t *a2aHTTPJSONTransport) decode(ctx context.Context, method, endpoint string, body any, query url.Values, target any) error {
	data, err := t.request(ctx, method, endpoint, body, query)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode HTTP+JSON response: %w", err)
	}
	return nil
}

func (t *a2aHTTPJSONTransport) request(ctx context.Context, method, endpoint string, body any, query url.Values) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal HTTP+JSON request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, t.endpoint(endpoint, query), reader)
	if err != nil {
		return nil, fmt.Errorf("create HTTP+JSON request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if meta, ok := a2aclient.CallMetaFrom(ctx); ok {
		for key, values := range meta {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}
	resp, err := t.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send HTTP+JSON request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read HTTP+JSON response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("HTTP+JSON request failed: %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	return data, nil
}

func (t *a2aHTTPJSONTransport) stream(ctx context.Context, endpoint string, body any) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		data, err := json.Marshal(body)
		if err != nil {
			yield(nil, err)
			return
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint(endpoint, nil), bytes.NewReader(data))
		if err != nil {
			yield(nil, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		resp, err := t.httpClient.Do(req)
		if err != nil {
			yield(nil, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			yield(nil, fmt.Errorf("HTTP+JSON stream failed: %s", resp.Status))
			return
		}
		for raw, err := range a2aSSEData(resp.Body) {
			if err != nil {
				yield(nil, err)
				return
			}
			event, err := a2aHTTPJSONEvent(raw)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(event, nil) {
				return
			}
		}
	}
}

func (t *a2aHTTPJSONTransport) endpoint(suffix string, query url.Values) string {
	u, _ := url.Parse(t.baseURL)
	if strings.HasSuffix(strings.TrimRight(u.Path, "/"), "/v1") {
		suffix = strings.TrimPrefix(suffix, "v1/")
	}
	u.Path = path.Join(u.Path, suffix)
	u.RawQuery = query.Encode()
	return u.String()
}

func a2aSSEData(body io.Reader) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		scanner := bufio.NewScanner(body)
		var data []string
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				if len(data) > 0 && !yield([]byte(strings.Join(data, "\n")), nil) {
					return
				}
				data = nil
				continue
			}
			if value, ok := strings.CutPrefix(line, "data:"); ok {
				data = append(data, strings.TrimPrefix(value, " "))
			}
		}
		if err := scanner.Err(); err != nil {
			yield(nil, fmt.Errorf("read HTTP+JSON SSE stream: %w", err))
			return
		}
		if len(data) > 0 {
			yield([]byte(strings.Join(data, "\n")), nil)
		}
	}
}

func a2aHTTPJSONEvent(raw []byte) (a2a.Event, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("decode HTTP+JSON stream event: %w", err)
	}
	for _, key := range []string{"message", "task", "statusUpdate", "artifactUpdate"} {
		if value := envelope[key]; value != nil {
			return a2a.UnmarshalEventJSON(value)
		}
	}
	return nil, fmt.Errorf("HTTP+JSON stream event has no A2A payload")
}
