# Call

The Call task allows you to make calls to external services.

## When to use this

Use Call when your workflow needs to:

- Make an HTTP request to an external API
- Invoke a Temporal activity on another task queue
- Call a gRPC service
- Interact with an A2A agent

## Properties

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| call | `string` | `yes` | The name of the function to call. One of `activity`, `grpc`, `http` or `a2a`. |
| with | `map` | `no` | A name/value mapping of the parameters to call the function with |

## Activity

Call an external [Temporal activity](https://docs.temporal.io/activities) running
on a separate [Task Queue](https://docs.temporal.io/task-queue#task-queue) within
the same namespace.

To use this, the `call` property must equal `activity`.

### Properties {/*#activity-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| name | `string` | `yes` | The activity name to call. |
| arguments | `any[]` | `no` | The arguments to pass to the activity. These are interpolated through the state. |
| taskQueue | `string` | `yes` | The task queue where the activity is running. |

### Example {/*#activity-example*/}

```yaml
document:
  dsl: 1.0.0
  taskQueue: zigflow
  workflowType: external-activity-call
  version: 0.0.1
do:
  - captureInput:
      set:
        requestedUserId: ${ $input.userId }
        requestId: ${ uuid }
  - fetchProfile:
      call: activity
      with:
          name: activitycall.FetchProfile
          arguments:
            - ${ $data.requestedUserId }
            - ${ $data.requestId }
          taskQueue: activity-call-worker
```

## gRPC

Call an external resource via gRPC. To use this, the `call` property must equal
`grpc`.

### Properties {/*#grpc-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| proto.endpoint | `string` | `yes` | The proto resource that describes the gRPC service to call.. |
| service.name | `string` | `yes` | The name of the gRPC service to call. |
| service.host | `string` | `yes` | The hostname of the gRPC service to call. |
| service.port | `integer` | `no` | The port number of the gRPC service to call. |
| method | `string` | `yes` | The name of the gRPC service method to call. |
| arguments | `map` | `no` | A name/value mapping of the method call's arguments, if any. |

### Example {/*#grpc-example*/}

```yaml
document:
  dsl: 1.0.0
  taskQueue: zigflow
  workflowType: external-activity-call
  version: 0.0.1
do:
  - greet:
      call: grpc
      with:
        proto:
          endpoint: file:///go/app/examples/external-calls/grpc/basic/proto/basic/v1/basic.proto
        service:
          name: providers.v1.BasicService
          host: grpc
          port: 3000
        method: Command1
        arguments:
          input: hello world
```

## HTTP

Call an external resource via HTTP. To use this, the `call` property must equal
`http`.

### Properties {/*#http-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| method | `string` | `yes` | The HTTP request method. |
| endpoint | `string`\|[`endpoint`](https://github.com/open-workflow-specification/specification/blob/main/dsl-reference.md#endpoint) | `yes` | An URI or an object that describes the HTTP endpoint to call. |
| headers | `map` | `no` | A name/value mapping of the HTTP headers to use, if any. |
| body | `any` | `no` | The HTTP request body, if any. *Encoded to match the declared `Content-Type` — see [Gotchas](#gotchas).* |
| query | `map[string, any]` | `no` | A name/value mapping of the query parameters to use, if any. |
| output | `string` | `no` | The http call's output format.<br />*Supported values are:*<br />*- `raw`, which output's the base-64 encoded [http response](https://github.com/open-workflow-specification/specification/blob/main/dsl-reference.md#http-response) content, if any.*<br />*- `content`, which outputs the content of [http response](https://github.com/open-workflow-specification/specification/blob/main/dsl-reference.md#http-response), possibly deserialized.*<br />*- `response`, which outputs the [http response](https://github.com/open-workflow-specification/specification/blob/main/dsl-reference.md#http-response).*<br />*Defaults to `content`.* |
| redirect | `boolean` | `no` | Specifies whether redirection status codes (`300–399`) should be treated as errors.<br />*If set to `false`, runtimes must raise an error for response status codes outside the `200–299` range.*<br />*If set to `true`, they must raise an error for status codes outside the `200–399` range.*<br />*Defaults to `false`.* |

### Example {/*#http-example*/}

```yaml
document:
  dsl: 1.0.0
  taskQueue: zigflow
  workflowType: call-http
  version: 0.0.1
do:
  - getUser:
      call: http
      with:
        method: get
        endpoint: https://jsonplaceholder.typicode.com/users/2
```

## A2A

Call an [Agent2Agent (A2A)](https://a2a-protocol.org/v0.3.0/specification/)
agent. Zigflow implements the Open Workflow Specification 1.0.3 `call: a2a`
contract with A2A protocol version 0.3.0.

The call runs as a Temporal activity. Agent Card discovery, transport selection
and network I/O therefore do not run in workflow code. A2A `messageId` values
that Zigflow has to supply are deterministic for an activity, so Temporal retry
attempts keep the same A2A message identity.

### Properties {/*#a2a-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| method | `string` | `yes` | An A2A JSON-RPC method. Supported methods are `message/send`, `message/stream`, `tasks/get`, `tasks/list`, `tasks/cancel`, `tasks/resubscribe`, the `tasks/pushNotificationConfig/*` methods, and `agent/getAuthenticatedExtendedCard`. |
| agentCard | `externalResource` | conditional | The URL of the Agent Card. Required when `server` is not set. Its `endpoint` identifies the Agent Card itself, including a non-standard card path. Zigflow follows the card's declared preferred transport and can use its declared additional interfaces. |
| server | `string` or `endpoint` | conditional | An A2A JSON-RPC server endpoint. Required when `agentCard` is not set. Ignored when an Agent Card is supplied. |
| parameters | `map` | no | Parameters for the selected A2A method. Runtime expressions are evaluated before the request is sent and may resolve to a map. |

For `message/send` and `message/stream`, Zigflow defaults a missing
`parameters.message.messageId` and `parameters.message.role` to a stable UUID
and `user`, respectively. Non-streaming calls return the A2A result.
Streaming calls return the ordered array of all A2A events.

### Supported transports

| Target | Supported transport | Selection |
| --- | --- | --- |
| `server` | JSON-RPC | Always JSON-RPC. |
| `agentCard` | JSON-RPC, gRPC and HTTP+JSON | The Agent Card's `preferredTransport` is selected. Declared `additionalInterfaces` provide compatible fallback candidates. |

Zigflow uses the official `a2aproject/a2a-go` client for JSON-RPC and gRPC.
That SDK declares, but does not currently implement, HTTP+JSON. Zigflow supplies
its own A2A 0.3 HTTP+JSON adapter, including REST method mappings and SSE
streaming, so every standard A2A 0.3 transport can be used when an Agent Card
declares it.

### Transport interpretation

OWS 1.0.3 defines `method` as an “A2A JSON-RPC method”, but does not define a
transport-selection property. Zigflow applies the following interpretation:

- With `server`, the URI is a JSON-RPC endpoint. The `method` value is sent as
  the A2A JSON-RPC method.
- With `agentCard`, Zigflow reads the card's `preferredTransport` and uses that
  declared interface. Declared `additionalInterfaces` are compatible fallback
  candidates when Zigflow implements their transport.

The `method` value always identifies the logical A2A operation. For an Agent
Card that selects gRPC or HTTP+JSON, Zigflow maps the same value to A2A's gRPC
or REST operation. For example, `message/send` maps to `SendMessage` over gRPC
and `POST /v1/message:send` over HTTP+JSON. The returned value remains the
semantically equivalent A2A result. This is Zigflow's runtime interpretation
of the OWS wording, based on A2A 0.3 transport mappings; it is not an
additional OWS DSL property.

### Example {/*#a2a-example*/}

```yaml
document:
  dsl: 1.0.3
  taskQueue: zigflow
  workflowType: generate-report
  version: 0.0.1
do:
  - generateReport:
      call: a2a
      with:
        method: message/send
        agentCard:
          endpoint: https://reports.example.com/.well-known/agent-card.json
        parameters:
          message:
            parts:
              - kind: text
                text: Generate the Q1 sales report.
```

### Current limitations

> **Important:** This is not full A2A authentication support. Zigflow currently
> supports only A2A agents that do not require client authentication. Do not use
> this feature with an authenticated A2A agent.

Standard TLS server verification applies to HTTPS and A2A gRPC connections, but
Zigflow does not execute Agent Card security schemes or provide A2A credentials
to a remote agent. API-key and Bearer injection, OAuth/OIDC token acquisition
and refresh, and client certificates for mTLS are not supported. When an Agent
Card declares security requirements, Zigflow rejects the call before contacting
the agent. A direct `server` call has no Agent Card to inspect and must target
an agent that does not require client authentication.

The installed version of `github.com/open-workflow-specification/sdk-go/v4`
does not expose an A2A task model. Zigflow therefore uses its generic `call`
model and validates the A2A `with` object itself. The separately versioned
`github.com/zigflow/schema` package likewise has no native A2A variant yet;
Zigflow's validation command contains a temporary compatibility bridge.

The bridge clones the workflow only for validation, validates the A2A-specific
`with` object in Zigflow, and substitutes a valid HTTP call in that clone before
passing it to `zigflow/schema`. The source workflow is never changed. This keeps
the schema checks for every shared task field, nesting rule and non-A2A task
while allowing the Zigflow CLI and runtime to accept `call: a2a` today.

This is a compatibility measure, not native schema support. Studio and other
direct consumers of the published schema cannot discover or validate A2A until
`zigflow/schema` publishes an A2A variant. The bridge exists because changing
the independently versioned schema package is a separate upstream change, but
making Zigflow reject a valid A2A workflow until that release would make the
runtime feature unusable.

### Future work

The following work is intentionally outside this A2A-only addition:

- Build a general Zigflow authentication provider. It should map OWS endpoint
  authentication and A2A Agent Card security requirements to stored secrets and
  transport credentials, inject them into HTTP, gRPC and A2A calls, manage
  OAuth/OIDC token lifetime, and configure mTLS.
- Add a native A2A task model to `sdk-go` and an A2A call definition to
  `github.com/zigflow/schema`. After both are released, Zigflow should upgrade
  its dependencies, parse the typed A2A model directly, delete the validation
  bridge, and replace its bridge-specific tests with native schema validation.
  Studio and other schema consumers can then discover and advertise A2A safely.
- Contribute HTTP+JSON support upstream to `a2aproject/a2a-go`, allowing
  Zigflow to replace its local transport adapter with the official SDK
  implementation.
- Run interoperability tests against independently implemented A2A agents for
  each supported transport, including streaming, errors and push notification
  configuration.

## Gotchas

### HTTP calls

**The request body is encoded for the declared `Content-Type`.** A `body` given
as a mapping is form-encoded when the headers declare
`application/x-www-form-urlencoded`, and sent as JSON otherwise. A `body` given
as a string is sent verbatim under any non-JSON media type, so pre-encoded and
XML payloads are not JSON-quoted.

Header lookup is case-insensitive, and form keys are sorted so the output is
deterministic.

**HTTP errors raise by default.** Responses outside the success range raise an error.

By default, Zigflow classifies HTTP response codes as follows:

- `200–299`: success
- `300–399`: non-retryable error when `redirect: false`
- `408` and `429`: retryable error
- `400–499`: non-retryable error by default
- `500–599`: retryable error by default
- `501`: non-retryable error

When `redirect: true`, redirects are followed by the HTTP client, so `3xx`
responses are typically not returned to the workflow.

Retryable errors use the task's Temporal retry policy. Non-retryable errors fail
immediately unless handled in a `try` task.

When a retryable HTTP response includes a valid `Retry-After` header, Zigflow
delays the next retry until the requested time instead of using the normal retry
backoff.

`Retry-After` delays are not capped by Zigflow. Use Temporal timeout settings,
such as `ScheduleToCloseTimeout`, to enforce a maximum execution time.

### SDK metrics {/*#sdk-metrics*/}

Activity-dispatching tasks (`call: a2a`, `call: http`, `call: grpc`, and
`run:` tasks
of type Container, Script and Shell) are each registered on their worker
under a name derived from the task's path from the workflow root. For a
task at the top of a workflow's `do:` list, the registered name is
`<workflowType>.<taskName>` (for example `call-http.getUser`). For
nested tasks, the path includes each parent's name, so a task `step`
inside a `for:` task `loop` registers as `<workflowType>.loop.step`.
Try bodies use `try` and `catch` as path segments to disambiguate the
two scopes.

Temporal's SDK metrics use the registered name for the `activity_type`
label, so individual tasks can be attributed in dashboards and alerts
without colliding when sibling scopes happen to reuse a leaf name.
`run.workflow` is a child workflow and is not affected.

**Activity names are case-sensitive.** The `name` field in an activity call
must exactly match the name the activity was registered with on the remote
worker.

**gRPC proto files must be accessible.** The `proto.endpoint` path must be
readable by the Zigflow worker process at runtime.

## Related pages

- [Try](/docs/dsl/tasks/try): handling HTTP and activity errors
- [Raise](/docs/dsl/tasks/raise): raising explicit errors
- [Concepts: error handling and retries](/docs/concepts/error-handling-and-retries):
  retry policy configuration
- [Examples: HTTP call](/docs/examples/http-call): practical walkthrough
- [Examples: error handling](/docs/examples/error-handling):
  catching HTTP errors
