#!/usr/bin/env python3
"""Validate every Codex App Server shape used by the portal."""

import json
import pathlib
import re
import sys
from collections import Counter

from jsonschema import Draft7Validator


SCHEMA_DIR = pathlib.Path(sys.argv[1])


def validate(file_name, value, label):
    schema = json.loads((SCHEMA_DIR / file_name).read_text())
    errors = sorted(Draft7Validator(schema).iter_errors(value), key=lambda error: list(error.path))
    if errors:
        details = "\n".join(f"  {list(error.path)}: {error.message}" for error in errors)
        raise SystemExit(f"{label} is incompatible with {file_name}:\n{details}")


def require_fields(file_name, value, paths, label):
    schema = json.loads((SCHEMA_DIR / file_name).read_text())
    validator = Draft7Validator(schema)
    for path in paths:
        candidate = json.loads(json.dumps(value))
        parent = candidate
        for component in path[:-1]:
            parent = parent[component]
        del parent[path[-1]]
        if validator.is_valid(candidate):
            rendered = ".".join(str(component) for component in path)
            raise SystemExit(
                f"{label} requires {rendered}, but {file_name} permits it to be absent"
            )


def require_declared_property(file_name, definition, property_name, label):
    schema = json.loads((SCHEMA_DIR / file_name).read_text())
    properties = schema.get("definitions", {}).get(definition, {}).get("properties", {})
    if property_name not in properties:
        raise SystemExit(
            f"{label} requires declared {definition}.{property_name}, "
            f"but {file_name} does not define it"
        )


def request(method, params):
    return {"id": 1, "method": method, "params": params}


environment = {
    "VPSFREE_DEV_SESSION_SLUG": "example",
    "VPSFREE_DEV_SESSION_WORKSPACE": "/workspace",
}
text_input = [{"type": "text", "text": "continue"}]

client_requests = [
    request(
        "initialize",
        {
            "capabilities": {"experimentalApi": True},
            "clientInfo": {
                "name": "vpsfree-workspace-portal",
                "title": "vpsFree.cz Workspace Portal",
                "version": "0.1.0",
            },
        },
    ),
    request(
        "thread/start",
        {
            "cwd": "/workspace/work/example",
            "runtimeWorkspaceRoots": ["/workspace"],
            "config": {"shell_environment_policy": {"set": environment}},
        },
    ),
    request(
        "thread/resume",
        {
            "threadId": "thread-1",
            "cwd": "/workspace/work/example",
            "excludeTurns": True,
            "config": {"shell_environment_policy": {"set": environment}},
        },
    ),
    request(
        "model/list",
        {"limit": 100, "includeHidden": False},
    ),
    request(
        "thread/resume",
        {
            "threadId": "thread-1",
            "cwd": "/workspace/work/example",
            "excludeTurns": True,
            "model": "test-model",
            "config": {"model_reasoning_effort": "high"},
        },
    ),
    request(
        "thread/fork",
        {
            "threadId": "thread-1",
            "cwd": "/workspace/work/fork",
            "runtimeWorkspaceRoots": ["/workspace"],
            "excludeTurns": True,
            "deferGoalContinuation": True,
            "model": "test-model",
            "config": {
                "shell_environment_policy": {"set": environment},
                "model_reasoning_effort": "high",
            },
        },
    ),
    request("thread/resume", {"threadId": "thread-1", "excludeTurns": True}),
    request("thread/resume", {"threadId": "thread-1", "excludeTurns": True}),
    request(
        "thread/list",
        {
            "cwd": "/workspace/work/example",
            "limit": 2,
            "sortDirection": "asc",
            "sourceKinds": ["vscode"],
        },
    ),
    request(
        "thread/list",
        {
            "cwd": "/workspace/work/fork",
            "limit": 2,
            "sortDirection": "asc",
            "sourceKinds": ["vscode"],
        },
    ),
    request("thread/name/set", {"threadId": "thread-1", "name": "example"}),
    request("thread/read", {"threadId": "thread-1"}),
    request("thread/read", {"threadId": "thread-1"}),
    request("thread/read", {"threadId": "thread-1"}),
    request("thread/read", {"threadId": "thread-1"}),
    request("thread/loaded/list", {"limit": 100}),
    request(
        "thread/turns/list",
        {"threadId": "thread-1", "limit": 20, "sortDirection": "desc", "itemsView": "full"},
    ),
    request(
        "thread/turns/list",
        {"threadId": "thread-1", "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
    ),
    request(
        "thread/turns/list",
        {"threadId": "thread-1", "limit": 1, "sortDirection": "asc", "itemsView": "full"},
    ),
    request(
        "thread/turns/list",
        {"threadId": "thread-1", "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
    ),
    request(
        "thread/items/list",
        {"threadId": "thread-1", "limit": 100, "sortDirection": "desc"},
    ),
    request("thread/unsubscribe", {"threadId": "thread-1"}),
    request("turn/start", {"threadId": "thread-1", "input": text_input}),
    request("turn/start", {"threadId": "thread-1", "input": text_input}),
    request(
        "turn/steer",
        {"threadId": "thread-1", "expectedTurnId": "turn-1", "input": text_input},
    ),
    request("turn/interrupt", {"threadId": "thread-1", "turnId": "turn-1"}),
]
for message in client_requests:
    validate("ClientRequest.json", message, f"client request {message['method']}")
client_source = (
    pathlib.Path(__file__).resolve().parents[1] / "portal/internal/codex/client.go"
).read_text()
call_pattern = re.compile(
    r'\b(?:Request|requestConnected|requestOn)\s*\([^)]*?"([a-z]+(?:/[A-Za-z]+)*)"',
    re.DOTALL,
)
implemented_calls = Counter(call_pattern.findall(client_source))
covered_calls = Counter(message["method"] for message in client_requests)
if implemented_calls != covered_calls:
    raise SystemExit(
        "Codex request call sites and schema corpus differ: "
        f"implemented={implemented_calls}, covered={covered_calls}"
    )

validate("ClientNotification.json", {"method": "initialized"}, "initialized notification")

server_requests = [
    request(
        "item/commandExecution/requestApproval",
        {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-command",
            "startedAtMs": 1,
            "command": "echo test",
            "cwd": "/workspace",
        },
    ),
    request(
        "item/fileChange/requestApproval",
        {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-file",
            "startedAtMs": 1,
        },
    ),
    request(
        "item/permissions/requestApproval",
        {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-permissions",
            "startedAtMs": 1,
            "cwd": "/workspace",
            "permissions": {},
        },
    ),
    request(
        "item/tool/requestUserInput",
        {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-input",
            "isBlocking": True,
            "questions": [
                {
                    "id": "choice",
                    "header": "Choice",
                    "question": "Choose",
                    "isOther": True,
                    "options": [{"label": "First", "description": "first choice"}],
                }
            ],
        },
    ),
]
for message in server_requests:
    validate("ServerRequest.json", message, f"server request {message['method']}")
supported_server_methods = set(
    re.findall(r'case "(item/[^"]+/request(?:Approval|UserInput))"', client_source)
)
covered_server_methods = {message["method"] for message in server_requests}
if supported_server_methods != covered_server_methods:
    raise SystemExit(
        "handled Codex server requests and schema corpus differ: "
        f"implemented={supported_server_methods}, covered={covered_server_methods}"
    )

command_with_decisions = json.loads(json.dumps(server_requests[0]))
command_with_decisions["params"]["availableDecisions"] = [
    "accept",
    "acceptForSession",
    "decline",
    "cancel",
]
validate(
    "ServerRequest.json",
    command_with_decisions,
    "server command approval request with advertised decisions",
)

validate(
    "ServerNotification.json",
    {"method": "serverRequest/resolved", "params": {"requestId": 1, "threadId": "thread-1"}},
    "resolved-request notification",
)

for decision in ("accept", "acceptForSession", "decline", "cancel"):
    validate(
        "CommandExecutionRequestApprovalResponse.json",
        {"decision": decision},
        f"command approval response {decision}",
    )
    validate(
        "FileChangeRequestApprovalResponse.json",
        {"decision": decision},
        f"file-change approval response {decision}",
    )
validate(
    "ToolRequestUserInputResponse.json",
    {"answers": {"choice": {"answers": ["First"]}}},
    "user-input response",
)

thread = {
    "cliVersion": "test",
    "createdAt": 1,
    "cwd": "/workspace/work/example",
    "ephemeral": False,
    "historyMode": "paginated",
    "id": "thread-1",
    "modelProvider": "openai",
    "path": "/workspace/.codex/session.jsonl",
    "preview": "example",
    "projectId": None,
    "sessionId": "session-1",
    "source": "vscode",
    "status": {"type": "idle"},
    "turns": [],
    "updatedAt": 1,
}
turn = {"id": "turn-1", "items": [], "status": "completed"}
command_item = {
    "id": "item-command",
    "type": "commandExecution",
    "command": "echo test",
    "commandActions": [],
    "cwd": "/workspace",
    "status": "completed",
    "exitCode": 0,
}
file_item = {
    "id": "item-file",
    "type": "fileChange",
    "changes": [],
    "status": "completed",
}
transcript_turn = {
    "id": "turn-1",
    "status": "completed",
    "items": [
        {"id": "item-user", "type": "userMessage", "content": [{"type": "text", "text": "request"}]},
        {"id": "item-agent", "type": "agentMessage", "text": "response"},
        command_item,
        file_item,
        {
            "id": "item-tool",
            "type": "mcpToolCall",
            "arguments": {},
            "server": "server",
            "status": "completed",
            "tool": "tool",
        },
        {"id": "item-reasoning", "type": "reasoning", "summary": ["summary"]},
        {"id": "item-plan", "type": "plan", "text": "plan"},
    ],
}

validate(
    "v1/InitializeResponse.json",
    {"codexHome": "/home/test/.codex", "platformFamily": "unix", "platformOs": "linux", "userAgent": "test"},
    "initialize result",
)
common_start = {
    "approvalPolicy": "never",
    "approvalsReviewer": "user",
    "cwd": "/workspace/work/example",
    "model": "test-model",
    "modelProvider": "openai",
    "sandbox": {"type": "dangerFullAccess"},
    "thread": thread,
}
validate("v2/ThreadStartResponse.json", common_start, "thread/start result")
validate("v2/ThreadResumeResponse.json", common_start, "thread/resume result")
fork_thread = dict(thread)
fork_thread["id"] = "thread-fork"
fork_thread["cwd"] = "/workspace/work/fork"
fork_thread["forkedFromId"] = "thread-1"
validate(
    "v2/ThreadForkResponse.json",
    {**common_start, "cwd": "/workspace/work/fork", "thread": fork_thread},
    "thread/fork result",
)
model = {
    "id": "model-id",
    "model": "test-model",
    "displayName": "Test model",
    "description": "Test model for the protocol contract.",
    "isDefault": True,
    "hidden": False,
    "defaultReasoningEffort": "high",
    "supportedReasoningEfforts": [
        {"reasoningEffort": "high", "description": "Thorough reasoning."}
    ],
}
validate(
    "v2/ModelListResponse.json",
    {"data": [model], "nextCursor": None},
    "model/list result",
)
validate("v2/ThreadListResponse.json", {"data": [thread]}, "thread/list result")
validate(
    "v2/ThreadLoadedListResponse.json",
    {"data": ["thread-1"], "nextCursor": None},
    "thread/loaded/list result",
)
validate(
    "v2/ThreadLoadedListParams.json",
    {"limit": 100, "cursor": "next-page"},
    "thread/loaded/list pagination params",
)
validate("v2/ThreadReadResponse.json", {"thread": thread}, "thread/read result")
validate(
    "v2/ThreadTurnsListResponse.json",
    {"data": [transcript_turn]},
    "thread/turns/list result",
)
validate(
    "v2/ThreadItemsListResponse.json",
    {"data": [{"turnId": "turn-1", "item": command_item}, {"turnId": "turn-1", "item": file_item}]},
    "thread/items/list result",
)
validate("v2/ThreadUnsubscribeResponse.json", {"status": "unsubscribed"}, "thread/unsubscribe result")
validate("v2/ThreadSetNameResponse.json", {}, "thread/name/set result")
validate("v2/TurnStartResponse.json", {"turn": turn}, "turn/start result")
validate("v2/TurnSteerResponse.json", {"turnId": "turn-1"}, "turn/steer result")
validate("v2/TurnInterruptResponse.json", {}, "turn/interrupt result")
validate("JSONRPCResponse.json", {"id": 1, "result": {}}, "successful response envelope")
validate(
    "JSONRPCError.json",
    {
        "id": 1,
        "error": {"code": -32601, "message": "workspace portal does not support this request"},
    },
    "unsupported-request error envelope",
)

for file_name, value, paths, label in [
    (
        "v2/ThreadStartResponse.json",
        common_start,
        [["thread"], ["thread", "id"], ["thread", "cwd"]],
        "thread/start result",
    ),
    (
        "v2/ThreadResumeResponse.json",
        common_start,
        [["thread"], ["thread", "id"], ["thread", "cwd"]],
        "thread/resume result",
    ),
    (
        "v2/ThreadForkResponse.json",
        {**common_start, "cwd": "/workspace/work/fork", "thread": fork_thread},
        [["thread"], ["thread", "id"], ["thread", "cwd"]],
        "thread/fork result",
    ),
    (
        "v2/ModelListResponse.json",
        {"data": [model], "nextCursor": None},
        [["data"]],
        "model/list result",
    ),
    (
        "v2/ThreadListResponse.json",
        {"data": [thread]},
        [["data"]],
        "thread/list result",
    ),
    (
        "v2/ThreadLoadedListResponse.json",
        {"data": ["thread-1"], "nextCursor": None},
        [["data"]],
        "thread/loaded/list result",
    ),
    (
        "v2/ThreadReadResponse.json",
        {"thread": thread},
        [
            ["thread"],
            ["thread", "id"],
            ["thread", "cwd"],
            ["thread", "source"],
            ["thread", "ephemeral"],
            ["thread", "preview"],
            ["thread", "status"],
            ["thread", "turns"],
        ],
        "thread/read result",
    ),
    (
        "v2/ThreadTurnsListResponse.json",
        {"data": [transcript_turn]},
        [["data"]],
        "thread/turns/list result",
    ),
    (
        "v2/ThreadItemsListResponse.json",
        {"data": [{"turnId": "turn-1", "item": command_item}]},
        [["data"], ["data", 0, "turnId"], ["data", 0, "item"]],
        "thread/items/list result",
    ),
]:
    require_fields(file_name, value, paths, label)

# Thread.path is intentionally nullable for remote and ephemeral Codex threads,
# but the portal requires the property to exist and validates a non-null path at
# runtime for its local persistent App Server threads.
require_declared_property(
    "v2/ThreadReadResponse.json", "Thread", "path", "thread/read result"
)
require_declared_property(
    "v2/ThreadReadResponse.json", "Thread", "model", "thread/read result"
)
require_declared_property(
    "v2/ThreadReadResponse.json", "Thread", "reasoningEffort", "thread/read result"
)
require_declared_property(
    "v2/ThreadForkResponse.json", "Thread", "forkedFromId", "thread/fork result"
)
require_declared_property(
    "v2/ThreadListResponse.json", "Thread", "forkedFromId", "thread/list fork recovery result"
)
for field in (
    "model",
    "displayName",
    "description",
    "isDefault",
    "defaultReasoningEffort",
    "supportedReasoningEfforts",
):
    require_declared_property("v2/ModelListResponse.json", "Model", field, "model/list result")

for message in server_requests:
    require_fields(
        "ServerRequest.json",
        message,
        [["params"], ["params", "threadId"], ["params", "turnId"], ["params", "itemId"]],
        f"server request {message['method']}",
    )

print("Codex App Server protocol contract is compatible")
