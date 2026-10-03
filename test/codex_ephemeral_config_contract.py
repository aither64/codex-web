#!/usr/bin/env python3
"""Check the fixed utility policy against the candidate's config schema.

App Server's arbitrary config JSON cannot detect misspelled/unsupported keys.
Invoke with core/config.schema.json from the same reviewed binary source.
"""
import json
import pathlib
import sys

from jsonschema import Draft7Validator

if len(sys.argv) not in (2, 3):
    raise SystemExit("usage: codex_ephemeral_config_contract.py CONFIG_SCHEMA [POLICY_JSON]")
schema = json.loads(pathlib.Path(sys.argv[1]).read_text())
policy_path = pathlib.Path(sys.argv[2]) if len(sys.argv) == 3 else (
    pathlib.Path(__file__).resolve().parents[1] / "codex/ephemeral_policy.json"
)
policy = json.loads(policy_path.read_text())


def resolve(node):
    if "$ref" in node:
        ref = node["$ref"]
        if not ref.startswith("#/"):
            raise SystemExit("external config schema reference is unsupported")
        target = schema
        for component in ref[2:].split("/"):
            target = target[component.replace("~1", "/").replace("~0", "~")]
        return resolve(target)
    return node


def declared(node, components):
    node = resolve(node)
    if not components:
        return True
    properties = node.get("properties", {})
    if components[0] in properties:
        return declared(properties[components[0]], components[1:])
    return any(declared(child, components) for branch in ("allOf", "anyOf", "oneOf")
               for child in node.get(branch, []))


expanded = {}
for key, value in policy.items():
    components = key.split(".")
    if not declared(schema, components):
        raise SystemExit(f"unsupported utility restriction: {key}")
    parent = expanded
    for component in components[:-1]:
        parent = parent.setdefault(component, {})
    parent[components[-1]] = value

# Configured MCP names are literal keys, including dots. This representative
# inherited server is complete for config-schema validation; production sends
# only the enabled override while retaining the operator's server definition.
expanded["mcp_servers"] = {"literal.name": {"command": "synthetic-mcp", "enabled": False}}
mcp = resolve(schema["properties"]["mcp_servers"])
if not declared(mcp["additionalProperties"], ["enabled"]):
    raise SystemExit("per-name MCP enabled restriction is unsupported")
errors = list(Draft7Validator(schema).iter_errors(expanded))
if errors:
    raise SystemExit("utility restriction type mismatch:\n" + "\n".join(
        f"  {'.'.join(map(str, error.path))}: {error.message}" for error in errors
    ))
print(f"validated {len(policy)} fixed utility restrictions and literal MCP enabled")
