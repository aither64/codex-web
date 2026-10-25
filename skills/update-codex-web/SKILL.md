---
name: update-codex-web
description: Update codex-web compatibility for a selected Codex release, including protocol contracts, immutable model catalogs and native utility verification. Use for Codex version upgrades; this repository does not select a runtime package.
---

# Update Codex compatibility

Read this repository's AGENTS.md and the development and ephemeral utility
sections of docs/reference.md. A consuming application supplies the exact Codex
version, executable and source. If called directly, establish those inputs before
changing the compatibility contract. Do not add a binary dependency to this
repository's flake merely to select a release.

The strict `RunEphemeralTurn` contract supports Codex 0.163.0 with both existing
model profiles. `RunPromptedEphemeralTurn` supports naming with raw Codex 0.164.0
and Luna/low. The prompted contract retains the bounded runner and fixed
defenses. Its prompt guides the model without establishing tool-effect isolation. Preserve both API
contracts; do not relabel a prompted native pass as strict isolation evidence.
The consuming runtime verifies the exact executable. Identical catalog bytes or
a pathname cannot identify a native version.

## Inspect the selected release

Record the exact executable's version and SHA-256, its package/source revisions
and any downstream patches. Compare upstream changes since the supported release
for request/event shapes, config keys and defaults, native tools, catalog fields,
daemon startup, persistence and cancellation. A version string does not identify
a patched executable or an already-running daemon.

Generate schemas from that executable in disposable private state:

```sh
CODEX_HOME="$probe_home" "$codex_binary" app-server generate-json-schema \
  --experimental --out "$schema_dir"
python3 test/codex_protocol_contract.py "$schema_dir" codex/client.go
python3 test/codex_ephemeral_config_contract.py "$codex_source/core/config.schema.json"
```

Use absolute paths belonging to this update. Run tools in the repository's Nix
environment. The config source must belong to the exact built candidate,
including relevant downstream changes. Preserve the request-corpus call-site
coverage and validation of fields actually consumed by the client. Adapt client
code only where the selected protocol requires it; retain public capabilities
and fail-closed utility behavior.

## Refresh catalogs together

The full fixture in codex/testdata/ephemeral-models.json is the unchanged 0.163
source models-manager/models.json. The 0.164 source is byte-identical. Retain
one canonical resource per catalog and provenance for both releases. Derive
codex/ephemeral_luna_models.json from that same full catalog, selecting only Luna
and changing its tool_mode to direct, experimental_supported_tools to an empty
list, and use_responses_lite to false. Preserve all other Luna metadata.

```sh
python3 skills/update-codex-web/scripts/refresh-catalogs.py \
  --source "$codex_source" --version "$codex_version"
```

For `--version 0.163.0`, the helper regenerates the strict catalogs and their
original provenance. For `--version 0.164.0`, it verifies that both generated
catalogs match the existing canonical resources, then writes separate prompted
provenance without overwriting the strict resource or provenance. It prints both
hashes. A mismatch requires compatibility inspection; do not manufacture version
distinctions through changed metadata, copied JSON files or trusted pathnames.

Keep corresponding immutable hashes in codex/ephemeral.go, tests and the
reference aligned with actual catalog content. Preserve both strict model/effort
pairs and the fixed prompted Luna/low pair. Do not broaden accepted contents or
change ApplyPatch metadata to make an upgrade pass.

## Verify and return evidence

Run focused Go/browser checks, request corpus coverage and the exact schema and
config-policy checks before final review. Follow the caller's normal commit,
review and publication gates; when orchestrated, return the completed committed
changes for its combined final review before long native tests.

After review, follow docs/reference.md for namespace ownership and the native
selector matching the supported contract. Strict 0.163 requires hostile-dispatch
integration with both model profiles. Prompted raw 0.164 naming records actual
tool schemas without requiring an empty list or absent registry handlers. It
still checks fixed requests, output, retry proof, question rejection, deadlines
and process teardown. Retain binary/source/patch/catalog identities and actual
observations. Missing prerequisites or failed required checks block release.
Fake transports and schemas establish neither native isolation nor real-provider
adherence to naming instructions. The consuming runtime owns its real naming
canary and native state/activation verification.

Return the final source revision, selected Codex/source identities, catalog
hashes, API/compatibility changes, executed checks and remaining release gates.
The consuming runtime owns its dependency pin and deployment. Existing callers
must update their catalog and library selection together.
