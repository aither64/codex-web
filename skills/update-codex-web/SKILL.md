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

The full fixture in codex/testdata/ephemeral-models.json is the unchanged source
models-manager/models.json. Refresh its provenance and hash. Derive
codex/ephemeral_luna_models.json from that same full catalog, selecting only Luna
and changing its tool_mode to direct, experimental_supported_tools to an empty
list, and use_responses_lite to false. Preserve all other Luna metadata.

```sh
python3 skills/update-codex-web/scripts/refresh-catalogs.py \
  --source "$codex_source" --version "$codex_version"
```

The helper writes both catalogs and source provenance and prints their hashes.

Update the corresponding immutable hashes in codex/ephemeral.go, tests and the
reference. Keep the catalog's exact bytes reproducible. Preserve both supported
model/effort pairs unless the requested upgrade explicitly changes that contract.
Do not broaden accepted catalog contents to make an upgrade pass.

## Verify and return evidence

Run focused Go/browser checks, request corpus coverage and the exact schema and
config-policy checks before final review. Follow the caller's normal commit,
review and publication gates; when orchestrated, return the completed committed
changes for its combined final review before long native tests.

After review, follow docs/reference.md for namespace ownership, exact-binary
hostile-dispatch integration and both model profiles. Retain the binary hash,
catalog identity, actual tool exposure/rejection and process teardown evidence.
Missing prerequisites or failed isolation block release. Fake transports and
schema checks alone do not establish native isolation.

Return the final source revision, selected Codex/source identities, catalog
hashes, API/compatibility changes, executed checks and remaining release gates.
The consuming runtime owns its dependency pin and deployment. Existing callers
must update their catalog and library selection together.
