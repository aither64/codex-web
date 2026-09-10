# Repository guidelines

## Repository structure

- `codex/` contains the public Go App Server client and generic durable
  operation primitives.
- `conversation/` contains the capability-checked HTTP handler and browser ES
  module.
- `cmd/codex-web-example/` is the standalone reference application.
- `test/` contains browser contracts and the Codex protocol request-corpus
  validator.

## Development commands

Use the repository's Nix environment. Run `nix flake check --print-build-logs`
for the packaged suite. Focused Go tests run from the directory containing
`go.mod`; browser checks use the Node.js supplied by the flake.

## Security and compatibility

The HTTP integration must resolve an opaque application conversation ID to a
trusted thread, canonical working directory, App Server connection and allowed
operations on every request. Do not accept a socket, thread ID or working
directory from browser input. Deny access when authorization is absent or
ambiguous.

Keep exact-origin checks, payload bounds, Markdown sanitization, approval and
question validation, retry receipts and private persistent-state permissions.
Treat every Codex version change as a protocol compatibility change and check
generated schemas plus behavior before release.

## Commits and tests

Use focused commits with subjects in `area: action` form. Explain public API,
security and compatibility effects in the commit body. Write commit messages
to a temporary file and pass it to `git commit -F`. Do not bypass declared
hooks.

Run quick checks before the mandatory change review. Run long live
App Server tests only after review findings are resolved or explicitly
accepted in the initiative state.
