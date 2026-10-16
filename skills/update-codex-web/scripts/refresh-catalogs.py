#!/usr/bin/env python3
"""Refresh immutable utility catalogs from the selected codex-rs source."""

import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="selected codex-rs source")
    parser.add_argument("--version", required=True, help="selected Codex version")
    args = parser.parse_args()
    original = (args.source / "models-manager/models.json").read_bytes()
    catalog = json.loads(original)
    models = [model for model in catalog["models"] if model["slug"] == "gpt-6-luna"]
    if len(models) != 1 or models[0].get("tool_mode") != "code_mode_only":
        raise SystemExit("selected source has an unsupported Luna profile; inspect it first")
    luna = models[0]
    luna["tool_mode"] = "direct"
    luna["experimental_supported_tools"] = []
    luna["use_responses_lite"] = False
    derived = (json.dumps({"models": [luna]}, ensure_ascii=False, indent=2) + "\n").encode()
    original_hash = hashlib.sha256(original).hexdigest()
    derived_hash = hashlib.sha256(derived).hexdigest()
    root = Path(__file__).resolve().parents[3]
    (root / "codex/testdata/ephemeral-models.json").write_bytes(original)
    (root / "codex/ephemeral_luna_models.json").write_bytes(derived)
    (root / "codex/testdata/ephemeral-models.provenance").write_text(
        f"Full unchanged Codex {args.version} codex-rs/models-manager/models.json.\n"
        "Source selected by the runtime locked Codex input.\n"
        f"{len(original)} bytes; SHA-256\n{original_hash}\n"
        "Used for hermetic public helper preflight, with no metadata reductions.\n"
    )
    print(json.dumps({"version": args.version, "full_sha256": original_hash,
                      "luna_sha256": derived_hash}, indent=2))


if __name__ == "__main__":
    main()
