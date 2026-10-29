#!/usr/bin/env python3
"""Refresh immutable utility catalogs from the selected codex-rs source."""

import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path, help="selected codex-rs source")
    parser.add_argument("--version", required=True, choices=("0.163.0", "0.164.0", "0.165.0"),
                        help="strict 0.163.0 or prompted 0.164.0/0.165.0 provenance")
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
    full_path = root / "codex/testdata/ephemeral-models.json"
    luna_path = root / "codex/ephemeral_luna_models.json"
    if args.version == "0.165.0":
        # 0.165 adds Terra to the full catalog; every retained model is unchanged.
        old = json.loads(full_path.read_bytes())
        old_models = {model["slug"]: model for model in old["models"]}
        new_models = {model["slug"]: model for model in json.loads(original)["models"]}
        if (original_hash != "1384625c0b6b3e67d3c60da29becea2b017f2c283fc1e2d05b8edafb4698eb10"
                or set(new_models) - set(old_models) != {"gpt-6.1-terra"}
                or any(new_models.get(key) != model for key, model in old_models.items())
                or luna_path.read_bytes() != derived):
            raise SystemExit("0.165 catalog differs from reviewed source; inspect compatibility first")
    elif args.version == "0.164.0":
        # Both supported contracts share these exact bytes. Do not overwrite
        # strict fixtures merely to make a selected release validate.
        if full_path.read_bytes() != original or luna_path.read_bytes() != derived:
            raise SystemExit("0.164 catalogs differ from strict resources; inspect compatibility first")
    else:
        full_path.write_bytes(original)
        luna_path.write_bytes(derived)
    provenance = root / "codex/testdata/ephemeral-models.provenance"
    if args.version == "0.163.0":
        provenance.write_text(
            "Full unchanged Codex 0.163.0 codex-rs/models-manager/models.json.\n"
            "Source selected by the runtime locked Codex input.\n"
            f"{len(original)} bytes; SHA-256\n{original_hash}\n"
            "Used for hermetic public helper preflight, with no metadata reductions.\n"
        )
    else:
        provenance = root / "codex/testdata/prompted-ephemeral-models.provenance"
        if args.version == "0.165.0":
            provenance = root / "codex/testdata/prompted-ephemeral-models-0.165.provenance"
        provenance.write_text(
            f"Raw Codex {args.version} codex-rs/models-manager/models.json.\n" +
            ("Full catalog adds only gpt-6.1-terra to the retained 0.163/0.164 models.\n"
             if args.version == "0.165.0" else
             "Uses the unchanged canonical codex/testdata/ephemeral-models.json.\n") +
            f"{len(original)} bytes; SHA-256 {original_hash}\n"
            "Luna reuses canonical codex/ephemeral_luna_models.json.\n"
            f"{len(derived)} bytes; SHA-256 {derived_hash}\n"
            "Only tool_mode=direct, experimental_supported_tools=[] and\n"
            "use_responses_lite=false differ from upstream Luna metadata.\n"
            "Luna bytes match Codex 0.163.0; content does not prove native version\n"
            f"or tool isolation. Prompted naming supports raw {args.version};\n"
            "the consuming runtime verifies the exact executable.\n"
        )
    print(json.dumps({"version": args.version, "full_sha256": original_hash,
                      "luna_sha256": derived_hash}, indent=2))


if __name__ == "__main__":
    main()
