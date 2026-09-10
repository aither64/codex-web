{
  description = "Reusable Codex App Server web integration";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      python = pkgs.python3.withPackages (pythonPackages: [ pythonPackages.jsonschema ]);
      checks = pkgs.buildGoModule {
        pname = "codex-web-checks";
        version = "0.0.0";
        src = ./.;
        vendorHash = "sha256-BmrFvNSP3wbz/FsAOTJCP+bj81jgUlveu2N4xz5Os94=";
        subPackages = [ ];
        nativeCheckInputs = [
          pkgs.nodejs
          python
        ];
        checkPhase = ''
          runHook preCheck
          ${python}/bin/python3 test/codex_protocol_contract.py \
            --coverage-only codex/client.go
          go test ./...
          ${pkgs.nodejs}/bin/node --check conversation/assets/conversation.js
          ${pkgs.nodejs}/bin/node --test test/conversation_browser_contract_test.cjs
          runHook postCheck
        '';
        installPhase = ''
          mkdir -p "$out"
          touch "$out/passed"
        '';
      };
    in
    {
      checks.${system} = {
        default = checks;
        generic-source = pkgs.runCommand "codex-web-generic-source" { } ''
          first=vps
          second=aither
          forbidden="$first"'free|'"$second"'dev'
          if grep -RilE "$forbidden" ${./.} --exclude-dir=.git > matches; then
            cat matches >&2
            exit 1
          fi
          if ${pkgs.findutils}/bin/find ${./.} -printf '%P\n' | grep -iE "$forbidden" > matches; then
            cat matches >&2
            exit 1
          fi
          touch "$out"
        '';
      };
      packages.${system}.default = checks;
    };
}
