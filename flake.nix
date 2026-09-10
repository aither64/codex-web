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
        vendorHash = "sha256-05Qdy/y6o9aWMF7/1WcQwD4sTUhLeZXNVY4EJxUxgFk=";
        subPackages = [ "codex" ];
        nativeCheckInputs = [
          pkgs.nodejs
          python
        ];
        checkPhase = ''
          runHook preCheck
          ${python}/bin/python3 test/codex_protocol_contract.py \
            --coverage-only codex/client.go
          go test ./codex
          ${pkgs.nodejs}/bin/node --check portal/internal/web/static/app.js
          ${pkgs.nodejs}/bin/node portal/internal/web/browser_contract_test.cjs --unit
          runHook postCheck
        '';
        installPhase = ''
          mkdir -p "$out"
          touch "$out/passed"
        '';
      };
    in
    {
      checks.${system}.default = checks;
      packages.${system}.default = checks;
    };
}
