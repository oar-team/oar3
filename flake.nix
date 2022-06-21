{
  description = "nixos-compose";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:

    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        app = pkgs.poetry2nix.mkPoetryApplication {
          projectDir = ./.;
          propagatedBuildInputs = [ ];
        };

        packageName = "oar";
      in {
        packages.${packageName} = app;
        packages.documentation = pkgs.stdenv.mkDerivation {
          pname = "oar3-documentation";
          version = "1.0.1";
          src = ./.;
          buildInputs = with pkgs.python3Packages; [
            (pkgs.poetry2nix.mkPoetryEnv { projectDir = self; })
            sphinx
            sphinx_rtd_theme
          ];

          buildPhase = ''
            export PYTHONPATH=$PYTHONPATH:$PWD
            cd docs && make html
          '';

          installPhase = ''
            mkdir -p $out
            cp -r _build/html $out/
          '';
        };

        defaultPackage = self.packages.${system}.${packageName};

        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            (poetry2nix.mkPoetryEnv { projectDir = self; })
            python3Packages.sphinx_rtd_theme
            poetry
            postgresql
            pre-commit
          ];
        };
    });
}
