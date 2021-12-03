{
  description = "nixos-compose";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:

    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        #customOverrides = self: super: {
        # Overrides go here
        #};

        app = pkgs.poetry2nix.mkPoetryApplication {
          projectDir = ./.;
          #overrides =
          #  [ pkgs.poetry2nix.defaultPoetryOverrides customOverrides ];
          propagatedBuildInputs = [ ];
        };

        packageName = "oar";
      in {
        packages.${packageName} = app;

        defaultPackage = self.packages.${system}.${packageName};

        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            (poetry2nix.mkPoetryEnv { projectDir = self; })
            poetry
            postgresql
            pre-commit
          ];
        };
    });
}
