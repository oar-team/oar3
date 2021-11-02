{
  description = "nixos-compose";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs";
    flake-utils.url = "github:numtide/flake-utils";
    poetry2nix.url = "github:nix-community/poetry2nix";
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix}:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

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
          (pkgs.poetry2nix.mkPoetryEnv {
            projectDir = self;
          })
            poetry
            postgresql
          ];
      };
    });
}
