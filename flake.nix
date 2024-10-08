{
  description = "oar";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.11";
    flake-utils.url = "github:numtide/flake-utils";
    kapack.url = "github:oar-team/nur-kapack?ref=23.05";
    kapack.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, kapack }:

    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        kapackpkgs = kapack.packages.${system};
        app = pkgs.python3Packages.buildPythonPackage {
            pname = "oar";
            version = "3.0.0";
            format = "pyproject";
            src = ./.;
            nativeBuildInputs = with pkgs;  [ poetry python3Packages.poetry-core ];
            propagatedBuildInputs = with pkgs.python3Packages; [
                poetry-core
                pyzmq
                requests
                alembic
                kapackpkgs.procset
                click
                simplejson
                flask
                tabulate
                psutil
                sqlalchemy-utils
                psycopg2
                passlib
                escapism
                toml
                fastapi
                uvicorn
                pyyaml
                ptpython
                python-multipart
                importlib-metadata
                clustershell
                rich
                httpx
                python-jose
                passlib
                bcrypt
            ];
        };
        packageName = "oar";
      in {
          #pythonEnv = pkgs.python3.withPackages(ps: [ ]);
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

        devShells = {
            old = pkgs.mkShell {
                LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
                buildInputs = with pkgs; [
                    (poetry2nix.mkPoetryEnv { projectDir = self; })
                    # Install the entry point and the plugins
                    # Which is not needed anymore bc the plugins are on a new repo
                    # (poetry2nix.mkPoetryApplication { projectDir = self; })
                    python3Packages.sphinx_rtd_theme
                    poetry
                    postgresql
                    pre-commit
                ];
            };
            default = let
                pythonEnv = with pkgs.python3Packages; [
                    pytest
                    pyzmq
                    requests
                    alembic
                    click
                    simplejson
                    flask
                    tabulate
                    psutil
                    sqlalchemy-utils
                    psycopg2
                    passlib
                    escapism
                    toml
                    fastapi
                    uvicorn
                    pyyaml
                    ptpython
                    python-multipart
                    python-jose
                    kapackpkgs.procset
                    rich
                    pexpect
                    simpy
                    redis
                    clustershell

                    # Dev dependencies
                    isort
                    flake8

                    # Docs
                    sphinx
                    sphinx-rtd-theme
                    flake8
                ];
            in
              pkgs.mkShell {
                  packages = with pkgs; [ pre-commit ] ++ pythonEnv;
              };

        };

    });
}
