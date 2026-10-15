{
  description = "oar";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    flake-utils.url = "github:numtide/flake-utils";
    kapack.url = "git+https://gricad-gitlab.univ-grenoble-alpes.fr/kairns/kapack.git?ref=master";
    kapack.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { nixpkgs, flake-utils, kapack, ... }:

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
                pyjwt
                passlib
                bcrypt
            ];
        };
        packageName = "oar";
      in {
          #pythonEnv = pkgs.python3.withPackages(ps: [ ]);
        packages.${packageName} = app;
        packages.default = app;
        packages.documentation = pkgs.stdenv.mkDerivation {
          pname = "oar3-documentation";
          version = "1.0.1";
          src = ./.;
          buildInputs = with pkgs.python3Packages; [
            app
            sphinx
            sphinx-rtd-theme
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

        devShells = {
            old = pkgs.mkShell {
                LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
                buildInputs = with pkgs; [
                    app
                    # Install the entry point and the plugins
                    # Which is not needed anymore bc the plugins are on a new repo
                    # (poetry2nix.mkPoetryApplication { projectDir = self; })
                    python3Packages.sphinx-rtd-theme
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
                    pyjwt
                    kapackpkgs.procset
                    rich
                    pexpect
                    simpy
                    redis
                    clustershell
                    httpx
                    matplotlib

                    # Dev dependencies
                    isort
                    flake8
                    black

                    # Docs
                    sphinx
                    sphinx-rtd-theme
                    flake8
                ];
            in
              pkgs.mkShell {
                  packages = with pkgs; [ pre-commit ] ++ pythonEnv;
              };
            with-oar = pkgs.mkShell {
                packages = [ app ];
            };
            format = pkgs.mkShell {
                packages = with pkgs.python3Packages; [
                    isort
                    black
                    flake8
                ];
            };
        };
    });
}
