.. _dev-install:

Development environment
=======================

.. note::

  This guide details how to install and run OAR for developers.
  If you want to install OAR on a cluster, please follow :ref:`the administration installation guide<admin-install>`.

Overview
--------

This guide explain different strategy to develop a new feature or fix a bug in OAR.
The distributed nature of OAR makes it difficult to execute it a single computer. Therefore, testing ongoing development is not always easy.
There is (at least) two ways of developing with OAR locally.

- The first method is to run the OAR tests locally. It is the recommended way but not always sufficient. Once the tests (successfully) run on your machine, you can start developing in OAR by first adding a new test corresponding to your feature (or bug).
- Sometimes, it is required to test OAR directly, it is possible to run a small containerized cluster on your computer with `oar-docker-compose` (which is based on `docker-compose`). To start with `oar-docker-compose`, you can directly follow the `oar-docker-compose readme <https://github.com/oar-team/oar-docker-compose>`_.

**The Continuous Integration (CI) will check that every tests pass, we will not accept a merge request with a failing CI, so make sure to run the tests before asking to merge your changes**.

Install and run the tests
-------------------------

There are currently 3 methods to install OAR locally:

  - from source with :ref:`nix <nix-install>`.
  - from sources with :ref:`poetry <poetry-install>`.
  - use the CI scripts :ref:`ci <ci-install>`.

.. _poetry-install:

Install from source with poetry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Dependencies*
  - `poetry <https://python-poetry.org/docs/#installation>`_
  - `postgresql` and `postgresql-client`, and `libpq` (headers file to link c programs)
  - The sources of `OAR <https://github.com/oar-team/oar3>`_

Once you have all the requirements installed, you can run the commands:

1. First use poetry to install the dependencies: ``poetry install``.
2. Then enter a poetry shell with the installed packages accessible: ``poetry shell``.
3. Finally, run the tests with ``pytest tests``.


.. _nix-install:

Install from source with nix
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*Dependencies*
    - `nix <https://nixos.org/download.html>`_
    - The sources of `OAR <https://github.com/oar-team/oar3>`_

Once you have all the requirements installed, you can run the commands:
1. Use the nix command ``nix develop`` to simultaneously install all the dependencies and enter a new shell to use them.
2. Run the tests with `pytests`: ``pytest tests``.

.. _ci-install:


Using CI scripts
~~~~~~~~~~~~~~~~

One last way of running the tests is to use the CI scripts. This is useful to reproduce the CI behavior when it has break or before pushing.
The advantage of this method is that it runs a postgresql database inside a docker container (the local tests use sqlite).
Also, it is a quick way to run the tests as it only requires `docker` and `docker-compose`.
On the other hand, it makes the test longer and it is difficult to debug the python as everything runs inside a docker container.

The command to execute the tests is `./scripts/ci/run-tests-with-docker.sh`.
It is also possible to select tests in the same way it is done with `pytest`: `./scripts/ci/run-tests-with-docker.sh tests/lib`.


pre-commit hook
---------------

We use pre-commit to check that the staged changes are well formatted.
Don't forget to install the pre-commit dependencies with ``pre-commit install``.

