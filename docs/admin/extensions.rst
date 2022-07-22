Extensions
==========

Overview
--------

OAR enables to customize or extend some part of its logic to customize the scheduling, without the need to directly modify the code of OAR.
Instead, the extensions can be provided as an external python package that needs to be installed as any other python project.

The extensions use the python `entry point <https://packaging.python.org/en/latest/specifications/entry-points/>`_
mechanism that make it possible to register and discover cross-project code.
The extensions registers functions in the namespace `oar`, so that oar can retrieve them.

Internally, OAR adds some function invocations at defined place, or override existing scheduling functions.
The different functions that can be override using the extension mechanism are:
- Add an extra function at the beginning of the meta-scheduler loop processing each queues.
- Customize the allocation of the resources to jobs (assign, and find functions).
- Customize the job order used by the scheduler.

How to use oar extension
------------------------

Install
~~~~~~~

Installation of a plugin works by installing the plugin in the same python installation as OAR.

Write and test your extension (with poetry)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OAR has an official extension `repository <https://github.com/oar-team/oar3-plugins>`_; if you want to check out how it works, or start your own extension repository you can start forking the OAR plugins repository.
It contains examples for each customizable functions, and non-regression tests.

The oar version your extensions depends on can be specified in the `pyproject.toml` file, with the following line::

        oar = { git = "https://github.com/oar-team/oar3", branch = "plugins" }


Or with the command line::

        poetry add git+https://github.com/oar-team/oar3.git#master


The easiest way to run the tests is using directly pytest. Keep it mind that to speed up the tests, it uses an sqlite database, which is not suitable for production.

*Run the tests with sqlite (faster)*::

        poetry install
        poetry shell
        pytest tests

Docker files are available to run the tests with postgres to use it run the command::

        # Install docker and docker-compose first
        ./scripts/ci/run-tests-with-docker.sh


Features
--------

Functions assign and find
~~~~~~~~~~~~~~~~~~~~~~~~~

Job sorting
~~~~~~~~~~~

Extra meta_metasched
~~~~~~~~~~~~~~~~~~~~

