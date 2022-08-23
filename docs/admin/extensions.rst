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

Install an extension
~~~~~~~~~~~~~~~~~~~~

Installation of an extension works by installing the extension in the same python installation as OAR.

Create your extension (with poetry)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OAR has an official extension `repository <https://github.com/oar-team/oar3-plugins>`_; if you want to check out how it works, or start your own extension repository you can start forking the OAR plugins repository.
It contains examples for each customizable functions, and non-regression tests.

The oar version your extensions depends on can be specified in the `pyproject.toml` file, with the following line::

        oar = { git = "https://github.com/oar-team/oar3", branch = "master" }


Or with the command line::

        poetry add git+https://github.com/oar-team/oar3.git#master


Create the entry points
~~~~~~~~~~~~~~~~~~~~~~~

OAR uses four different entry points that can be used to expose customized functions. Each entry point is associated with a different purpose.
With poetry, the entry point is specified in the file `pyproject.toml` as is:


.. code-block:: toml
        :caption: pyproject.toml entry point example. Mutliple functions can be exposed under the same group name (in the example case, the group name is "oar.extra_metasched_func").

        [tool.poetry.plugins."oar.extra_metasched_func"]
        # Define a function named default
        default = "src.extra_metasched:extra_metasched_default"
        # And another funcdtion named foo
        foo = "src.extra_metasched:extra_metasched_foo"

The function named ``extra_metasched_default`` located in ``src/extra_metasched.py`` of your plugin repository can be accessed by OAR under the name ``default`` of the ``oar.extra_metasched_func`` entry point group name.

OAR defines four entry points:

- ``oar.extra_metasched_func``
- ``oar.assign_func``
- ``oar.find_func``
- ``oar.jobs_sorting_func``

Run the tests
~~~~~~~~~~~~~

Once you have your repository extension up and working, you can write and run tests (with the database).

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

Both function are executed *per* jobs during a scheduling loop. These functions can be used to tune the scheduling and modify how resources are allocated.

The assign function (``oar.assign_func``) is supposed to, according to a job request and a :class:`oar.kao.slot.SlotSet`, find the time slot for the job.
That is to say, find a suitable time slot for which all necessary resources for the job are fulfilled.
Without extension, the default behavior of OAR, is to call the function :func:`oar.kao.scheduling.assign_resources_mld_job_split_slots`.

The find function (``oar.find_func``) is supposed to, given a resources hierarchy and a job request, allocates the resources need by the job.
Under the hood, the ``assign_func`` calls the ``find_func`` on every slot of the slot set.
Without extension, the default behavior of OAR, is to call the function :func:`oar.kao.scheduling.find_resource_hierarchies_job`.

Job sorting
~~~~~~~~~~~

This function can be used to customize the jobs priority by tuning the order by which the jobs are precessed by the scheduler.

Extra meta_metasched
~~~~~~~~~~~~~~~~~~~~

This function is called by the meta scheduler (:func:`oar.kao.meta_sched.meta_schedule`) at each scheduling loop.
Note, that it is called once for each different priority level of queues (i.e if two queues have the same priority it will be called once for both queue).