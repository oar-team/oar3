===================================
Kao: A Scheduling Framework for OAR
===================================

.. image:: https://img.shields.io/travis/oar-team/oar-kao.svg
        :target: https://travis-ci.org/oar-team/oar-kao

.. image:: https://img.shields.io/pypi/v/oar-kao.svg
        :target: https://pypi.python.org/pypi/oar-kao

.. image:: http://codecov.io/github/oar-team/oar-kao/coverage.svg?branch=master
        :target: http://codecov.io/github/oar-team/oar-kao?branch=master


Another Metascheduler for OAR.

* Free software: BSD license
* Documentation: https://oar-kao.readthedocs.org.


Goal
----

Yet another meta-scheduler and scheduler for OAR.

Main features
--------------

- Use Python as scripting language
- Same data structures as Kamelot
    - intervals to represent resource sets
    - available resources are timely arranged through contiguous slots.
- Other choices (high level programs' structures) follow those of the Perl version.

Installation
------------

Kao depends of a standard oar's server proper installation and python oar-lib
module. For the first follow the standard documentation. For the second follow
the instructions below:

*Supports Python 2.7 and 3.4+.*

.. code:: bash

    $ pip install [--user] oar-kao
    $ pip install [--user] --upgrade oar-kao
    $ pip uninstall oar-kao

Or from git (last development version)

.. code:: bash

    $ pip install [--user] git+https://github.com/oar-team/oar-kao.git

Or if you already pulled the sources

.. code:: bash

    $ git clone https://github.com/oar-team/oar-kao.git
    $ python oar-kao/setup.py install # or pip install ./oar-kao

Or if you don't have pip

.. code:: bash

    $ easy_install oar-kao

After you need to configure a queue use kamelot

Install within oar-docker
~~~~~~~~~~~~~~~~~~~~~~~~~

Simulation Mode
---------------
Kao provides an a simple integrated simulator called *Simsim*. Its first purpose is the simulation of scheduler attached to one queue,
in other words it mights simulate only one Kamelot instance (one queue).
