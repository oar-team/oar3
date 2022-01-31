Mixing OAR3 and OAR2
====================

OAR2 and OAR3 are divided in different modules (see :ref:`modules_reference`).
Using the modularity, one can mix modules from OAR3 with modules from OAR2.
This can be useful, for instance, to do the migration from OAR2 to OAR3.

**This page explain how to install and use OAR3 scheduler and metascheduler on a working OAR2 server.**

.. note::

  Whereas section :ref:`target_metaschedulers_oar3_with_oar2` and section :ref:`target_schedulers_oar3_with_oar2`
  can be setup independently, it is also possible to use them together in the same OAR2 server.

Installation
------------

There is no official package for OAR3 yet, but it can be generated from sources.
The only required tool to generate the package is docker.

dependencies
^^^^^^^^^^^^

First install oar3's dependencies.

.. code-block:: bash

  apt-get update && \
  apt-get install -y python3 \
  python3-sqlalchemy python3-alembic \
  python3-click python3-flask \
  python3-passlib python3-psutil python3-requests \
  python3-simplejson python3-sqlalchemy-utils  \
  python3-tabulate python3-toml python3-yaml \
  python3-zmq python3-psycopg2 python3-fastapi


Install from pre-generated .deb
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The packages can be downloaded at : TODO.
Also download the debian package for `ProcSet <https://gitlab.inria.fr/bleuse/procset.py>`_ (which is an OAR3 dependency).

Generate the debian packages from sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

*Generate .deb*::

    # Clone oar3
    git clone git@github.com:oar-team/oar3.git

    # Generate packages
    cd misc/deb-gen && ./build-deb.sh

    # Get the python package python3-oar*.deb to your OAR server and install with
    dpkg -i python3-oar*.deb


.. warning::
  The procedure to generate the procset's debian package is similar to OAR3.
  The repository containing the package generation scripts is
  `here (branch debian) <https://gitlab.inria.fr/adfaure/procset.py/-/tree/debian>`_.

.. warning::
  For debian bullseye change the variable `BRANCHE_NAME`
  in oar3/mis/deb-gen/build-deb.sh for `bullseye/3.0`.

.. _target_schedulers_oar3_with_oar2:

Using OAR3 schedulers with OAR2
-------------------------------

This documentation describe how to install OAR3 scheduler (named kamelot) on a standard OAR2 installation.

Active scheduler for the default queue
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once installed the schedulers can be activated with the following manipulations.

*Activate OAR3 scheduler (named kamelot) for default queue*::

  # Link the scheduler to the folder expected by OAR2
  ln -s /usr/lib/oar/kamelot /usr/lib/oar/schedulers/kamelot

  # Delete the default queue
  oarnotify --remove-queue default

  # add kamelot as default queue scheduler
  oarnotify --add-queue default,3,kamelot


Kamelot configuration
^^^^^^^^^^^^^^^^^^^^^

In order for kamelot to work, the resources hierarchy of the
cluster must be provided in order to be used in resource request.
To do so, add the variable `HIERARCHY_LABELS` to OAR2 configuration.
Labels' order does not matter here.

*Example*::

    # Default value is "resource_id,network_address,cpu,core"
    HIERARCHY_LABELS="resource_id,network_address,cpu,core"


.. note::

  In case of issue with kamelot, It is possible to restore OAR2 scheduler with oarnotify.

  .. code-block:: bash

    oarnotify --remove-queue default
    oarnotify --add-queue default,3,<you-scheduler>


.. _target_metaschedulers_oar3_with_oar2:

Using OAR3 metascheduler with OAR2
----------------------------------

This section explain how to setup kao in OAR2.

The metascheduler should be available at `/usr/lib/oar/kao`.
Activating the kao for OAR2 requires to edit oar configuration (`/etc/oar/oar.conf`).

.. code-block:: bash

  # Change the metascheduler command
  META_SCHED_CMD="kao"
  # Configuration variable that tells kao to enable compatibility with OAR2
  METASCHEDULER_OAR3_WITH_OAR2="yes"


If the changes are applied to a running server, it might be necessary to restart OAR2.

.. code-block:: bash

  systemctl restart oar-server


.. note::

  To restore OAR2's metascheduler, set back the `META_SCHED_CMD` to "oar_meta_sched".
  and restart oar-server service `systemctl restart oar-server`.
