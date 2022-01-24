Install OAR3 schedulers for OAR2
================================

Prerequisites
-------------

This documentation describe how to install OAR3 schedulers on a standard OAR2 installation.

Installation
------------

The only package needed for the scheduler is python3-oar.

Install from pre-generated .deb
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The packages can be downloaded at : TODO.
Also download the debian package for `ProcSet <https://gitlab.inria.fr/bleuse/procset.py>`_ (which is an OAR3 dependency).

Generate the debian packages from sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Configuration
-------------

Active scheduler for the default queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once installed the schedulers can be activated with the following manipulations.

*Activate OAR3 scheduler (named kamelot) for default queue*::

  # Link the scheduler to the folder expected by OAR2
  ln -s /usr/lib/oar/kamelot /usr/lib/oar/schedulers/kamelot

  # Delete the default queue
  oarnotify --remove-queue default

  # add kamelot as default queue scheduler
  oarnotify --add-queue default,3,kamelot


Kamelot configuration
~~~~~~~~~~~~~~~~~~~~~

In order for kamelot to work, the resources hierarchy of the
cluster must be provided in order to be used in resource request.
To do so, add the variable `HIERARCHY_LABELS` to OAR2 configuration.
Labels' order does not matter here.

*Example*::

    # Default value is "resource_id,network_address,cpu,core"
    HIERARCHY_LABELS="resource_id,network_address,cpu,core"

