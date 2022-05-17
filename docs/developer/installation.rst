Installation
============


Overview
--------

There are currently 3 methods to install OAR:

  - from the Debian packages (TBD)
  - from the Nix/Nixos packages  (TBD)
  - from sources  (TBD)


Alernatively, OAR can by evaluated through 2 ways:
  - oar-docker (TBD)
  - arion-oar (TBD)


Before going further, please have in mind OAR's architecture. A common OAR
installation is composed of:

  - a **server** which will hold all of OAR "smartness". That host will run
    the OAR server daemon;
  - one or more **frontends**, which users will have to login to, in order
    to reserve computing nodes (oarsub, oarstat, oarnodes, ...);
  - **computing nodes** (or basically *nodes*), where the jobs will execute;
  - optionally a **visualisation server** which will host the
    visualisation webapps (monika, drawgantt, ...);
  - optionally an **API server**, which will host OAR restful API service.

    
Many OAR data are stored and archived in a **PostgreSQL** database.


Computing nodes
---------------

Installation from the Debian packages
_____________________________________

**Instructions**

*For the Debian like systems*::

For now, only OAR's serie 2 versions is shipped as part of Debian official distributions. Version 3 is (will be) available at  http://oar.imag.fr/download#debian

.. code:: bash

          $ wget  http://oar.imag.fr/
          
