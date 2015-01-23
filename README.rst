
Kao: a Scheduling Framework for OAR
====================================

Goal
----
  Provide meta-scheduler and scheduler alernatives to OAR

Main features
--------------

- Python as script language
- Same data structures of Kamelot
    - intervals to represent resource sets
    - available resources are timely arranged through contiugous slots.
- Other choices (high level programs' structures) follow those from the Perl version.
- The first version will be without DB interaction (it'll be just a demonstrator) also
1rst scheduler will not support :
    - dependencies, fairsharing, timesharing, placeholder, quotas, resource always added,
and will support :
    - hierarchy, container


Installation
------------

Install from source on git repos
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kao depends of a standard oar's server proper installation and python-oar-lib module. For the first follow the standard
documentation. For the second follow the instructions below:

  git clone https://gforge.inria.fr/git/oar/oar-docker.git
  cd python-oar-lib
  sudo python setup.py install

To install Kao:

  git clone https://gforge.inria.fr/git/oar/oar-kao.git
  cd kao
  sudo python setup.py install

After you need to configure a queue use kamelot  



Install within oar-docker
~~~~~~~~~~~~~~~~~~~~~~~~~

Simulation Mode
---------------
Kao provides an a simple integrated simulator called *Simsim*. Its first purpose is the simulation of scheduler attached to one queue, 
in other words it mights simulate only one Kamelot instance (one queue).

  
