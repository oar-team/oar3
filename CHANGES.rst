.. :changelog:

.. _ref-dev-changelog:

Changelog
=========

Unreleased
----------

- Fix karma_proj_targets needed to be cast as float
- Add each scheduling loop update the message field of jobs to add information about project, number of resources, walltime, karma etc

Version 3.0.0.dev5
------------------

Released on Mar 4, 2022

- Fix and cleaning
- Add oarbench for scheduling evaluation
- Add metasched config to communicate with OAR2
- Add back ported commit from OAR2
- Add poetry packaging
- Add new API version with FastAPI
- Add job_resources_manager_cgroups_nixos
- Add temporal quotas
- Add oarwalltime


Version 3.0.0.dev4
------------------

Released on Mar 9, 2020

- Add oarqueue, oarnotify, oarconnect and oarprint CLIs
- Modify and clean installtion process (setup.py and Makefiles)
- Use docker to test on travis-ci
- Add script helper to generate debian package
- Add job resource cgroup manager for NixOS
- Add NIX package to nur-kapack project
- Fix oar2trace
- Complete and fix array job
- Add factor script and Rest API entry


Version 3.0.0.dev3
------------------

Released on Nov 12, 2018

- Add admission rules
- Bataar (Batsim's adaptor)
- Various bug fixes
- Add many unitary tests 
- Add accounting
- Rest API (incomplete version)
- Makefiles
- Installation without need of OAR2 installation
- Manpages (from OAR2)
- Remove use of judas_notify_user.pl
- Add pingchecker
- Remove ruby version of DrawGantt  

Version 3.0.0.dev2
------------------

Released on Apr 2, 2018

- Minor progresses of previous dev version  

Version 3.0.0.dev1
------------------

Released on Mar 29th 2018

- Pre-alpha (or Demo) version 
- All core features are written
- Scheduling (core part is completed)
- Need OAR2 installation procedure to function
- Usable with oardocker
- Incomplete an missing CLIs, few options available
- Nodes energy saving unfinished
- Pingchecker unavailable


Version 3.0.0.dev0
------------------

**unreleased**

- First release on PyPI (obselete oar-lib).
