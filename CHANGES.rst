.. :changelog:

.. _ref-dev-changelog:

Changelog
=========

Version 3.0.0.dev12
-------------------

Released on December 22, 2023

- Add types of jobs to oarstat details (cli or api)
- Fix unpacking init_oar into Finaud
- New Phoenix version (python rewrite and variables now into oar.conf)
- Admission rules errors return explicit 403 into API
- Misc code enhancements (typing, tests, global logger)
- Replaced REST API apache config by uvicorn + reverse proxy

Version 3.0.0.dev11
-------------------

Released on November 28, 2023

- Add taktuk to the package list in the doc
- Add oarstat and oarnodes have `yaml` output
- Remove SQLAlchemy2 warning in oarnodesetting
- Remove call unwanted call to `strace`
- Change oarporperty check from `isalpha` to `isascii`
- Change Hulot renamed Greta
- Fix oarnodesetting wrong error code
- Fix Almighty check_Greta always return true
- Fix oarsub -T displays unnecessary log
- Cosmetic,  message `a failed admission rule prevented submitting the job` before custom message of the rule, and including the `# Admission rule` tag.


Version 3.0.0.dev10
-------------------

Released on November 8, 2023

Version 3.0.0.dev9
------------------

Released on November 7, 2023

- Rework SlotSets
  - Fixing numerous bug
  - New programming interface for using SlotSets
- Upgrade to SQLAlchemy2
- Rework quotas to match the definition of OAR2

Version 3.0.0.dev8
------------------

Released on June 7, 2023

- (Huge) Database refactoring
  - Remove the global database definition
  - Remove the custom lazy loading module
- New cli output with rich
- Fix bug with resource hierarchy
- Numerous fixes and improvements

Version 3.0.0.dev7
------------------

- Fix save_assigns failed because of bad sqlalchemy function use

Version 3.0.0.dev6
------------------

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
