Create a new release
====================

Bump commit
-----------

Common step to create a release:

- Make sure the tests pass.
- The first step to create a release is to update the Changelog. The changelogs are written in the file CHANGES.rst at the project directory root, to make a release create a new entry in this file.
- Change the version name in oar/__init__.py and in pyproject.toml.
- Create a new commit with this changes, named :code:`Bump version to <version_number>`.

Release for Debian
------------------

The package definition for the debian packages are located in the branch :code:`debian/3.0.0`. This branch contains the source that will be packaged, and the definitions on the packages.
**When a new OAR3 version has been release, it is necessary to merge the debian branch with the release commit.**

Currently the branch :code:`debian/3.0.0.` contains the debian packages for debian bookworm (future stable) and the branch :code:`bullseye/3.0.0` contains the package for debian bullseye the current stable.

.. note::
  The bullseye branch has one commit to make OAR3 compatible with SQLAlchemy 1.3. Once the branch :code:`debian/3.0.0` has been updated, the branch :code:`bullseye/3.0.0` can be created by adding the aforementioned commit to the branch :code:`debian/3.0.0` (using :code:`git cherry-pick` for instance).

.. _Debian package generation:

Generate the debian packages from sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The package for debian can be generated with the scripts located in `misc/deb-gen`.
This entry script `./build-deb.sh` will pull the :code:`debian/3.0.0` branch and build the package in a docker container (so you can generate the package from another distribution).

*Generate .deb*::

    # Clone oar3
    git clone git@github.com:oar-team/oar3.git

    # Generate packages
    cd misc/deb-gen && ./build-deb.sh

    # Get the python package python3-oar*.deb to your OAR server and install with
    dpkg -i python3-oar*.deb

.. warning::
  For debian bullseye change the variable `BRANCHE_NAME`
  in oar3/mis/deb-gen/build-deb.sh for `bullseye/3.0`.

Automated package generation with github action
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The workflow defined in `.github/workflows/debian-generation.yml` is triggered when a new tag starting with 3 (e.g. 3.0.0.dev3) is pushed.
It will automatically create a github release, generate the debian packages and attach it to the `release page of github`_.

.. warning::
  Before pushing the tag, ensure that the branches containing the debian packages are up to date. Otherwise the generated packages will contain the old release.

.. _release page of github: https://github.com/oar-team/oar3/releases