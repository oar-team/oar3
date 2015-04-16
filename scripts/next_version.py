#!/usr/bin/env python
# coding: utf-8
from __future__ import unicode_literals, print_function
import argparse
import re
import fileinput
import os
import datetime
import subprocess


def do_next_release(part='patch'):
    """ Increment the version number to the next development version

    * (Configurably) bumps the development dev version number
    * Preloads the correct changelog template for editing

    You can run it like::

        $ python scripts/make_release.py

    which, by default, will create a 'patch' dev version (0.0.1 => 0.0.2-dev).

    You can also specify a patch level (patch, minor, major) to change to::

        $ python scripts/make_release.py major

    which will create a 'major' release (0.0.2 => 1.0.0-dev).

    """

    # Dry run 'bumpversion' to find out what the new version number
    # would be. Useful side effect: exits if the working directory is not
    # clean.

    bumpver = subprocess.check_output(
        ['bumpversion', part, '--dry-run', '--verbose'],
        stderr=subprocess.STDOUT)
    m = re.search(r'New version will be \'(\d+\.\d+\.\d+)\'', bumpver)
    version = m.groups(0)[0]

    date = datetime.date.today().isoformat()

    # Add the new section for this release we're doing
    # Using the 'fileinput' module to do inplace editing.

    for line in fileinput.input(files=['CHANGES'], inplace=1):
        # by default pass the lines through
        print(line, end="")
        # if we just passed through the '-----' line (after the header),
        # inject a new section for this new release

        if line.startswith('----'):
            ver_str = "{} ({})".format(version, date)
            separator = "".join(["+" for _ in ver_str])
            print("\n{}\n{}\n".format(ver_str, separator))
            print("* Fill notable features in here\n")

    # Tries to load the EDITOR environment variable, else falls back to vim
    editor = os.environ.get('EDITOR', 'vim')
    os.system("{} CHANGES".format(editor))

    # Have to add it so it will be part of the commit
    subprocess.check_output(['git', 'add', 'CHANGES'])
    subprocess.check_output(
        ['git', 'commit', '-m', 'Changelog for {}'.format(version)])

    # Really run bumpver to set the new release and tag
    bv_args = ['bumpversion', part]

    bv_args += ['--new-version', version]

    subprocess.check_output(bv_args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=do_release.__doc__,
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("part",
                        default="patch",
                        choices=["patch", "minor", "major"],
                        help="Specify the new version level "
                             "(default: %(default)s)")

    args = parser.parse_args()
    # do_release(part=args.part)
