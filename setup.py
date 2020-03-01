#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path as op
import re
from codecs import open

from setuptools import find_packages, setup


def read(fname):
    """ Return the file content. """
    here = op.abspath(op.dirname(__file__))
    with open(op.join(here, fname), "r", "utf-8") as fd:
        return fd.read()


def get_requirements(basename):
    return read("requirements/{}.txt".format(basename)).strip().split("\n")


readme = read("README.rst")
changelog = read("CHANGES.rst").replace(".. :changelog:", "")

version = ""
version = re.search(
    r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    read(op.join("oar", "__init__.py")),
    re.MULTILINE,
).group(1)

if not version:
    raise RuntimeError("Cannot find version information")


extras_require = {
    key: get_requirements(key) for key in ["full", "coorm", "dev", "test"]
}

setup(
    name="oar3",
    author="Olivier Richard, Salem Harrache",
    author_email="oar-devel@lists.gforge.inria.fr",
    version=version,
    url="https://github.com/oar-team/oar3",
    packages=find_packages(),
    package_dir={"oar": "oar"},
    package_data={"oar": ["tools/*.pl", "tools/*.pm", "tools/*.sh", "tools/oarexec"]},
    install_requires=get_requirements("base"),
    extras_require=extras_require,
    include_package_data=True,
    zip_safe=False,
    description="OAR next generation",
    long_description=readme + "\n\n" + changelog,
    keywords="oar3",
    license="BSD",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Clustering",
    ],
    entry_points="""
    [console_scripts]
    oar3-database-migrate=oar.cli.db.commands.migrate:cli
    oar3-database-archive=oar.cli.db.commands.archive:cli
    oar3-database-manage=oar.cli.db.commands.manage:cli
    oar3-almighty=oar.modules.almighty:main
    oar3-bipbip-commander=oar.modules.bipbip_commander:main
    oar3-appendice-proxy=oar.modules.appendice_proxy:main
    oar3-hulot=oar.modules.hulot:main
    oarsub3=oar.cli.oarsub:cli
    oarstat3=oar.cli.oarstat:cli
    oardel3=oar.cli.oardel:cli
    oarhold3=oar.cli.oarhold:cli
    oarresume3=oar.cli.oarresume:cli
    oarnodes3=oar.cli.oarnodes:cli
    oarnotify3=oar.cli.oarnotify:cli
    oarqueue3=oar.cli.oarqueue:cli
    oarconnect3=oar.cli.oarconnect:cli
    oarremoveresource3=oar.cli.oarremoveresource:cli
    oarnodesetting3=oar.cli.oarnodesetting:cli
    oaraccounting3=oar.cli.oaraccounting:cli
    oarproperty3=oar.cli.oarproperty:cli
    oar2trace=oar.cli.oar2trace:cli
    kao=oar.kao.kao:main
    kamelot=oar.kao.kamelot:main
    kamelot-fifo=oar.kao.kamelot_fifo:main
    bataar=oar.kao.bataar:bataar
    oar-batsim-sched-proxy=oar.kao.batsim_sched_proxy:cli
    oar3-sarko=oar.modules.sarko:main
    oar3-finaud=oar.modules.finaud:main
    oar3-leon=oar.modules.leon:main
    oar3-node-change-state=oar.modules.node_change_state:main
    oar3-bipbip=oar.modules.bipbip:main
    """,
)
