#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import re
import os.path as op


from codecs import open
from setuptools import setup, find_packages


def read(fname):
    ''' Return the file content. '''
    here = op.abspath(op.dirname(__file__))
    with open(op.join(here, fname), 'r', 'utf-8') as fd:
        return fd.read()

readme = read('README.rst')
changelog = read('CHANGES.rst').replace('.. :changelog:', '')

requirements = [
    'sqlalchemy',
    'sqlalchemy_utils',
    'alembic',
    'flask>=0.10',
    'tabulate',
    'click',
    'simpy',
    'pyzmq',
    'redis',
    'requests',
    'procset',
    'pybatsim',
    'simplejson',
    'psutil',
]

version = ''
version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(op.join('oar', '__init__.py')),
                    re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


setup(
    name='oar3',
    author="Olivier Richard, Salem Harrache",
    author_email='oar-devel@lists.gforge.inria.fr',
    version=version,
    url='https://github.com/oar-team/oar3',
    packages=find_packages(),
    package_dir={'oar': 'oar'},
    package_data={'oar': ['scripts/*.pl', 'scripts/*.pm', 'scripts/*.sh', 'scripts/oarexec']},
    install_requires=requirements,
    extras_require={
        'coorm': ['zerorpc', 'requests'],
        'dev': ['zerorpc', 'requests', 'pytest', 'pytest-flask', 'pytest-cov', 'sphinx']
   },
    include_package_data=True,
    zip_safe=False,
    description="OAR next generation",
    long_description=readme + '\n\n' + changelog,
    keywords='oar3',
    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ],
    entry_points='''
    [console_scripts]
    oar3-database-migrate=oar.cli.db.commands.migrate:cli
    oar3-database-archive=oar.cli.db.commands.archive:cli
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
    oarremoveresource3=oar.cli.oarremoveresource:cli
    oarnodesetting3=oar.cli.oarnodesetting:cli
    oaraccounting3=oar.cli.oaraccounting:cli
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
    ''',
)
