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
    'six',
]

if sys.version_info[0] == 2:
    requirements.append('subprocess32')

version = ''
version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(op.join('oar', '__init__.py')),
                    re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


setup(
    name='oar3',
    author="Salem Harrache",
    author_email='salem.harrache@inria.fr',
    version=version,
    url='https://github.com/oar-team/oar3',
    packages=find_packages(),
    install_requires=requirements,
    extras_require={
        'coorm': ['zerorpc', 'requests'],
        'dev': ['zeropc', 'resuests']
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ],
    entry_points='''
    [console_scripts]
    oar-database-migrate=oar.cli.db.commands.migrate:cli
    oar-database-archive=oar.cli.db.commands.archive:cli
    oarsub3=oar.cli.oarsub:cli
    kao=oar.kao.kao:main
    kamelot=oar.kao.kamelot:main
    kamelot_fifo=oar.kao.kamelot_fifo:main
    bataar=oar.kao.bataar:bataar
    ''',
)
