#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os.path as op


from codecs import open
from setuptools import setup


def read(fname):
    ''' Return the file content. '''
    here = op.abspath(op.dirname(__file__))
    with open(op.join(here, fname), 'r', 'utf-8') as fd:
        return fd.read()

readme = read('README.rst')
history = read('CHANGES.rst').replace('.. :changelog:', '')

requirements = [
    'sqlalchemy',
    'alembic',
]

version = ''
version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(op.join('oar_lib', '__init__.py')),
                    re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


setup(
    name='oar-lib',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=version,
    description='Python version of OAR Common Library',
    long_description=readme + '\n\n' + history,
    keywords='oar-lib',
    url='https://github.com/oar-team/python-oar-lib',
    packages=['oar', 'oar_lib'],
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ]
)
