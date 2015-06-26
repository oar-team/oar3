#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os.path as op


from codecs import open

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def read(fname):
    ''' Return the file content. '''
    here = op.abspath(op.dirname(__file__))
    with open(op.join(here, fname), 'r', 'utf-8') as fd:
        return fd.read()

readme = read('README.rst')
history = read('HISTORY.rst').replace('.. :changelog:', '')

requirements = [
    'oar-lib',
    'flask>=0.10',
]

version = ''
version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(op.join('oar_rest_api', '__init__.py')),
                    re.MULTILINE).group(1)

if not version:
    raise RuntimeError('Cannot find version information')


setup(
    name='oar-rest-api',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=version,
    url='https://github.com/oar-team/python-oar-rest-api',
    packages=['oar_rest_api'],
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    description='Python OAR RESTful API',
    long_description=readme + '\n\n' + history,
    license="GNU GPL",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ],
)
