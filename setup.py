import re
import os.path as op
from setuptools import setup

here = op.abspath(op.dirname(__file__))

requirements = [
    'oar-lib',
]

dependency_links = [
    'git+https://github.com/oar-team/python-oar-lib.git#egg=oar-lib',
]


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join(here, 'oar_utils', '__init__.py'))).group(1)


setup(
    name='oar-utils',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=get_version(),
    url='https://github.com/oar-team/python-oar-utils',
    packages=['oar_utils'],
    install_requires=requirements,
    dependency_links=dependency_links,
    include_package_data=True,
    zip_safe=False,
    description='Custom scripts and various utility functions for OAR.',
    long_description=read('README.rst') + '\n\n' + read('CHANGELOG.rst'),
    license="GNU GPL",
    classifiers=[
        'Development Status :: 1 - Planning',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ],
    entry_points='''
    [console_scripts]
    oar-database-archive=oar_utils.archive:main
    ''',
)
