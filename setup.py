import re
import os.path as op
from setuptools import setup, find_packages

here = op.abspath(op.dirname(__file__))

requirements = [
    'oar-lib',
    'tabulate',
    'click',
]


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join(here, 'oar_cli', '__init__.py'))).group(1)


setup(
    name='oar-cli',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=get_version(),
    url='https://github.com/oar-team/oar-cli',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    description='OAR Command line interface.',
    long_description=read('README.rst') + '\n\n' + read('CHANGES.rst'),
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
    oar-database-migrate=oar_cli.db.commands.migrate:cli
    oar-database-archive=oar_cli.db.commands.archive:cli
    oarsub=oar_cli.oarsub:cli
    ''',
)
