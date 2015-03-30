import re
import os.path as op
from setuptools import setup

here = op.abspath(op.dirname(__file__))

requirements = [
    'sqlalchemy',
]

dependency_links = []


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join(here, 'oar_lib', '__init__.py'))).group(1)

setup(
    name='oar-lib',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=get_version(),
    url='https://github.com/oar-team/python-oar-lib',
    packages=['oar', 'oar_lib'],
    install_requires=requirements,
    dependency_links=dependency_links,
    include_package_data=True,
    zip_safe=False,
    description='Interact with OAR in Python',
    long_description=read('README.rst') + '\n\n' + read('CHANGES'),
    license="GNU GPL v2",
    classifiers=[
        'Development Status :: 1 - Planning',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)',  # noqa
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
    ]
)
