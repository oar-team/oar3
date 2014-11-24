import re
import os.path as op
from setuptools import setup, find_packages

here = op.abspath(op.dirname(__file__))


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as f:
        return f.read()


v_file = op.join(here, 'lib', 'oar', '__init__.py')
VERSION = re.compile(r".*__version__ = '(.*?)'",
                     re.S).match(read(v_file)).group(1)


setup(
    name='oar-lib',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=VERSION
    ,
    url='https://github.com/oar-team/python-oar-common',
    install_requires=[
        'sqlalchemy',
    ],
    packages=find_packages('lib'),
    package_dir={'': 'lib'},
    include_package_data=True,
    zip_safe=False,
    description='Interact with OAR in Python',
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
    ]
)
