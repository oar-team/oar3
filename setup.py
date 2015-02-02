import re
import os.path as op
from setuptools import setup

here = op.abspath(op.dirname(__file__))

requirements = [
    'oar-lib>=0.1-dev',
    'flask>=0.10',
]


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join(here, 'oar_rest_api', '__init__.py'))).group(1)


setup(
    name='oar-rest-api',
    author='Salem Harrache',
    author_email='salem.harrache@inria.fr',
    version=get_version(),
    url='https://github.com/oar-team/python-oar-rest-api',
    packages=['oar_rest_api'],
    install_requires=requirements,
    include_package_data=True,
    zip_safe=False,
    description='Python OAR RESTful API',
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
)
