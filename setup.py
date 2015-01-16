import os.path as op
import re
from setuptools import setup

requirements = [
    'oar-lib',
    'Click',
    'SimPy',
]

dependency_links = [
    'git+http://github.com/oar-team/python-oar-lib.git#egg=oar-lib',
]

here = op.abspath(op.dirname(__file__))

def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as fd:
        return fd.read()


def get_version():
    return re.compile(r".*__version__ = '(.*?)'", re.S)\
             .match(read(op.join(here, 'oar_kao', '__init__.py'))).group(1)


setup(
    name='kao',
    author='Olivier Richard',
    author_email='olivier.richard@imag.fr',
    version=get_version(),
    url='https://github.com/oar-team/kao',
    install_requires=requirements,
    dependency_links=dependency_links,
    packages=['oar_kao'],
    include_package_data=True,
    zip_safe=False,
    description='Another Metascheduler for OAR.',
    long_description=read('README.rst') + '\n\n' + read('CHANGELOG.rst'),
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    entry_points='''
        [console_scripts]
        kao=oar_kao:kao
    ''',
)
