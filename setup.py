import os.path as op
from setuptools import setup, find_packages
from kao import VERSION

here = op.abspath(op.dirname(__file__))


def read(fname):
    ''' Return the file content. '''
    with open(op.join(here, fname)) as f:
        return f.read()


setup(
    name='kao',
    author='Olivier Richard',
    author_email='olivier.richard@imag.fr',
    version=VERSION,
    url='https://github.com/oar-team/kao',
    install_requires=[
        'Click',
    ],
    packages=find_packages(),
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
        kao=kao.kao:kao
    ''',
)
