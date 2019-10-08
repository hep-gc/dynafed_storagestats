#!/usr/bin/env python3
"""Defines setup for pip."""

from os import path
from io import open
import re
import sys

from setuptools import setup, find_packages

PWD = path.abspath(path.dirname(__file__))
PACKAGE_NAME = 'dynafed_storagestats'

# Obtain version from version.py
try:
    filepath = PWD + '/' + PACKAGE_NAME + '/version.py'

    with open(filepath) as file:
        __version__, = re.findall('__version__ = "(.*)"', file.read())

except Exception as error:
    sys.stderr.write("Warning: Could not open '%s' due %s\n" % (filepath, error))
    sys.exit(1)

# Get the long description from the README file
with open(path.join(PWD, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name=PACKAGE_NAME,
    version=__version__,
    description='Dynafed Storage Stats Module',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/hep-gc/dynafed_storagestats',
    author='Fernando Fernandez Galindo',
    author_email='ffernandezgalindo@triumf.ca',
    license='Apache',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
    ],
    packages=find_packages(exclude=['docs', 'scripts', 'tests']),
    python_requires='~=3.4',
    install_requires=[
        'azure-storage',
        'boto3',
        'lxml',
        'prometheus_client',
        'python-memcached',
        'PyYAML',
        'requests',
        'requests_aws4auth'
    ],
    entry_points={
        'console_scripts': [
            'dynafed-storage=dynafed_storagestats.runner:main',
        ],
    },
)
