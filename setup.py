"""Defines setup for pip."""

from os import path
from io import open

from setuptools import setup, find_packages

PWD = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(PWD, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dynafed_storagestats',
    version='1.0.22',
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
        'requests',
        'requests_aws4auth'
    ],
    entry_points={
        'console_scripts': [
            'dynafed-storage=dynafed_storagestats.runner:main',
        ],
    },
)
