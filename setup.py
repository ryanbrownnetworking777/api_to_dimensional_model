# -*- coding: utf8 -*-
#
# This file were created by Python Boilerplate. Use Python Boilerplate to start
# simple, usable and best-practices compliant Python projects.
#
# Learn more about it at: http://github.com/fabiommendes/python-boilerplate/
#

import os

from setuptools import setup, find_packages

# Meta information
dirname = os.path.dirname(__file__)

# Save version and author to __meta__.py
setup(
    # Basic info
    name='apitodimensionalmodel',
    version='0.0.1',
    author='Ryan Brown',
    author_email='ryanbrownnetworking777@gmail.com',
    url='https://ghttps://github.com/ryanbrownnetworking777/api_to_dimensional_model',
    description='Take the results of an API endpoint, and process them into star schema with Python.',
    long_description=open('README.md').read(),
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License' 
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
    ],

    # Packages and depencies
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        'pandas'
        ,'requests'
    ],
    extras_require={
        'dev': [
            'manuel'
            ,'pytest'
            ,'pytest-cov'
            ,'coverage'
            ,'mock'
        ],
    },
    # Data files
    package_data={
        'api_to_dimensional_model': [
            'templates/*.*',
            'templates/license/*.*',
            'templates/docs/*.*',
            'templates/package/*.*'
        ],
    },


    # Other configurations
    zip_safe=False,
    platforms='any',
)
