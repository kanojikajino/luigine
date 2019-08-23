#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' setup file for luigine. '''

__author__ = 'Hiroshi Kajino'
__version__ = '1.0'
__copyright__ = '(C) Copyright IBM Corp. 2019'

from setuptools import setup, find_packages

setup(
    name = 'luigine',
    version = '1.0',
    author = 'Hiroshi Kajino',
    package_dir = {'': 'src'},
    packages = find_packages(where='src', exclude=['*.tests', '*.tests.*', 'tests.*', 'tests']),
    test_suite = 'tests',
    include_package_data=True,
)
