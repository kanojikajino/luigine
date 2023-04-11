#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' setup file for luigine. '''

__author__ = 'Hiroshi Kajino'
__copyright__ = 'Copyright IBM Corp. 2019, 2021'

from setuptools import setup, find_packages

def _requires_from_file(filename):
    return open(filename).read().splitlines()

setup(
    name='luigine',
    version='1.4.1',
    author='Hiroshi Kajino',
    url='https://github.com/kanojikajino/luigine',
    author_email='hiroshi.kajino.1989@gmail.com',
    package_dir={'': 'src'},
    packages=find_packages(where='src', exclude=['*.tests', '*.tests.*', 'tests.*', 'tests']),
    test_suite='tests',
    include_package_data=True,
    install_requires=_requires_from_file('requirements.txt'),
    license='MIT',
)
