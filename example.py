#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Title """

__author__ = "Hiroshi Kajino <KAJINO@jp.ibm.com>"
__copyright__ = "(c) Copyright IBM Corp. 2018"
__version__ = "0.1"
__date__ = "Jan 1 2018"

# set luigi_config_path BEFORE importing luigi
import argparse
import os
import sys
try:
    working_dir = sys.argv[1:][sys.argv[1:].index("--working-dir") + 1]
    os.chdir(working_dir)
except ValueError:
    raise argparse.ArgumentError("--working-dir option must be specified.")
# add a path to luigi.cfg
os.environ["LUIGI_CONFIG_PATH"] = os.path.abspath(os.path.join("INPUT", "luigi.cfg"))
sys.path.append(os.path.abspath(os.path.join("INPUT")))

# load parameters from `INPUT/param.py`
from param import *
from luigine.abc import MainTask, AutoNamingTask, main
import logging
logger = logging.getLogger('luigi-interface')

# Define tasks


if __name__ == "__main__":
    for each_engine_status in glob.glob("./engine_status.*"):
        os.remove(each_engine_status)
    with open("engine_status.ready", "w") as f:
        f.write("ready: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    main()
