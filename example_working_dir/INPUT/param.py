#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Title """

__author__ = "Hiroshi Kajino <KAJINO@jp.ibm.com>"
__copyright__ = "(c) Copyright IBM Corp. 2019"
__version__ = "1.0"
__date__ = "Aug 23 2019"

DataPreprocessing_params = {
    'in_dim': 5,
    'train_size': 1000,
    'test_size': 100}

Train_params = {
    'model_kwargs': {'alpha': 1e-2,
                     'fit_intercept': True}
}

PerformanceEvaluation_params = {}
