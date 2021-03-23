#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Title """

__author__ = "Hiroshi Kajino <KAJINO@jp.ibm.com>"
__copyright__ = "(c) Copyright IBM Corp. 2019"
__version__ = "1.0"
__date__ = "Aug 23 2019"


MultipleRun_params = {
    'DataPreprocessing_params': {
        'in_dim': 50,
        'train_size': 100,
        'val_size': 100,
        'test_size': 500},
    'Train_params': {
        'model_kwargs': {'@alpha': [1e-4, 1e-3, 1e-2, 1e-1],
                         '@fit_intercept': [True, False]}
    },
    'PerformanceEvaluation_params': {}
}
