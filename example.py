#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A template main script.
"""

__author__ = "Hiroshi Kajino"
__copyright__ = "(c) Copyright IBM Corp. 2019"
__version__ = "1.0"
__date__ = "Aug 23 2019"

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
from param import (DataPreprocessing_params,
                   Train_params,
                   PerformanceEvaluation_params,
                   HyperparameterOptimization_params)
from luigine.abc import MainTask, AutoNamingTask, main, OptunaTask

from datetime import datetime
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import glob
import gzip
import logging
import luigi
import numpy as np
import pickle
import sklearn

logger = logging.getLogger('luigi-interface')


# Define tasks

class DataPreprocessing(AutoNamingTask):

    DataPreprocessing_params = luigi.DictParameter()
    working_subdir = luigi.Parameter(default='data_prep')

    def requires(self):
        return []

    def run(self):
        w = np.random.randn(self.DataPreprocessing_params['in_dim'])
        X_train = np.random.randn(self.DataPreprocessing_params['train_size'],
                                  self.DataPreprocessing_params['in_dim'])
        X_val =  np.random.randn(self.DataPreprocessing_params['val_size'],
                                 self.DataPreprocessing_params['in_dim'])
        X_test =  np.random.randn(self.DataPreprocessing_params['test_size'],
                                  self.DataPreprocessing_params['in_dim'])
        y_train = X_train @ w
        y_val = X_val @ w
        y_test = X_test @ w

        with gzip.open(self.output().path, 'wb') as f:
            pickle.dump((X_train, y_train,
                         X_val, y_val,
                         X_test, y_test), f)



class Train(AutoNamingTask):

    DataPreprocessing_params = luigi.DictParameter()
    Train_params = luigi.DictParameter()
    working_subdir = luigi.Parameter(default='train')

    def requires(self):
        return DataPreprocessing(DataPreprocessing_params=self.DataPreprocessing_params)

    def run(self):
        X_train, y_train, _, _, _, _ = self.requires().load_output()
        model = Ridge(**self.Train_params['model_kwargs'])
        model.fit(X_train, y_train)
        with gzip.open(self.output().path, 'wb') as f:
            pickle.dump(model, f)


class PerformanceEvaluation(MainTask, AutoNamingTask):

    ''' Performance evaluation on the validation set.
    '''

    output_ext = luigi.Parameter(default='txt')
    DataPreprocessing_params = luigi.DictParameter(default=DataPreprocessing_params)
    Train_params = luigi.DictParameter(default=Train_params)
    PerformanceEvaluation_params = luigi.DictParameter(default=PerformanceEvaluation_params)
    working_subdir = luigi.Parameter(default='eval')

    def requires(self):
        return [DataPreprocessing(DataPreprocessing_params=self.DataPreprocessing_params),
                Train(DataPreprocessing_params=self.DataPreprocessing_params,
                      Train_params=self.Train_params)]

    def run(self):
        _input = self.requires()
        _, _, X_val, y_val, _, _ = _input[0].load_output()
        model = _input[1].load_output()
        y_pred = model.predict(X_val)
        mse = mean_squared_error(y_val, y_pred)

        with open(self.output().path, 'w') as f:
            f.write(f'{mse}')


class HyperparameterOptimization(OptunaTask, MainTask):

    ''' Hyperparameter tuning using the validation set.
    '''

    OptunaTask_params = luigi.DictParameter(default=HyperparameterOptimization_params)

    def obj_task(self, **kwargs):
        logger.info(kwargs)
        return PerformanceEvaluation(working_dir=self.working_dir, **kwargs)


class TestPerformanceEvaluation(MainTask, AutoNamingTask):

    ''' Pick up the best model (on the validation set), and examine its real performance on the test set.
    '''

    output_ext = luigi.Parameter(default='txt')
    OptunaTask_params = luigi.DictParameter(default=HyperparameterOptimization_params)
    n_trials = luigi.IntParameter()
    working_subdir = luigi.Parameter(default='test_performance')

    def requires(self):
        return [DataPreprocessing(DataPreprocessing_params=self.OptunaTask_params['DataPreprocessing_params']),
                HyperparameterOptimization(OptunaTask_params=self.OptunaTask_params,
                                           n_trials=self.n_trials,
                                           working_dir=self.working_dir)]

    def run(self):
        _input = self.requires()
        _, _, _, _, X_test, y_test = _input[0].load_output()
        best_params = _input[1].get_best_params()
        get_model_task = Train(DataPreprocessing_params=best_params['DataPreprocessing_params'],
                               Train_params=best_params['Train_params'])
        luigi.build([get_model_task])
        model = get_model_task.load_output()
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        logger.info(f'''

===================
test_mse = {mse}
===================

''')

        with open(self.output().path, 'w') as f:
            f.write(f'{mse}')
        

if __name__ == "__main__":
    for each_engine_status in glob.glob("./engine_status.*"):
        os.remove(each_engine_status)
    with open("engine_status.ready", "w") as f:
        f.write("ready: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    main()
