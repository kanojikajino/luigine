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
import os
from pathlib import Path
import sys
from luigine.abc import AutoNamingTask, main, OptunaTask

from datetime import datetime
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import glob
import logging
import luigi
import numpy as np

try:
    working_dir = Path(sys.argv[1:][sys.argv[1:].index("--working-dir")
                                    + 1]).resolve()
except ValueError:
    raise ValueError("--working-dir option must be specified.")

# load parameters from `INPUT/param.py`
sys.path.append(str((working_dir / 'INPUT').resolve()))
from param import (DataPreprocessing_params,
                   Train_params,
                   PerformanceEvaluation_params,
                   HyperparameterOptimization_params)

logger = logging.getLogger('luigi-interface')
AutoNamingTask._working_dir = working_dir
AutoNamingTask.working_dir = luigi.Parameter(default=str(working_dir))

# ----------- preamble ------------


# Define tasks

class DataPreprocessing(AutoNamingTask):

    DataPreprocessing_params = luigi.DictParameter()

    def requires(self):
        return []

    def run_task(self, input_list):
        w = np.random.randn(self.DataPreprocessing_params['in_dim'])
        X_train = np.random.randn(self.DataPreprocessing_params['train_size'],
                                  self.DataPreprocessing_params['in_dim'])
        X_val = np.random.randn(self.DataPreprocessing_params['val_size'],
                                self.DataPreprocessing_params['in_dim'])
        X_test = np.random.randn(self.DataPreprocessing_params['test_size'],
                                 self.DataPreprocessing_params['in_dim'])
        y_train = X_train @ w
        y_val = X_val @ w
        y_test = X_test @ w
        return (X_train, y_train,
                X_val, y_val,
                X_test, y_test)


class Train(AutoNamingTask):

    DataPreprocessing_params = luigi.DictParameter()
    Train_params = luigi.DictParameter()

    def requires(self):
        return [DataPreprocessing(DataPreprocessing_params=self.DataPreprocessing_params)]

    def run_task(self, input_list):
        X_train, y_train, _, _, _, _ = input_list[0]
        model = Ridge(**self.Train_params['model_kwargs'])
        model.fit(X_train, y_train)
        return model


class PerformanceEvaluation(AutoNamingTask):

    ''' Performance evaluation on the validation set.
    '''

    output_ext = luigi.Parameter(default='txt')
    DataPreprocessing_params = luigi.DictParameter(default=DataPreprocessing_params)
    Train_params = luigi.DictParameter(default=Train_params)
    PerformanceEvaluation_params = luigi.DictParameter(default=PerformanceEvaluation_params)

    def requires(self):
        return [DataPreprocessing(DataPreprocessing_params=self.DataPreprocessing_params),
                Train(DataPreprocessing_params=self.DataPreprocessing_params,
                      Train_params=self.Train_params)]

    def run_task(self, input_list):
        _, _, X_val, y_val, _, _ = input_list[0]
        model = input_list[1]
        y_pred = model.predict(X_val)
        mse = mean_squared_error(y_val, y_pred)
        return mse

    def save_output(self, mse):
        with open(self.output().path, 'w') as f:
            f.write(f'{mse}')

    def load_output(self):
        with open(self.output().path, 'w') as f:
            mse = f.read()
        return mse


class HyperparameterOptimization(OptunaTask):

    ''' Hyperparameter tuning using the validation set.
    '''

    OptunaTask_params = luigi.DictParameter(default=HyperparameterOptimization_params)

    def obj_task(self, **kwargs):
        logger.info(kwargs)
        return PerformanceEvaluation(**kwargs)


class TestPerformanceEvaluation(AutoNamingTask):

    ''' Pick up the best model (on the validation set), and examine its real performance on the test set.
    '''

    output_ext = luigi.Parameter(default='txt')
    OptunaTask_params = luigi.DictParameter(default=HyperparameterOptimization_params)
    use_mlflow = luigi.BoolParameter(default=True)
    n_trials = luigi.IntParameter()

    def requires(self):
        return [DataPreprocessing(DataPreprocessing_params=self.OptunaTask_params['DataPreprocessing_params']),
                HyperparameterOptimization(OptunaTask_params=self.OptunaTask_params,
                                           n_trials=self.n_trials)]

    def run_task(self, input_list):
        _, _, _, _, X_test, y_test = input_list[0]
        _, best_params = input_list[1]
        get_model_task = Train(DataPreprocessing_params=best_params['DataPreprocessing_params'],
                               Train_params=best_params['Train_params'])
        luigi.build([get_model_task])
        model = get_model_task.load_output()
        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        import mlflow
        mlflow.log_metric('mse', mse)
        logger.info(f'''

===================
test_mse = {mse}
===================

''')
        return mse

    def save_output(self, mse):
        with open(self.output().path, 'w') as f:
            f.write(f'{mse}')

    def load_output(self):
        with open(self.output().path, 'w') as f:
            mse = f.read()
        return mse


if __name__ == "__main__":
    for each_engine_status in glob.glob(str(working_dir / 'engine_status.*')):
        os.remove(each_engine_status)
    with open(working_dir / 'engine_status.ready', 'w') as f:
        f.write("ready: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    main(working_dir)
