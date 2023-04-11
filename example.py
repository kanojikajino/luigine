#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A template main script.
"""

__author__ = "Hiroshi Kajino"
__copyright__ = "Copyright IBM Corp. 2019, 2021"

# set luigi_config_path BEFORE importing luigi
import os
from pathlib import Path
import sys
from luigine.abc import (AutoNamingTask,
                         main,
                         MultipleRunBase,
                         HyperparameterSelectionTask)
from datetime import datetime
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
import glob
import logging
from luigi.util import requires
import luigi
import numpy as np

try:
    working_dir = Path(sys.argv[1:][sys.argv[1:].index("--working-dir")
                                    + 1]).resolve()
except ValueError:
    raise ValueError("--working-dir option must be specified.")

# load parameters from `INPUT/param.py`
sys.path.append(str((working_dir / 'INPUT').resolve()))
from param import MultipleRun_params

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


@requires(DataPreprocessing)
class Train(AutoNamingTask):

    Train_params = luigi.DictParameter()

    def run_task(self, input_list):
        X_train, y_train, _, _, _, _ = input_list[0]
        model = Ridge(**self.Train_params['model_kwargs'])
        model.fit(X_train, y_train)
        return model


@requires(DataPreprocessing, Train)
class PerformanceEvaluation(AutoNamingTask):

    ''' Performance evaluation on the validation set.
    '''

    PerformanceEvaluation_params = luigi.DictParameter()

    def run_task(self, input_list):
        _, _, X_val, y_val, _, _ = input_list[0]
        model = input_list[1]
        y_pred = model.predict(X_val)
        mse = mean_squared_error(y_val, y_pred)
        return mse


class HyperparameterOptimization(HyperparameterSelectionTask):

    ''' Hyperparameter tuning using the validation set.
    '''

    MultipleRun_params = luigi.DictParameter()

    def obj_task(self, **kwargs):
        return PerformanceEvaluation(**kwargs)


@requires(HyperparameterOptimization)
class TestPerformanceEvaluation(AutoNamingTask):

    ''' Pick up the best model (on the validation set), and examine its real performance on the test set.
    '''

    MultipleRun_params = luigi.DictParameter(
        default=MultipleRun_params)

    def run_task(self, input_list):
        _, best_params = input_list[0]

        data_task = DataPreprocessing(
            DataPreprocessing_params=best_params['DataPreprocessing_params'])
        _, _, _, _, X_test, y_test = data_task.load_output()
        get_model_task = Train(
            DataPreprocessing_params=best_params['DataPreprocessing_params'],
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
        return mse

    def save_output(self, mse):
        with open(self.output().path, 'w') as f:
            f.write(f'{mse}')

    def load_output(self):
        with open(self.output().path, 'r') as f:
            mse = f.read()
        return mse


class MultipleRun(MultipleRunBase):

    ''' Hyperparameter tuning using the validation set.
    '''

    MultipleRun_params = luigi.DictParameter()

    def obj_task(self, **kwargs):
        return PerformanceEvaluation(**kwargs)


@requires(MultipleRun)
class GatherScores(AutoNamingTask):

    MultipleRun_params = luigi.DictParameter(
        default=MultipleRun_params)

    def run_task(self, input_list):
        logger.info(input_list[0])

if __name__ == "__main__":
    for each_engine_status in glob.glob(str(working_dir / 'engine_status.*')):
        os.remove(each_engine_status)
    with open(working_dir / 'engine_status.ready', 'w') as f:
        f.write("ready: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    main(working_dir)
