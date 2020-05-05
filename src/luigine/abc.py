#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Hiroshi Kajino, Takeshi Teshima"
__copyright__ = "(c) Copyright IBM Corp. 2019"

import datetime
import errno
import gc
import gzip
import hashlib
import logging
import math
import os
import pickle
import pprint
import shutil
from copy import deepcopy
from collections import OrderedDict
import mlflow
import luigi
import numpy as np
from optuna import exceptions
from optuna import structs
from optuna import trial as trial_module
import optuna
from .utils import sort_dict, dict_to_str, checksum

logger = logging.getLogger('luigi-interface')


@luigi.Task.event_handler(luigi.Event.FAILURE)
@luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
def curse_failure(*kwargs):
    #if os.path.exists("engine_status.progress"):
    #    os.rename("engine_status.progress", "engine_status.error")
    #with open("engine_status.error", "a") as f:
    #    f.write("error: {}\n".format(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    with open(os.path.join("ENGLOG", "engine.log"), "a") as f:
        f.write("{}".format(kwargs))
    #raise RuntimeError('error occurs and halt.')


def main():
    optuna.logging.enable_propagation()
    optuna.logging.disable_default_handler()
    # check INPUT directory
    if not os.path.exists(os.path.join("INPUT", "luigi.cfg")):
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                os.path.join("INPUT", "luigi.cfg"))
    if not os.path.exists(os.path.join("INPUT", "logging.conf")):
        raise FileNotFoundError(errno.ENOENT,
                                os.strerror(errno.ENOENT),
                                os.path.join("INPUT", "luigi.cfg"))

    # mkdir if not exists
    if not os.path.exists(os.path.join("ENGLOG")):
        os.mkdir(os.path.join("ENGLOG"))
    if not os.path.exists(os.path.join("OUTPUT")):
        os.mkdir(os.path.join("OUTPUT"))

    os.rename("engine_status.ready", "engine_status.progress")
    with open("engine_status.progress", "a") as f:
        f.write("progress: {}\n".format(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')))

    # run
    try:
        status_code = luigi.run(local_scheduler=True, detailed_summary=True)
        is_success\
            = (status_code.status == luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY\
               or status_code.status == luigi.execution_summary.LuigiStatusCode.SUCCESS)
        if not is_success:
            raise RuntimeError('task fails')
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.complete")
    except:
        import traceback
        if os.path.exists("engine_status.progress"):
            # when KeyboardInterrupt occurs, curse_failure may be halted during its process.
            os.rename("engine_status.progress", "engine_status.error")
        with open("engine_status.error", "a") as f:
            f.write("error: {}\n".format(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        with open(os.path.join("ENGLOG", "engine.log"), "a") as f:
            f.write(traceback.format_exc())


class MainTask(luigi.Task):

    '''
    A main task should inherit this class.
    '''

    working_dir = luigi.Parameter()

    def load_output(self):
        "Interface to load and return the output object."
        pass


class MainWrapperTask(luigi.WrapperTask):

    '''
    A main wrapper task should inherit this class.
    '''

    working_dir = luigi.Parameter()

    def load_output(self):
        "Interface to load and return the output object."
        pass


class AutoNamingTask(luigi.Task):

    '''
    This task defines the output name automatically from task parameters.

    Attributes
    ----------
    hash_num : int
        the number of characters used for hashing
    working_subdir : str
        the name of directory where the output of this task is stored
    output_ext : str
        extension of the output file
    '''

    __no_hash_keys__ = []
    hash_num = luigi.IntParameter(default=10)
    use_mlflow = luigi.BoolParameter(default=False)
    remove_output_file = luigi.BoolParameter(default=False)
    copy_output_to_top = luigi.Parameter(default='')
    output_ext = luigi.Parameter(default='pklz')

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.param_name = ""
        if self.use_mlflow:
            if mlflow.active_run() is None:
                mlflow.set_experiment(self.__class__.__name__)
                mlflow.start_run()

        # md5checksum of input files
        if self.input_file():
            for each_input_file in self.input_file():
                self.param_name = self.param_name + checksum(each_input_file)[:self.hash_num] + '_'

        param_kwargs = deepcopy(self.__dict__["param_kwargs"])
        if "working_subdir" in param_kwargs:
            param_kwargs.pop("working_subdir")
        for each_key in self.__no_hash_keys__:
            if len(each_key) == 2:
                self.param_name = self.param_name + str(param_kwargs[each_key[0]][each_key[1]]) + "_"
                param_kwargs[each_key[0]] = dict(param_kwargs[each_key[0]])
                param_kwargs[each_key[0]].pop(each_key[1])
            else:
                self.param_name = self.param_name + str(param_kwargs[each_key]) + "_"
                param_kwargs.pop(each_key)
        for each_key in sorted(param_kwargs.keys()):
            if isinstance(param_kwargs[each_key], (dict, OrderedDict, luigi.freezing.FrozenOrderedDict)):
                self.param_name \
                    = self.param_name \
                    + hashlib.md5(dict_to_str(sort_dict(param_kwargs[each_key]))\
                                  .encode("utf-8")).hexdigest()[:self.hash_num] + "_"
            else:
                self.param_name = self.param_name \
                                  + hashlib.md5(str(param_kwargs[each_key]).encode("utf-8")).hexdigest()[:self.hash_num] + "_"
        self.param_name = self.param_name[:-1]

    def run_task(self, input_list):
        raise NotImplementedError

    def run(self):
        if not isinstance(self.requires(), list):
            raise ValueError('self.requires must return a list of tasks.')

        input_list = [each_task.load_output() for each_task in self.requires()]
        valid_input = self.check_input(input_list)
        if not valid_input:
            raise ValueError('input format is not valid.')

        res = self.run_task(input_list)

        valid_output = self.check_output(res)
        if not valid_output:
            raise ValueError('output format is not valid.')

        if res is not None:
            self.save_output(res)
        if self.copy_output_to_top != '':
            shutil.copy(self.output().path, os.path.join('OUTPUT', self.copy_output_to_top))

    def output(self):
        if not os.path.exists('OUTPUT'):
            os.mkdir('OUTPUT')
        if not os.path.exists(os.path.join(
                "OUTPUT",
                self.working_subdir)):
            os.mkdir(os.path.join(
                "OUTPUT",
                self.working_subdir))
        return luigi.LocalTarget(os.path.join(
            "OUTPUT",
            self.working_subdir,
            "{}.{}".format(self.param_name, self.output_ext)))

    def load_output(self):
        with gzip.open(self.output().path, 'rb') as f:
            res = pickle.load(f)
        if self.remove_output_file:
            self.remove_output()
        return res

    def save_output(self, obj):
        with gzip.open(self.output().path, 'wb') as f:
            pickle.dump(obj, f)
        if self.use_mlflow:
            mlflow.log_artifact(self.output().path)

    def check_input(self, input_list):
        ''' check the input format
        '''
        return True

    def check_output(self, res):
        ''' check the output format
        '''
        return True

    def remove_output(self):
        if os.path.exists(self.output().path):
            os.remove(self.output().path)

    def input_file(self):
        ''' return a list of input file paths
        '''
        return []

    @property
    def working_subdir(self):
        return self.__class__.__name__


class OptunaTask(AutoNamingTask):

    ''' Parameter optimization task using Optuna.
    Given a task that receives parameters and outputs a loss to be minimized,
    this task can find better parameters that will achieve lower loss.

    Attributes
    ----------
    OptunaTask_params : DictParameters
        Dictionary of Dictionaries, where each dictionary corresponds to each task's `DictParameter`
        A key starting from `@` will get suggestion from optuna.
        For example, if the user would like to choose better regularization parameter `C` from [0, 1], instead of specifying {'C': 1.0}, specify it as {'@C': ['uniform', [0, 1]]}.
        The first element of the value is used to choose `suggest_{}` method, and the second element (list) is used as args for the suggestion method.
    n_trials : int
        the number of trials in optuna.
    '''

    output_ext = luigi.Parameter(default='db')
    OptunaTask_params = luigi.DictParameter()
    n_trials = luigi.IntParameter(default=100)

    def obj_task(self):
        ''' return a `luigi.Task` instance, which, given a set of parameters in dict, returns a `loss` to be minimized.
        '''
        raise NotImplementedError

    def run_task(self, input_list):
        pass

    def run(self):
        ''' non-negligible part of this function was copied from optuna code distributed under MIT license.
        License file: https://github.com/pfnet/optuna/blob/master/LICENSE
        Copyright (c) 2018 Preferred Networks, Inc.
        '''
        study_name = os.path.splitext(os.path.basename(self.output().path))[0]
        study = optuna.create_study(study_name=study_name, storage=f'sqlite:///{self.output().path}',
                                    load_if_exists=True)
            
        n_prev_trials = len(study.trials)
        n_trials = self.n_trials
        timeout = None
        gc_after_trial = True
        if not study._optimize_lock.acquire(False):
            raise RuntimeError("Nested invocation of `Study.optimize` method isn't allowed.")

        try:
            i_trial = 0
            time_start = datetime.datetime.now()
            while True:
                if n_trials is not None:
                    if i_trial >= n_trials:
                        break
                    i_trial += 1

                if timeout is not None:
                    elapsed_seconds = (datetime.datetime.now() - time_start).total_seconds()
                    if elapsed_seconds >= timeout:
                        break

                if n_prev_trials == 0:
                    task_done = False
                else:
                    if i_trial > n_prev_trials:
                        pass
                    else:
                        if study.trials_dataframe().loc[i_trial-1, 'value'] is None:
                            task_done = False
                        elif np.isnan(study.trials_dataframe().loc[i_trial-1, 'value']):
                            task_done = False
                        else:
                            task_done = True
                if i_trial > n_prev_trials:
                    trial_id = study._pop_waiting_trial_id()
                    if trial_id is None:
                        trial_id = study._storage.create_new_trial(study._study_id)
                    trial = trial_module.Trial(study, trial_id)
                    trial_number = trial.number
                    param_dict = self.create_params(trial)
                    res = self.obj_task(**param_dict)
                    res_output = yield res
                elif i_trial >= n_prev_trials and (not task_done):
                    trial_id = i_trial
                    trial = trial_module.Trial(study, trial_id)
                    trial_number = trial.number

                    param_dict = self.create_params(trial)
                    res = self.obj_task(**param_dict)
                    res_output = yield res
                    try:
                        result = float(res_output.open('r').read())
                    except exceptions.TrialPruned as e:
                        message = 'Setting status of trial#{} as {}. {}'.format(trial_number,
                                                                                structs.TrialState.PRUNED,
                                                                                str(e))
                        logger.info(message)

                        # Register the last intermediate value if present as the value of the trial.
                        # TODO(hvy): Whether a pruned trials should have an actual value can be discussed.
                        frozen_trial = study._storage.get_trial(trial_id)
                        last_step = frozen_trial.last_step
                        if last_step is not None:
                            study._storage.set_trial_value(
                                trial_id, frozen_trial.intermediate_values[last_step])
                        study._storage.set_trial_state(trial_id, structs.TrialState.PRUNED)
                    except Exception as e:
                        message = 'Setting status of trial#{} as {} because of the following error: {}'\
                            .format(trial_number, structs.TrialState.FAIL, repr(e))
                        logger.warning(message, exc_info=True)
                        study._storage.set_trial_system_attr(trial_id, 'fail_reason', message)
                        study._storage.set_trial_state(trial_id, structs.TrialState.FAIL)
                        raise
                    finally:
                        # The following line mitigates memory problems that can be occurred in some
                        # environments (e.g., services that use computing containers such as CircleCI).
                        # Please refer to the following PR for further details:
                        # https://github.com/optuna/optuna/pull/325.
                        if gc_after_trial:
                            gc.collect()

                    try:
                        result = float(result)
                    except (
                            ValueError,
                            TypeError,
                    ):
                        message = 'Setting status of trial#{} as {} because the returned value from the ' \
                                  'objective function cannot be casted to float. Returned value is: ' \
                                  '{}'.format(trial_number, structs.TrialState.FAIL, repr(result))
                        logger.warning(message)
                        study._storage.set_trial_system_attr(trial_id, 'fail_reason', message)
                        study._storage.set_trial_state(trial_id, structs.TrialState.FAIL)
                        return trial

                    if math.isnan(result):
                        message = 'Setting status of trial#{} as {} because the objective function ' \
                                  'returned {}.'.format(trial_number, structs.TrialState.FAIL, result)
                        logger.warning(message)
                        study._storage.set_trial_system_attr(trial_id, 'fail_reason', message)
                        study._storage.set_trial_state(trial_id, structs.TrialState.FAIL)
                        return trial

                    study._storage.set_trial_value(trial_id, result)
                    study._storage.set_trial_state(trial_id, structs.TrialState.COMPLETE)
                    study._log_completed_trial(trial_number, result)
                    #study._progress_bar.update((datetime.datetime.now() - time_start).total_seconds())
            study._storage.remove_session()
        finally:
            study._optimize_lock.release()
            #study._progress_bar.close()
            #del study._progress_bar
        best_params_str = pprint.pformat(study.best_params)
        logger.info(f'''
=====================================
best_params:\n{best_params_str}
best_value:\t{study.best_value}
=====================================
        ''')
        return None

    def create_params(self, trial):
        ''' create a dictionary of parameters for `obj_task` given a trial

        Parameters
        ----------
        trial : optuna.trial.Trial
            trial object

        Returns
        -------
        dict
            parameters for `obj_task`
        '''

        def _create_subparams(trial, param_dict):
            out_param_dict = dict()
            for each_key, each_val in param_dict.items():
                if isinstance(each_val, (dict, OrderedDict, luigi.freezing.FrozenOrderedDict)):
                    out_param_dict[each_key] = _create_subparams(trial, each_val)
                elif each_key.startswith('@'):
                    out_param_dict[each_key[1:]] = getattr(
                        trial,
                        'suggest_{}'.format(each_val[0]))(each_key[1:], *each_val[1])
                else:
                    out_param_dict[each_key] = each_val
            return out_param_dict

        return _create_subparams(trial, self.OptunaTask_params)

    def objective(self, trial):
        ''' objective function

        Parameters
        ----------
        trial : optuna.trial.Trial
            trial object

        Returns
        -------
        float
            loss to be minimized. if a task fails, it returns `np.inf`
        '''
        param_dict = self.create_params(trial)
        res = self.obj_task(**param_dict)
        luigi.build([res], workers=1)
        try:
            val = float(res.output().open('r').read())
        except:
            val = np.inf
            
        return val

    def load_study(self):
        study_name = os.path.splitext(os.path.basename(self.output().path))[0]
        study = optuna.load_study(study_name=study_name,
                                  storage=f'sqlite:///{self.output().path}')
        return study

    def load_output(self):
        study = self.load_study()
        return study, self.best_params

    @property
    def best_params(self):
        study = self.load_study()

        def _create_subparams(study, param_dict):
            out_param_dict = dict()
            for each_key, each_val in param_dict.items():
                if isinstance(each_val, (dict, OrderedDict, luigi.freezing.FrozenOrderedDict)):
                    out_param_dict[each_key] = _create_subparams(study, each_val)
                elif each_key.startswith('@'):
                    out_param_dict[each_key[1:]] = study.best_params[each_key[1:]]
                else:
                    out_param_dict[each_key] = each_val
            return out_param_dict
        return _create_subparams(study, self.OptunaTask_params)

    @property
    def best_value(self):
        study = self.load_study()
        return study.best_value
