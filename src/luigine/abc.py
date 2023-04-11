#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Hiroshi Kajino, Takeshi Teshima'
__copyright__ = 'Copyright IBM Corp. 2019, 2021'

import datetime
import dill
import gzip
import hashlib
from itertools import product
import logging
import os
import pickle
import shutil
import time
from copy import deepcopy
from collections import OrderedDict
import luigi
from luigi.setup_logging import InterfaceLogging
import pandas as pd
from .utils import sort_dict, dict_to_str, checksum

logger = logging.getLogger('luigi-interface')


'''
@luigi.Task.event_handler(luigi.Event.FAILURE)
@luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
def curse_failure(*kwargs):
    with open(os.path.join('ENGLOG', 'engine.log'), 'a') as f:
        f.write('{}'.format(kwargs))
'''

def main(working_dir):
    @classmethod
    def _cli(cls, opts):
        """Setup logging via CLI options

        If `--background` -- set INFO level for root logger.
        If `--logdir` -- set logging with next params:
            default Luigi's formatter,
            INFO level,
            output in logdir in `luigi-server.log` file
        """
        logging.basicConfig(
            level=logging.INFO,
            filename=str(working_dir / 'ENGLOG' / 'engine.log'))
        return True
    InterfaceLogging._cli = _cli
    luigi.Task.disable_window_seconds = None

    # mkdir if not exists
    if not (working_dir / 'ENGLOG').exists():
        os.mkdir(working_dir / 'ENGLOG')
    if not (working_dir / 'OUTPUT').exists():
        os.mkdir(working_dir / 'OUTPUT')

    os.rename(working_dir / 'engine_status.ready',
              working_dir / 'engine_status.progress')
    with open(working_dir / 'engine_status.progress', 'a') as f:
        f.write('progress: {}\n'.format(
            datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')))

    # run
    try:
        status_code = luigi.run(local_scheduler=True, detailed_summary=True)
        is_success\
            = (status_code.status == luigi.execution_summary.LuigiStatusCode.SUCCESS_WITH_RETRY\
               or status_code.status == luigi.execution_summary.LuigiStatusCode.SUCCESS)
        if not is_success:
            raise RuntimeError('task fails')
        if (working_dir / 'engine_status.progress').exists():
            os.rename(working_dir / 'engine_status.progress',
                      working_dir / 'engine_status.complete')
    except:
        import traceback
        if (working_dir / 'engine_status.progress').exists():
            # when KeyboardInterrupt occurs, curse_failure may be halted during its process.
            os.rename(working_dir / 'engine_status.progress',
                      working_dir / 'engine_status.error')
        with open(working_dir / 'engine_status.error',
                  'a') as f:
            f.write('error: {}\n'.format(datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        with open(working_dir / 'ENGLOG' / 'engine.log', 'a') as f:
            f.write(traceback.format_exc())


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
    remove_output_file = luigi.BoolParameter(default=False)
    copy_output_to_top = luigi.Parameter(default='')
    output_ext = luigi.Parameter(default='pklz')
    working_dir = luigi.Parameter()  # used for argparse
    _working_dir = ''  # containing full path

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.param_name = ''

        # md5checksum of input files
        if self.input_file():
            for each_input_file in self.input_file():
                self.param_name = self.param_name + checksum(each_input_file)[:self.hash_num] + '_'

        param_kwargs = deepcopy(self.__dict__['param_kwargs'])
        if 'working_subdir' in param_kwargs:
            param_kwargs.pop('working_subdir')
        if 'working_dir' in param_kwargs:
            param_kwargs.pop('working_dir')
        if '_working_dir' in param_kwargs:
            param_kwargs.pop('_working_dir')
        for each_key in self.__no_hash_keys__:
            if len(each_key) == 2:
                self.param_name = self.param_name + str(param_kwargs[each_key[0]][each_key[1]]) + '_'
                param_kwargs[each_key[0]] = dict(param_kwargs[each_key[0]])
                param_kwargs[each_key[0]].pop(each_key[1])
            else:
                self.param_name = self.param_name + str(param_kwargs[each_key]) + '_'
                param_kwargs.pop(each_key)
        for each_key in sorted(param_kwargs.keys()):
            if isinstance(param_kwargs[each_key],
                          (dict,
                           OrderedDict,
                           luigi.freezing.FrozenOrderedDict)):
                self.param_name \
                    = self.param_name \
                    + hashlib.md5(dict_to_str(sort_dict(param_kwargs[each_key]))\
                                  .encode('utf-8')).hexdigest()[:self.hash_num] + '_'
            else:
                self.param_name \
                    = self.param_name \
                    + hashlib.md5(str(param_kwargs[each_key]).encode('utf-8')).hexdigest()[:self.hash_num] + '_'
        self.param_name = self.param_name[:-1]

    def run_task(self, input_list):
        raise NotImplementedError

    def run(self):
        if isinstance(self.requires(), luigi.Task):
            requires_list = [self.requires()]
        else:
            requires_list = self.requires()

        input_list = [each_task.load_output() for each_task in requires_list]
        valid_input = self.check_input(input_list)
        if not valid_input:
            raise ValueError('input format is not valid.')

        logger.info('the output file will be {}'.format(
            self._working_dir / 'OUTPUT' / self.working_subdir / '{}.{}'.format(self.param_name, self.output_ext)))

        self.start_time = time.time()
        res = self.run_task(input_list)
        self.end_time = time.time()
        self.elapsed_seconds = self.end_time - self.start_time
        logger.info(' * computation time: {} sec'.format(self.elapsed_seconds))

        valid_output = self.check_output(res)
        if not valid_output:
            raise ValueError('output format is not valid.')

        if res is not None:
            self.save_output(res)
        if self.copy_output_to_top != '':
            shutil.copy(self.output().path,
                        self._working_dir / 'OUTPUT' / self.copy_output_to_top)

    def output(self):
        if not os.path.exists(self._working_dir / 'OUTPUT'):
            os.mkdir(self._working_dir / 'OUTPUT')
        if not os.path.exists(self._working_dir
                              / 'OUTPUT'
                              / self.working_subdir):
            os.mkdir(self._working_dir / 'OUTPUT' / self.working_subdir)
        return luigi.LocalTarget(
            self._working_dir / 'OUTPUT' / self.working_subdir /
            '{}.{}'.format(self.param_name, self.output_ext))

    def load_output(self):
        if self.output_ext == 'pklz':
            with gzip.open(self.output().path, 'rb') as f:
                res = pickle.load(f)
        elif self.output_ext == 'pkl':
            with open(self.output().path, 'rb') as f:
                res = pickle.load(f)
        elif self.output_ext == 'dill':
            with open(self.output().path, 'rb') as f:
                res = dill.load(f)
        else:
            raise ValueError('ext {} is not supported'.format(self.output_ext))
        if self.remove_output_file:
            self.remove_output()
        return res

    def save_output(self, obj):
        if self.output_ext == 'pklz':
            with gzip.open(self.output().path, 'wb') as f:
                pickle.dump(obj, f)
        elif self.output_ext == 'pkl':
            with open(self.output().path, 'wb') as f:
                pickle.dump(obj, f)
        elif self.output_ext == 'dill':
            with open(self.output().path, 'wb') as f:
                dill.dump(obj, f)
        else:
            raise ValueError('ext {} is not supported'.format(self.output_ext))

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


class MultipleRunBase(AutoNamingTask):

    ''' Hyperparameter selection task.
    '''

    output_ext = luigi.Parameter(default='pklz')
    MultipleRun_params = luigi.DictParameter()
    score_name = luigi.Parameter(default='score')

    def requires(self):
        task_list = [self.obj_task(**each_param_dict)
                     for each_param_dict in self.param_dict_generator()]
        return task_list

    def obj_task(self):
        ''' return a `luigi.Task` instance,
        which, given a set of parameters in dict,
        returns a `loss` to be minimized.
        '''
        raise NotImplementedError

    def run_task(self, input_list):
        res_list = []
        for each_idx, each_param_dict in enumerate(
                self.param_dict_generator()):
            param_df = self.extract_variable(each_param_dict)
            param_df = param_df.rename({0: each_idx})
            if isinstance(input_list[each_idx], dict):
                score_df = pd.DataFrame(input_list[each_idx],
                                        index=[each_idx])
            else:
                score_df = pd.DataFrame([[input_list[each_idx]]],
                                        columns=[self.score_name],
                                        index=[each_idx])
            res_df = pd.concat([param_df, score_df], axis=1)
            res_list.append(res_df)
        return pd.concat(res_list)

    def param_dict_generator(self):

        def _create_subparams(param_dict):
            product_key_list = []
            product_val_list = []
            for each_key, each_val in param_dict.items():
                if isinstance(each_val, (dict,
                                         OrderedDict,
                                         luigi.freezing.FrozenOrderedDict)):
                    product_key_list.append(each_key)
                    product_val_list.append(_create_subparams(each_val))
                elif each_key.startswith('@'):
                    product_key_list.append(each_key[1:])
                    product_val_list.append(each_val)

            for each_config in product(*product_val_list):
                out_param_dict = dict()
                for each_key, each_val in param_dict.items():
                    if isinstance(each_val,
                                  (dict,
                                   OrderedDict,
                                   luigi.freezing.FrozenOrderedDict)):
                        out_param_dict[each_key] \
                            = each_config[product_key_list.index(each_key)]
                    elif each_key.startswith('@'):
                        out_param_dict[each_key[1:]] \
                            = each_config[product_key_list.index(each_key[1:])]
                    else:
                        out_param_dict[each_key] = each_val
                yield out_param_dict
        return _create_subparams(self.MultipleRun_params)

    def extract_variable(self, param_dict):

        def _identify_variable(param_dict):
            variable_list = []
            for each_key, each_val in param_dict.items():
                if isinstance(each_val, (dict,
                                         OrderedDict,
                                         luigi.freezing.FrozenOrderedDict)):
                    variable_list += [(each_key,) + each_tuple
                                      for each_tuple
                                      in _identify_variable(each_val)]
                elif each_key.startswith('@'):
                    variable_list.append((each_key[1:],))
            return variable_list

        def _extract_variable(each_param_dict, key_tuple):
            if len(key_tuple) == 1:
                return each_param_dict[key_tuple[0]]
            else:
                return _extract_variable(
                    each_param_dict[key_tuple[0]],
                    key_tuple[1:])

        column_list = []
        val_list = []
        for each_variable in _identify_variable(self.MultipleRun_params):
            column_list.append(each_variable)
            val_list.append(
                _extract_variable(param_dict,
                                  each_variable))
        return pd.DataFrame([val_list], columns=column_list, index=[0])


class HyperparameterSelectionTask(MultipleRunBase):

    ''' Hyperparameter selection task.
    '''

    lower_better = luigi.BoolParameter(default=True)

    def run_task(self, input_list):
        best_params = None
        best_score = None
        if self.lower_better:
            best_score = float('inf')
        else:
            best_score = -float('inf')

        for each_idx, each_param_dict in enumerate(self.param_dict_generator()):
            val_score = input_list[each_idx]
            if isinstance(val_score, tuple):
                val_score = val_score[0]
            if self.lower_better:
                if val_score < best_score:
                    best_params = each_param_dict
                    best_score = val_score
            else:
                if val_score > best_score:
                    best_params = each_param_dict
                    best_score = val_score
        logger.info(' * best score is {}'.format(best_score))
        return best_score, best_params


class LinePlotMultipleRun(AutoNamingTask):

    ''' Line plot of the result of `MultipleRun`.
    Shared x-axis, multiple lines are supported.
    The parameter will be like:

PlotTestLoss_params = {
    'x': ('DataGeneration_params', 'train_sample_size'),
    'plot_config_list': [{'extract_list': [(('Train_params', 'model_name'), 'SigmoidPOSNN')]},
                         {'extract_list': [(('Train_params', 'model_name'), 'SigmoidDiffSNN')]}],
    'fig_config': {'xlabel': {'xlabel': r'\# of training examples'},
                   'ylabel': {'ylabel': 'ELBO'},
                   'legend': {'labels': ['SNN', r'$\partial$SNN']}}
}
    '''

    output_ext = luigi.Parameter(default='pdf')
    MultipleRun_params = luigi.DictParameter()
    LinePlotMultipleRun_params = luigi.DictParameter()

    def run_task(self, input_list):
        import matplotlib
        matplotlib.rcParams['pdf.fonttype'] = 42
        matplotlib.rcParams['ps.fonttype'] = 42
        #matplotlib.rcParams['ps.useafm'] = True
        #matplotlib.rcParams['pdf.use14corefonts'] = True
        #matplotlib.rcParams['text.usetex'] = True
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        res_df = input_list[0]

        fig, ax = plt.subplots()
        for each_plot_config in self.LinePlotMultipleRun_params['plot_config_list']:
            # each_plot_config is a dict containing key 'extract_list'
            _res_df = deepcopy(res_df)
            if each_plot_config.get('extract_list', False):
                for each_extract_rule in each_plot_config['extract_list']:
                    # each_extract_rule is a tuple of a column name of res_df and its value.
                    # each rule extracts entries that has
                    # the specified value in the specified column.
                    _res_df = _res_df[_res_df[each_extract_rule[0]] == each_extract_rule[1]]
                _res_df.plot(x=self.LinePlotMultipleRun_params['x'],
                             y=each_plot_config['col_name'],
                             ax=ax,
                             **each_plot_config.get('plot_kwargs', {}))
                if 'yerr_col_name' in each_plot_config:
                    if each_plot_config['yerr_col_name'] in _res_df.columns:
                        ax.fill_between(_res_df[self.LinePlotMultipleRun_params['x']],
                                        _res_df[each_plot_config['col_name']] - _res_df[each_plot_config['yerr_col_name']],
                                        _res_df[each_plot_config['col_name']] + _res_df[each_plot_config['yerr_col_name']],
                                        alpha=0.35,
                                        label='_nolegend_')
            elif each_plot_config.get('col_name', False):
                extract_col_list = [self.LinePlotMultipleRun_params['x'],
                                    each_plot_config['col_name']]
                if 'yerr_col_name' in each_plot_config:
                    extract_col_list.append(each_plot_config['yerr_col_name'])
                _res_df = _res_df[extract_col_list].dropna(axis=1, how='all')
                _res_df = _res_df.dropna(axis=0, how='any')
                _res_df.plot(x=self.LinePlotMultipleRun_params['x'],
                             y=each_plot_config['col_name'],
                             ax=ax,
                             #yerr=each_plot_config.get('yerr_col_name', None),
                             **each_plot_config.get('plot_kwargs', {}))
                color = each_plot_config['plot_kwargs']['color']
                if 'yerr_col_name' in each_plot_config:
                    if each_plot_config['yerr_col_name'] in _res_df.columns:
                        ax.fill_between(_res_df[self.LinePlotMultipleRun_params['x']],
                                        _res_df[each_plot_config['col_name']] - _res_df[each_plot_config['yerr_col_name']],
                                        _res_df[each_plot_config['col_name']] + _res_df[each_plot_config['yerr_col_name']],
                                        alpha=0.35,
                                        label='_nolegend_',
                                        color=color)
            else:
                raise NotImplementedError
        ax.set_xlabel(**self.LinePlotMultipleRun_params['fig_config']['xlabel'])
        ax.set_ylabel(**self.LinePlotMultipleRun_params['fig_config']['ylabel'])
        ax.legend(**self.LinePlotMultipleRun_params['fig_config']['legend'])
        return ax.get_figure()

    def save_output(self, res):
        res.savefig(self.output().path, bbox_inches='tight', pad_inches=0.0)
