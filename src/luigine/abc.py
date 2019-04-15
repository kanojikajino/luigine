#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Title """

__author__ = "Hiroshi Kajino <KAJINO@jp.ibm.com>"
__copyright__ = "(c) Copyright IBM Corp. 2019"
__version__ = "1.0"
__date__ = "Apr 15, 2019"

from copy import deepcopy
from collections import OrderedDict
from datetime import datetime
import errno
import hashlib
import os
import luigi
from .utils import sort_dict, dict_to_str


@luigi.Task.event_handler(luigi.Event.FAILURE)
@luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
def curse_failure(*kwargs):
    if os.path.exists("engine_status.progress"):
        os.rename("engine_status.progress", "engine_status.error")
    with open("engine_status.error", "a") as f:
        f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    with open(os.path.join("ENGLOG", "engine.log"), "a") as f:
        f.write("{}".format(kwargs))
    raise RuntimeError('something happens')


def main():
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
    if not os.path.exists(os.path.join("ENGLOG")): os.mkdir(os.path.join("ENGLOG"))
    if not os.path.exists(os.path.join("OUTPUT")): os.mkdir(os.path.join("OUTPUT"))
    
    os.rename("engine_status.ready", "engine_status.progress")
    with open("engine_status.progress", "a") as f:
        f.write("progress: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
    
    # run
    try:
        luigi.run(local_scheduler=True)
    except:
        import traceback
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.error")
        with open("engine_status.error", "a") as f:
            f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        with open(os.path.join("ENGLOG", "engine.log"), "a") as f:
            f.write(traceback.format_exc())


class MainTask(luigi.Task):

    '''
    A main task should inherit this class.
    '''

    working_dir = luigi.Parameter()
    
    def on_success(self):
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.complete")
            with open("engine_status.complete", "a") as f:
                f.write("complete: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        elif os.path.exists("engine_status.error"):
            with open("engine_status.error", "a") as f:
                f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        else:
            raise RuntimeError("unknown engine_status")

    def on_failure(self, exception):
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.error")
        if not os.path.exists("engine_status.error"):
            raise RuntimeError("unknown engine_status")
        with open("engine_status.error", "a") as f:
            f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        super().on_failure(exception)


class MainWrapperTask(luigi.WrapperTask):

    '''
    A main wrapper task should inherit this class.
    '''

    working_dir = luigi.Parameter()

    def on_success(self):
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.complete")
            with open("engine_status.complete", "a") as f:
                f.write("complete: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        elif os.path.exists("engine_status.error"):
            with open("engine_status.error", "a") as f:
                f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        else:
            raise RuntimeError("unknown engine_status")

    def on_failure(self, exception):
        if os.path.exists("engine_status.progress"):
            os.rename("engine_status.progress", "engine_status.error")
        if not os.path.exists("engine_status.error"):
            raise RuntimeError("unknown engine_status")
        with open("engine_status.error", "a") as f:
            f.write("error: {}\n".format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
        super().on_failure(exception)


class AutoNamingTask(luigi.Task):

    '''
    This task defines the output name automatically from task parameters.
    '''

    __no_hash_keys__ = []
    hash_num = luigi.IntParameter(default=10)
    working_subdir = luigi.Parameter()
    output_ext = luigi.Parameter(default='pklz')
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.param_name = ""
        param_kwargs = deepcopy(self.__dict__["param_kwargs"])
        if "working_subdir" in param_kwargs: param_kwargs.pop("working_subdir")
        for each_key in self.__no_hash_keys__:
            if len(each_key) == 2:
                self.param_name = self.param_name + str(param_kwargs[each_key[0]][each_key[1]]) + "_"
                param_kwargs[each_key[0]] = dict(param_kwargs[each_key[0]])
                param_kwargs[each_key[0]].pop(each_key[1])
            else:
                self.param_name = self.param_name + str(param_kwargs[each_key]) + "_"
                param_kwargs.pop(each_key)
        for each_key in sorted(param_kwargs.keys()):
            if isinstance(param_kwargs[each_key], (dict, OrderedDict, luigi.parameter._FrozenOrderedDict)):
                self.param_name \
                    = self.param_name \
                    + hashlib.md5(dict_to_str(sort_dict(param_kwargs[each_key]))\
                                  .encode("utf-8")).hexdigest()[:self.hash_num] + "_"
            else:
                self.param_name = self.param_name \
                                  + hashlib.md5(str(param_kwargs[each_key]).encode("utf-8")).hexdigest()[:self.hash_num] + "_"
        self.param_name = self.param_name[:-1]

    def output(self):
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
