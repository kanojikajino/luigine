#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Hiroshi Kajino"
__copyright__ = "(c) Copyright IBM Corp. 2019"

from copy import deepcopy
from collections import OrderedDict
import hashlib
import luigi


def dict_to_str(input_dict, hash_str=False):
    if hash_str:
        return hashlib.md5('_'.join(['%s=%s' % (k, input_dict[k])
                                     for k in sorted(input_dict.keys())]).encode("utf-8")).hexdigest()
    else:
        return '_'.join(['%s=%s' % (k, input_dict[k]) for k in sorted(input_dict.keys())])


def sort_dict(input_dict):
    """ this function converts a dictionary into an OrderedDict, where the keys are sorted.
    
    Parameters
    ----------
    input_dict : dict
        `input_dict` may contain another dictionary in it. this function recursively sorts the keys.
    
    Returns
    -------
    sorted_dict : OrderedDict
        dictionary whose keys are sorted.
    """
    if isinstance(input_dict, (tuple, list)):
        return input_dict
    _input_dict = deepcopy(dict(input_dict))
    for each_key, each_value in _input_dict.items():
        if isinstance(each_value, (dict, OrderedDict, luigi.freezing.FrozenOrderedDict)):
            _input_dict[each_key] = sort_dict(_input_dict[each_key])
    if isinstance(_input_dict, (dict, OrderedDict, luigi.freezing.FrozenOrderedDict)):
        return OrderedDict(sorted(_input_dict.items(), key=lambda t: t[0]))
    else:
        return _input_dict


def checksum(file_path):
    ''' compute md5checksum of the file

    Parameters
    ----------
    file_path : str
        a path to the file whose md5checksum is computed

    Returns
    -------
    str : md5checksum
    '''
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096 * md5.block_size), b''):
            md5.update(chunk)
    return md5.hexdigest()
