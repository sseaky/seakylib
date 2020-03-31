#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/12 10:46

import re
import shutil
import socket
import traceback
from pathlib import Path

import psutil


def get_hostname():
    return socket.gethostname()


def get_if(name=None):
    d = psutil.net_if_addrs()
    if name:
        return {k: v for k, v in d.items() if k.startswith(name)}
    return d


def get_pwd():
    '''返回程序所在目录'''
    return get_caller().parent


def get_caller(top=True):
    '''返回用户调用函数，过滤lib'''
    es = traceback.extract_stack()
    l = es if top else es[::-1]
    for x in l:
        if not re.search('(seakylib|pydev|pycharm)', x.filename):
            return Path(x.filename)


def get_term_size():
    width, depth = shutil.get_terminal_size((80, 20))
    return width, depth
