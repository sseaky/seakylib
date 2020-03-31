#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/26 11:38

# pip install concurrent-log-handler

import logging
import logging.config
from copy import deepcopy
from pathlib import Path

from ..os.info import get_pwd, get_caller


def make_logger(name=None, stem=None, log_dir='log', work_dir=None, console=True, write=True,
                focus_error=True, multi_process=True,
                level='INFO', **kwargs):
    '''
    :param name: logger name
    :param stem: file stem, default filename without type
    :param log_dir:
    :param work_dir:
    :param console:
    :param write:
    :param focus_error:     增加error日志
    :param multi_process:   支持多进程
    :param level:
    :param kwargs:
        {'handlers': {...}}
    :return:
    '''
    work_dir = work_dir or get_pwd()
    caller = get_caller()
    dpath = Path(work_dir) / log_dir
    dpath.mkdir(exist_ok=True)
    stem = stem or caller.stem
    name = name or caller.name

    # concurrent_log_handler.ConcurrentRotatingFileHandler 支持多进程，但不能做时间分割
    # logging.handlers.RotatingFileHandler 只支持多线程
    file_handler = {
        'level': 'DEBUG',
        'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler' if multi_process else 'logging.handlers.RotatingFileHandler',
        'maxBytes': 1024 * 1024 * 10,
        'backupCount': 10,
        # If delay is true,
        # then file opening is deferred until the first call to emit().
        'delay': True,
        'filename': str(dpath / '{0}.log'.format(stem)),
        'formatter': 'verbose'
    }

    d = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'verbose': {
                'format': "[%(asctime)s] [%(filename)s:%(lineno)s] %(levelname)s %(message)s",
                'datefmt': "%Y-%m-%d %H:%M:%S"
            },
            'simple': {
                'format': '%(levelname)s %(message)s'
            },
        },
        'handlers': {
            'null': {
                'level': 'DEBUG',
                'class': 'logging.NullHandler',
            },
        },
    }

    if console:
        d['handlers']['console'] = {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        }
    if write:
        d['handlers']['file'] = file_handler
        if focus_error:
            file_handler_error = deepcopy(file_handler)
            file_handler_error.update({'level': 'WARN', 'filename': str(dpath / '{0}_error.log'.format(stem))})
            d['handlers']['file_error'] = file_handler_error
    if 'handlers' in kwargs:
        d['handlers'].update(kwargs['handlers'])

    d['loggers'] = {name: {'handlers': d['handlers'].keys(),
                           'level': level, }}

    logging.config.dictConfig(d)
    logger = logging.getLogger(name)
    return logger
