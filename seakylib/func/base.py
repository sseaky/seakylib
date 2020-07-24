#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/14 12:05


import logging
import re
import time
import traceback
from collections import OrderedDict
from functools import wraps
from inspect import ismethod
from pathlib import Path
from pprint import pprint

from .log import make_logger
from .string import arg2list, str2list
from ..os.info import get_pwd
from ..os.oper import dump_data, load_data


def inspect_object(obj):
    '''
    检查object
    :param obj:
    :return:
    '''
    print('inspect {}'.format(obj))
    for x in dir(obj):
        sub = getattr(obj, x)
        if hasattr(sub, '__call__'):
            tp = 'func'
            if x.startswith('__'):
                try:
                    r = sub()
                except Exception as e:
                    r = e
            else:
                r = ''
            l = [tp, x, r]
        else:
            tp = 'attr'
            l = [tp, x, sub]
        print('{}  {:<10}  {}'.format(*l))


def get_obj_name(obj):
    '''
    返回 object 的名字
    :param obj:
    :return:
    '''
    if not hasattr(obj, '__name__'):
        return str(obj)
    name = obj.__name__
    if hasattr(obj, '__self__'):
        if hasattr(obj.__self__, '__class__'):
            name = '{}.{}'.format(obj.__self__.__class__.__name__, name)
        else:
            name = '{}.{}'.format(obj.__self__.__module__, name)
    return name


def current_function(skip=None):
    '''
    获取调用函数的名字
    :param skip:
    :return:
    '''
    skip = arg2list(skip)
    for x in traceback.extract_stack()[::-1]:
        if not re.search('^(\<|func_done|current_function)', x.name) and x.name not in skip:
            break
    return x.name
    # return sys._getframe().f_back.f_code.co_name


def func_done(name=None, cache=None, flag=True):
    '''
    :param name:    自定义函数名
    :param cache:   记录函数是否已被运行
    :param flag:    运行结果
    :return:
    '''
    already = False
    name = name or current_function()
    if isinstance(cache, dict):
        if cache.get(name) == flag:
            already = True
        else:
            cache[name] = flag
    return 'func {} is {}done.'.format(name, 'already ' if already else '')


def count_time(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        '''
        如果是普通函数，可以附加_show_timer参数控制是否显示计时结果，不会真正传入f；
        如果是in_class，还会被self.debug影响
        :param args:
        :param kwargs:
            _show_timer
        :return:
        '''
        _show_timer = kwargs.pop('_show_timer') if '_show_timer' in kwargs else False
        in_class = False
        if len(args) > 0:
            if is_instance(args[0]):
                self = args[0]
                in_class = True
        start_time = time.time()
        result = f(*args, **kwargs)
        elapsed_time = round(time.time() - start_time, 2)
        d = {'function': f.__name__, 'time': elapsed_time}
        msg = 'function: {function}, timer: {time:.2f}s'.format(**d)
        if in_class:
            # 放到self.debug_info['timer']中
            self.debug_info['timer'][f.__name__] = elapsed_time
            _show_timer = _show_timer or self.debug
            if _show_timer:
                self.log.debug(msg)
        else:
            if _show_timer:
                print(msg)
        return result

    return wrap


def is_instance(obj):
    '''对象是一个实例'''
    if hasattr(obj, '__dict__') and not callable(obj):
        return True


def catch_exception(ignore=False, retry=0, prune_func=None):
    '''
    :param ignore: 不记录错误, 尝试性的exception
    :param retry: 重试次数
    :param prune_func: 过滤一些不重要的log
    :return:
    如果 kwargs中含有no_catch_exception，忽略本catch
    如果是普通函数，可以附加 _show_exception 参数控制是否显示具体异常，不会真正传入f；
        如果是in_class，还会被self.traceback影响
    '''

    def deco(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            no_catch_exception = kwargs.pop('no_catch_exception') if 'no_catch_exception' in kwargs else False
            if no_catch_exception:
                return f(*args, **kwargs)
            _show_exception = kwargs.pop('_show_exception') if '_show_exception' in kwargs else False
            # 找出instance
            if f.__name__ == 'run_func':
                real_func = args[0][0] if isinstance(args[0], (tuple, list)) else args[0]
            else:
                real_func = f
            if ismethod(real_func):
                instance = real_func.__self__
            elif len(args) > 0 and hasattr(args[0], '__dict__'):
                instance = args[0]
            else:
                instance = None
            i = 0
            while i <= retry:
                try:
                    excep = False
                    excep_tp = None
                    result = f(*args, **kwargs)
                    break
                except AssertionError as e:
                    # e不能传递出try
                    excep_tp = 'assert'
                    excep, e1, trace = True, e, '{}'.format(traceback.format_exc())
                except Exception as e:
                    excep, e1, trace = True, e, '{}'.format(traceback.format_exc())
                i += 1
            if not excep:
                return result
            else:
                d = {'function': real_func.__name__, 'message': '{}'.format(e1), 'except': e1,
                     'trace': trace, 'ignore': ignore}
                msg = 'function: {function}, exception: {message}, ignore: {ignore}\ntrace: {trace}'.format(**d)
                if instance and hasattr(instance, 'debug_info'):
                    # 过滤重复的异常
                    if len(instance.debug_info['exception']) == 0 or \
                            instance.debug_info['exception'][-1]['message'] != d['message']:
                        instance.debug_info['exception'].append(d)
                        _show_exception = _show_exception or instance.debug
                        if _show_exception and not ignore and excep_tp != 'assert':
                            if not prune_func or not prune_func(msg):
                                instance.log.error(msg)
                    if instance.debug:
                        e1 = trace
                else:
                    if _show_exception and not ignore and excep_tp != 'assert':
                        if not prune_func or not prune_func(msg):
                            print(msg)
                return False, '{}'.format(e1)

        return wrap

    return deco


@catch_exception()
def run_func(obj):
    '''
    运行传入的函数，func/(func, (...))/(func, {...}), kwargs也会被传递给func
    :param obj:
    :return:
    '''
    if obj is None:
        return True, 'func is None.'
    elif isinstance(obj, tuple):
        if len(obj) == 2:
            func, args = obj
            if isinstance(args, tuple):
                return func(*args)
            elif isinstance(args, dict):
                return func(**args)
            else:
                return func(args)
        else:
            func, *args = obj
            return func(*args)
    elif hasattr(obj, '__call__'):
        return obj()
    return False, '{} can not be called'.format(obj)


# @catch_exception()
def run_functions(*funcs, message=None, order='and', watchdog=None, **kwargs):
    '''
    funcs见run_func, func.__self__可以获取instance
    如果是and，所有func返回True，最后返回True
    如果是or，任意func返回True即返回True，否则返回False
    watchdog: 额外判断函数
    '''
    fs_name = []
    for func in funcs:
        if func is None:
            continue
        real_func = func[0] if isinstance(func, tuple) else func
        func_name = get_obj_name(real_func)
        fs_name.append(func_name)
        is_ok, _message = run_func(func)
        if watchdog:
            x, y = run_func(watchdog)
            if not x:
                is_ok, _message = x, '{} ({}) after {}'.format(_message, y, func)
        if order == 'and':
            if not is_ok:
                return is_ok, _message
        elif order == 'or':
            if is_ok:
                return is_ok, _message
    # 如果是and，跳出循环表明都执行成功；如果是or，跳出循环表明都执行失败
    if order == 'and':
        return True, message if message else 'run funcs {} successfully.'.format(fs_name)
    elif order == 'or':
        return False, message if message else 'run funcs {} fail.'.format(fs_name)


class MyClass:
    def __init__(self, log=None, log_params=None, *args, **kwargs):
        '''
        :param args:
        :param log:    customer log
        :param make_logger的参数
            quite
            debug   logging.debug, catch_exception显示trace信息
        :param kwargs:
            path_output
            path_temp
            show_tick  显示count_time的值
        '''
        self.kwargs = kwargs
        self.pwd = get_pwd()
        self.path_output = self.pwd / Path(kwargs.get('path_output', 'output'))
        self.path_temp = self.pwd / Path(kwargs.get('path_temp', 'temp'))
        # 默认log是打印到console
        self.quite = kwargs.get('quite')
        # cache存放过程数据, 最好不要放入无法dump的数据
        self.verbose = kwargs.get('verbose')
        self.debug = kwargs.get('debug')
        # self.traceback = kwargs.get('traceback')
        self.db_session = kwargs.get('db_session')
        self.debug_info = {'error': [], 'timer': OrderedDict(), 'exception': [], 'warn': [], 'func_done': OrderedDict()}
        self.cache = {'message': ''}
        self.control = {}

        if isinstance(log, logging.Logger):
            self.log = log
        else:
            if not log_params:
                log_params = {}
            log_params.update(
                {'debug': self.debug, 'console': not self.quite, 'name': self.__class__.__name__, 'simple_log': False})
            # 两个类建立默认log时，前者会没有输出？
            self.log = make_logger(**log_params)

    def default(self, k):
        return self.control.get(k)

    def func_done(self, name=None, cache=None, flag=True):
        '''
        记录函数是否运行
        :param name:
        :param cache:
        :param flag:
        :return:
        '''
        if not cache:
            cache = self.debug_info['func_done']
        return func_done(name=name, cache=cache, flag=flag)

    def been_run(self):
        '''
        检查函数是否被运行过
        :return:
        '''
        name = current_function(skip=[current_function()])
        return self.debug_info['func_done'].get(name)

    def show_verbose(self, *obj):
        self.show_by_flag(self.verbose, *obj)

    def show_debug(self, *obj):
        self.show_by_flag(self.debug, *obj)

    @staticmethod
    def show_by_flag(flag, *obj):
        if flag:
            for o in obj:
                pprint(o) if isinstance(o, (list, dict)) else print(o)

    def log_error(self, s, skip=None):
        '''在这里记录手工错误'''
        if not skip:
            skip = []
        skip.append('log_error')
        d = {'function': current_function(skip=skip), 'message': s}
        self.debug_info['error'].append(d)
        self.log.error('function: {function}, error: {message}'.format(**d))

    def log_warn(self, s, skip=None):
        '''在这里记录手工警告'''
        if not skip:
            skip = []
        skip.append('log_warn')
        d = {'function': current_function(skip=skip), 'message': s}
        self.debug_info['warn'].append(d)
        self.log.warn('function: {function}, warn: {message}'.format(**d))


def myclass_precheck(have_run=None, check_func=None, retry=False):
    '''
    myclass的装饰器
    :param have_run:    检查内部函数是否被执行
    :param check_func:  用于执行检查的函数
    :param retry:  如果前置函数已经失败，是否重新执行
    :return:
    '''

    def deco(f):
        @wraps(f)
        def wrap(self, *args, **kwargs):
            d = self.debug_info['func_done']
            for func_name in str2list(have_run):
                if func_name not in d or (d[func_name] is False and retry):
                    if hasattr(self, func_name):
                        is_ok, result = getattr(self, func_name)()
                        if func_name not in d:
                            d[func_name] = is_ok
                        if is_ok:
                            continue
                        else:
                            return False, 'deco: function {}. {}'.format(func_name, result)
                    return False, 'deco: function {} is not exist.'.format(func_done())
            for func_name in str2list(check_func):
                if hasattr(self, func_name):
                    is_ok, result = getattr(self, func_name)()
                    if not is_ok:
                        return False, 'deco: function {}. {}'.format(func_name, result)

            return f(self, *args, **kwargs)

        return wrap

    return deco


# def a(func):
#     if func
#
# # save load需要编写, 最好兼容装饰器
# def sl(obj, filename, func=None, save=True, load=True, store_dir='temp', work_dir=None, mode='json'):
#     work_dir = work_dir or get_pwd()
#     dpath = Path(work_dir) / store_dir
#     fpath = dpath / os.fsdecode(filename)
#     if load and fpath.exists():
#         data = json.load(path_open(fpath))
#         return True, data
#     self.kwargs['os'] = self.data_in_db['device']['os']
#     is_ok, result = self.inspect(method='snmp' if self.os == 'comware' else 'cli')
#     assert is_ok, result
#     self.cache['inspect_done'] = True
#     if self.kwargs.get('save') and is_ok:
#         json.dump(self.info, path_open(path_data, 'w'))
#     self.data_new = self.info
#     return is_ok, result


def deco_result_sl(deco_save=False, deco_load=False, ident_key=None, mark=None, func_dump=None, func_load=None):
    '''
    保存成 <store_dir>/[mark_]<func_name>[_ident_key]，一些特殊的对象无法保存
    :param deco_save:
    :param deco_load:
    :param ident_key:
    :param mark:
    :param func_dump:   dump前处理
    :param func_load:   load后处理
    :return:
    '''

    def deco(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            filename = f.__name__
            if ident_key and ident_key in kwargs:
                filename += '_{}={}'.format(ident_key, kwargs[ident_key])
            if mark:
                filename = '{}_{}'.format(mark, filename)
            else:
                if 'mark' in kwargs:
                    filename = '{}_{}'.format(kwargs['mark'], filename)
            filename += '.result'
            filename = filename.replace('/', '-')
            in_class = False
            if len(args) > 0:
                if is_instance(args[0]):
                    self = args[0]
                    in_class = True
                    if hasattr(self, 'path_temp'):
                        filename = Path(self.path_temp) / filename
            if deco_load and filename.is_file():
                r, d = load_data(filename)
                if r:
                    return func_load(d['result']) if func_load else d['result']
                else:
                    return
            result = f(*args, **kwargs)
            if deco_save:
                r, d = dump_data({'result': func_dump(result) if func_dump else result,
                                  'func': f.__name__, 'args': str(args), 'kwargs': str(kwargs)}, filename)
            return result

        return wrap

    return deco
