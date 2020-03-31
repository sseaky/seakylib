#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/11/12 20:26

import argparse
import re
import sys
from configparser import ConfigParser

from ..os.oper import path_open


class CfgParser(ConfigParser):
    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs)

    def write(self, f, **kwargs):
        ConfigParser.write(self, path_open(f, 'w'), **kwargs)


class ArgParseClass:
    def __init__(self, description='', group=None):
        '''
        :param description:
        :param group: add_all()中优先于__init__
        '''
        self.parser = argparse.ArgumentParser(description=description)
        self.group = group
        self.groups = {}
        if group:
            self.groups[group] = self.parser.add_argument_group(title=group)
        self.opts = []
        # require 强制，hidden 隐藏arg, forbidden 禁止某些arg。
        # 这些列表是为继承时检查，都不需要加--
        self.require = []
        self.forbidden = []
        self.hidden = []

    def add_all(self):
        # 上层可以按需重写 add_all
        pass

    def add_base(self, group='Base'):
        self.add('--verbose', action='store_true', help='显示详细信息', group=group)
        self.add('--debug', action='store_true', help='显示debug信息', group=group)
        # self.add('--traceback', action='store_true', help='show traceback info', group=group)

    def add_notify(self, group='Notify'):
        self.add('--yx', action='store_true', help='yixin', group=group)
        self.add('--popo', action='store_true', help='popo', group=group)
        self.add('--sms', action='store_true', help='sms', group=group)

    def add(self, *args, **kwargs):
        opt = args[0]
        # 隐藏参数
        opt_str = re.sub('^-+', '', opt)
        if opt_str in self.hidden or opt_str in self.forbidden:
            return
        if opt in self.opts:
            return
        title = kwargs.get('group') or self.group
        if title:
            if title not in self.groups:
                self.groups[title] = self.parser.add_argument_group(title=title)
            parser = self.groups[title]
        else:
            parser = self.parser
        if 'group' in kwargs:
            del kwargs['group']
        self.opts.append(opt)
        parser.add_argument(*args, **kwargs)

    def parse(self):
        args = self.parser.parse_args()
        self.args = self.after_parse(args)
        return self.args

    def after_parse(self, args):
        # 对参数可以进行一些处理
        return args

    def dict(self):
        return self.parse().__dict__

    def check_args(self, relation='and'):
        '''
        检查require和forbidden列表
        :param relation:   and/or   参数满足关系
        :return:
        '''
        cmd = ' '.join(sys.argv)
        if '--help' in cmd:
            return
        found = False
        for x in self.require:
            if not re.search(' -+{}'.format(x), cmd):
                if relation == 'and':
                    self.parser.print_help()
                    print('\narg "--{}" is required !!!\n'.format(x))
                    exit(1)
            else:
                found = True
        if relation == 'or' and not found:
            self.parser.print_help()
            print('\nat least one of "{}" is required !!!\n'.format('/'.join(['--{}'.format(x) for x in self.require])))
            exit(1)
        for x in sys.argv[1:]:
            if x.startswith('--') and re.sub('^-+', '', x) in self.forbidden:
                self.parser.print_help()
                print('\narg "{}" is forbidden !!!\n'.format(x))
                exit(1)
