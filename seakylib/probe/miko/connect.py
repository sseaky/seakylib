#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/13 10:02

import re
import warnings
from functools import wraps

from netmiko import Netmiko

from . import IbmnosTelnet
from ...func.base import MyClass, catch_exception, count_time
from ...func.parser import ArgParseClass
from ...func.string import replace

warnings.filterwarnings("ignore")


class BaseDevice(MyClass):
    def __init__(self, ip, passwords, os=None, device_type=None, ssh_enable=True, telnet_enable=False,
                 timeout=None, blocking_timeout=None, *args, **kwargs):
        '''
        :param args:
        :param kwargs:
            ip
            port:   optional, default 22
            os: os或device_type设置其一
            passwords   [{'username':, 'password':, 'secret':}, {}, ...]
            timeout:    optional, default 3
            blocking_timeout
            device_type
                specific device_type for cli
            ssh_enable: enable telnet login, default True
            telnet_enable: enable telnet login, default False
        '''
        MyClass.__init__(self, *args, **kwargs)
        self.ip = ip
        self.os = os
        self.passwords = [passwords] if isinstance(passwords, dict) else passwords
        self.device_type = device_type
        self.ssh_enable = ssh_enable
        self.telnet_enable = telnet_enable
        self.timeout = timeout or 10
        self.blocking_timeout = blocking_timeout or 10

    @catch_exception(ignore=True)
    def login(self, auth):
        '''
        :param auth:
            username, password, secret
        对于dell_dnos9， 有时会报 'Oops, unhandled type' 错误，
        根据 https://github.com/ktbyers/netmiko/issues/373，设置 use_keys=True, allow_agent=True，运行正常
        '''
        # paramiko.transport may except 'Oops, unhandled type' warning. (dell_dnos9 on 10.60.5.16)
        # https://github.com/ktbyers/netmiko/issues/373
        if auth['device_type'] == 'ibmnos_telnet':
            net_connect = IbmnosTelnet(**auth)
        else:
            # timeout, blocking_timeout
            if re.search('dnos', auth['device_type']) and 'use_keys' not in auth:
                extra = dict(use_keys=True, allow_agent=True)
            else:
                extra = {}
            net_connect = Netmiko(**auth, **extra, timeout=self.kwargs.get('timeout', 20),
                                  blocking_timeout=self.kwargs.get('cli_blocking_timeout', 20),
                                  )
        self.session = net_connect
        self.cache['cli_protocol'] = auth['device_type']
        if not self.device_type:
            self.device_type = auth['device_type']
        return True, 'connected.'

    def set_device_types(self):
        if self.device_type:
            device_types = self.device_type if isinstance(self.device_type, list) else [
                self.device_type]
        else:
            device_types = []
            if self.os in ['ftos', 'dnos']:
                if self.ssh_enable:
                    device_types.append('dell_dnos9')
                if self.telnet_enable:
                    device_types.append('dell_dnos6_telnet')
            elif self.os in ['comware']:
                if self.ssh_enable:
                    device_types.append('hp_comware')
                if self.telnet_enable:
                    device_types.append('hp_comware_telnet')
            elif self.os in ['nxos']:
                if self.ssh_enable:
                    device_types.append('cisco_nxos')
                if self.telnet_enable:
                    device_types.append('cisco_ios_telnet')
            elif self.os in ['iosxr']:
                if self.ssh_enable:
                    device_types.append('cisco_xr')
                if self.telnet_enable:
                    device_types.append('cisco_xr_telnet')
            elif self.os in ['iosxe']:
                if self.ssh_enable:
                    device_types.append('cisco_xr')
                if self.telnet_enable:
                    device_types.append('cisco_xr_telnet')
            elif self.os in ['ios']:
                if self.ssh_enable:
                    device_types.append('cisco_ios')
                if self.telnet_enable:
                    device_types.append('cisco_ios_telnet')
            elif self.os in ['vrp']:
                if self.ssh_enable:
                    device_types.append('huawei_vrpv8')
                if self.telnet_enable:
                    device_types.append('huawei_telnet')
            elif self.os in ['powerconnect']:
                if self.ssh_enable:
                    device_types.append('dell_powerconnect')
                if self.telnet_enable:
                    device_types.append('dell_powerconnect_telnet')
            elif self.os in ['ibmnos']:
                if self.telnet_enable:
                    device_types.append('ibmnos_telnet')
            elif self.os in ['junos']:
                if self.ssh_enable:
                    device_types.append('juniper_junos')
                if self.telnet_enable:
                    device_types.append('juniper_junos_telnet')
        return device_types

    @catch_exception()
    @count_time
    def get_session(self):
        auth = {'host': self.ip}
        if not self.cache.get('cli_false'):
            if not hasattr(self, 'session'):
                device_types = self.set_device_types()
                if not device_types:
                    return False, 'no device type for {} cli.'.format(self.os)
                for device_type in device_types[:]:
                    for pw in self.passwords[:]:
                        auth.update(pw)
                        auth['device_type'] = device_type
                        is_ok, result = self.login(auth)
                        self.cache['login_result'] = result
                        if is_ok:
                            if self.os in ['ibmnos']:
                                self.send_command('terminal-length 0')
                            elif self.os in ['powerconnect']:
                                self.send_command('terminal length 0')
                            self.cache['auth'] = auth
                            return is_ok, result
                        else:
                            if re.search('(Authentication|login)', result, re.I):
                                continue
                            if re.search(r'(time[d -]*out)', result, re.I):
                                break
            else:
                return True, 'cli connection exist.'

        self.cache['cli_false'] = True
        return False, 'login false.'

    # @staticmethod
    def check_cli(self, f):
        def wrapper(*args, **kwargs):
            is_ok, connection = self.get_session()
            if is_ok:
                return f(*args, **kwargs)
            return False, self.cache.get('login_result') or 'no cli connection'

        return wrapper

    @catch_exception()
    def send_command(self, commands, show_cmd=False, one_line_cmd=False, timeout_offset=None, **kwargs):
        '''
        :param commands:
        :param show_cmd:
        :param one_line_cmd:
        :param timeout_offset: 临时补偿超时时间，依赖于session.timeout
        :param kwargs:
        :return:
        '''

        @self.check_cli
        def wrap(commands):
            if isinstance(commands, str):
                commands = commands.split('\n')
            output = []
            if one_line_cmd:
                commands = ['\n'.join(commands)]
            for cmd in commands:
                if not cmd.strip():
                    continue
                if show_cmd:
                    output.append('>>>>> {} <<<<<\n'.format(cmd))
                flag_change_timeout = False
                if isinstance(timeout_offset, int):
                    _to, self.session.timeout = self.session.timeout, self.session.timeout + timeout_offset
                    flag_change_timeout = True
                output.append('{}'.format(self.session.send_command(cmd, **kwargs)))
                if flag_change_timeout:
                    self.session.timeout = _to
            return True, '\n'.join(output)

        return wrap(commands)

    @catch_exception()
    def display(self, commands, func=None, func_kw=None, tidy=True, show_cmd=True, one_line_cmd=False,
                timeout_offset=None, **kwargs):
        '''

        :param commands:
        :param func: 回调结果处理函数
        :param func_kw:
        :param tidy: 删除重复的换行
        :param show_cmd: 结果中显示命令
        :param one_line_cmd: 不拆分命令
        :param timeout_offset:
        :return:
        '''
        is_ok, result = self.send_command(commands, show_cmd=show_cmd, one_line_cmd=one_line_cmd,
                                          timeout_offset=timeout_offset,
                                          **kwargs)
        assert is_ok, result
        if tidy:
            result = replace(result)
        if func:
            if not func_kw:
                func_kw = {}
            return is_ok, func(result, **func_kw)
        return is_ok, result

    @catch_exception()
    def enable(self, *args, **kwargs):
        '''# 进入enable模式, netmiko 2.3.3中，base_connection.py在调用check_enable_mode时没有带参数，始终返回True，不能用
        '''

        @self.check_cli
        def wrap(*args, **kwargs):
            return self.session.enable(*args, **kwargs)

        return True, wrap(*args, **kwargs)


def try_login(device_types, passwords):
    def deco(f):
        @wraps(f)
        def wrapper(inst, auth):
            is_ok, result = False, 'init'
            for device_type in device_types:
                for pw in passwords:
                    auth.update(pw)
                    auth['device_type'] = device_type
                    is_ok, result = f(inst, auth)
                    # break when success or timeout. Except 'broken pipe' when auth fail on ftos.
                    if is_ok or (isinstance(result, str) and re.search('(timed out|connection refused)', result, re.I)):
                        break
                if is_ok:
                    break
            if is_ok:
                inst.cache['protocol'] = device_type
            return is_ok, result

        return wrapper

    return deco


class MikoArgParse(ArgParseClass):
    def __init__(self, *args, **kwargs):
        ArgParseClass.__init__(self, *args, **kwargs)

    def add_miko(self, group='MIKO Args'):
        self.add('--ip', require=True, help='', group=group)
        self.add('--passswords', require=True, default=100000, help='', group=group)
        self.add('--os', help='', group=group)
        self.add('--device_type', type=int, default=161, help='指定netmiko类型', group=group)
        self.add('--ssh', action='store_true', help='', group=group)
        self.add('--telnet', action='store_true', help='', group=group)
        self.add('--timeout', type=int, default=10, help='', group=group)
        self.add('--blocking_timeout', type=int, default=10, help='', group=group)
