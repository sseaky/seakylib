#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2020/7/17 10:14

from io import StringIO

import paramiko
from paramiko import RSAKey

from ..func.base import MyClass, myclass_precheck, count_time


class Client(MyClass):
    def __init__(self, miko_param, *args, **kwargs):
        '''
        :param miko_param: 参考paramiko.SSHClient()
        :param args:
        :param kwargs:
        '''
        MyClass.__init__(self, *args, **kwargs)
        # pkey需要转换
        if isinstance(miko_param.get('pkey'), str):
            miko_param['pkey'] = RSAKey.from_private_key(StringIO(miko_param['pkey']))
        self.miko_param = miko_param

    def __enter__(self):
        is_ok, result = self.login()
        assert is_ok, result
        return self

    def __exit__(self, type, value, trace):
        self.close()

    @count_time
    def login(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(**self.miko_param)
        return True, self.func_done()

    def _assure_connect(self):
        if hasattr(self, 'ssh'):
            return self.check_alive()
        return False, 'no ssh available.'

    @myclass_precheck(have_run='login', check_func='_assure_connect')
    def exec_command(self, cmds, merge_output=True, split_cmds=True, show_cmd=True, encoding='utf8', one_off=False):
        if isinstance(cmds, str):
            if split_cmds:
                cmds = cmds.split(';')
            else:
                cmds = [cmds]
        outputs = []
        outs = []
        errs = []
        for cmd in cmds:
            if show_cmd:
                outputs.append('>>>>> {} <<<<<\n'.format(cmd))
            self.log.debug('execute command "{}"'.format(cmd))
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            stdout, stderr = stdout.read().decode(encoding), stderr.read().decode(encoding)
            outputs.extend([stdout, stderr])
            outs.append(stdout)
            errs.append(stderr)
        if one_off:
            self.close()
        if merge_output:
            return True, '\n'.join(outputs)
        else:
            return True, {'stdout': '\n'.join(stdout), 'stderr': '\n'.join(stderr)}

    @count_time
    def one_off(self, *args, **kwargs):
        kwargs['one_off'] = True
        return self.exec_command(*args, **kwargs)

    def close(self):
        self.ssh.close()
        return True, self.func_done()

    def check_alive(self):
        if self.ssh.get_transport() is None:
            return False, 'connection is closed.'
        else:
            return True, 'connection is alive'
