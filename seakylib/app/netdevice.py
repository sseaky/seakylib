#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/20 10:15

from ..func.base import MyClass
from ..func.mrun import MrunArgParse
from ..func.parser import ArgParseClass
from ..probe.miko.connect import BaseDevice
from ..probe.snmp import Snmp


class NetDeviceClass(MyClass):
    def __init__(self, ip=None, os=None, miko_param=None, snmp_param=None, *args, **kwargs):
        '''
        :param ip:
        :param miko_param:  {'os': os, 'telnet_enable': True, 'ssh_enable': True,
                'ip': ip, password: [{'username':, 'password':, 'secret':}, {}, ...]}
        :param snmp_param:  {'community': SNMP_COMMUNITY, 'ip': ip, 'port':161, timeout: 10, bulk_size: 10}
        :param args:
        '''
        MyClass.__init__(self, *args, **kwargs)
        self.miko_param = miko_param
        self.snmp_param = snmp_param
        if not ip:
            d = miko_param or snmp_param
            if isinstance(d, dict):
                ip = d['ip']
        assert ip, 'no ip given.'
        self.ip = ip
        if not os:
            if isinstance(miko_param, dict):
                os = miko_param['os']
        self.os = os

    def init_conn(self):
        if self.miko_param:
            self.cli = BaseDevice(**self.miko_param, log=self.log)
        if self.snmp_param:
            self.snmp = Snmp(**self.snmp_param, log=self.log)
        return True, self.func_done()

    def do(self):
        funcs = {'comware': self.do_comware,
                 'vrp': self.do_vrp,
                 'ios': self.do_ios,
                 'iosxe': self.do_iosxe,
                 'iosxr': self.do_iosxr,
                 'nxos': self.do_nxos,
                 'dnos': self.do_dnos,
                 'ftos': self.do_ftos,
                 'powerconnect': self.do_powerconnect,
                 'ibmnos': self.do_ibmnos,
                 'ruijie': self.do_ruijie,
                 'junos': self.do_junos,
                 }
        return funcs[self.cli.os]()

    def do_comware(self):
        return False, 'no code.'

    def do_vrp(self):
        return False, 'no code.'

    def do_ios(self):
        return False, 'no code.'

    def do_iosxe(self):
        return False, 'no code.'

    def do_iosxr(self):
        return False, 'no code.'

    def do_nxos(self):
        return False, 'no code.'

    def do_dnos(self):
        return False, 'no code.'

    def do_ftos(self):
        return False, 'no code.'

    def do_ibmnos(self):
        return False, 'no code.'

    def do_powerconnect(self):
        return False, 'no code.'

    def do_ruijie(self):
        return False, 'no code.'

    def do_junos(self):
        return False, 'no code.'


class NeteaseDeviceClass(NetDeviceClass):
    def __init__(self, *args, **kwargs):
        NetDeviceClass.__init__(self, *args, **kwargs)


class NetDeviceArgParse(ArgParseClass):
    def __init__(self, *args, **kwargs):
        ArgParseClass.__init__(self, *args, **kwargs)
        self.add_base()

    def add_device(self, group='Single Device', snmp_timeout=30, snmp_bulksize=1000, cli_timeout=30):
        self.add('--ip', required=True, help='目标IP', group=group)
        self.add('--os', default='', help='目标系统', group=group)
        self.add('--snmp_timeout', default=snmp_timeout, type=int, help='snmp超时, {}s'.format(snmp_timeout), group=group)
        self.add('--snmp_bulksize', default=snmp_bulksize, type=int, help='snmpwalk bulksize, {}'.format(snmp_bulksize),
                 group=group)
        self.add('--cli_timeout', default=cli_timeout, type=int, help='cli超时, {}s'.format(cli_timeout), group=group)
        # self.add('--cli_telnet_enable', default=10, type=int, help='cli使用telnet', group=group)


class MultiNetDeviceArgParse(MrunArgParse):
    def __init__(self, *args, **kwargs):
        MrunArgParse.__init__(self, *args, **kwargs)

    def add_multi_device(self, group='Multi Device'):
        self.add('--limit', type=int, default=100000, help='任务限制', group=group)
        self.add('--ips', default='', help='指定ip组', group=group)
        self.add('--os', default='', help='指定os', group=group)
