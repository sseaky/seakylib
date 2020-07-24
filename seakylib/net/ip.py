#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/20 14:56

import ipaddress
import re

import IPy

Pattern_IP = '(?P<ip>((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?))'


def is_ip(ip):
    if not isinstance(ip, str):
        return False
    p = re.compile('^{}$'.format(Pattern_IP))
    if p.match(ip):
        return True
    else:
        return False


def collect_ip(s):
    '''提取ip'''
    ips = []
    if isinstance(s, str):
        ips = re.findall(Pattern_IP, s)
    return ips


def in_network(ip, network):
    nw = IPy.IP(network) if isinstance(network, str) else network
    return ip in nw


def in_network1(ip, network):
    nw = ipaddress.ip_network(network) if isinstance(network, str) else network
    return ipaddress.ip_address(ip) in nw


def ip2int(s, default=-1):
    return sum(int(y) * 256 ** i for i, y in enumerate(s.split('.')[::-1])) if is_ip(s) else default


def sort_ips(l):
    return sorted(l, key=lambda x: ip2int(x))


def make_long_mask(short_mask):
    if not short_mask:
        return '255.255.255.0'
    s = ('1' * (int(short_mask) if short_mask else 24)).zfill(32)[::-1]
    return '.'.join([str(int(s[i * 8:i * 8 + 8], 2)) for i in range(4)])


def make_short_mask(long_mask):
    return 24 if not long_mask \
        else long_mask if isinstance(long_mask, int) \
        else len(''.join(bin(int(x))[2:].replace('0', '') for x in long_mask.split('.')))


def find_network(ip, mask=24):
    '''
    根据mask找到网段
    :param ip: 
    :param mask: 
    :return: 
    '''
    return ipaddress.ip_interface('{}/{}'.format(ip, mask)).network
