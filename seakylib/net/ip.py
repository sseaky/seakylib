#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/20 14:56

import ipaddress
import re

import IPy

Pattern_IP = '(?P<ip>((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?))'    # 老版本

Pattern_IPv4Seg = '(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])'
Pattern_IPv4Address = '(?P<ip4>{})'.format('({0}\.){{3,3}}{0}'.format(Pattern_IPv4Seg))
Pattern_IPv6Seg = '[0-9a-fA-F]{1,4}'
Pattern_IPv6Address = '(?P<ip6>{})'.format('|'.join([
    '({0}:){{7,7}}{0}'.format(Pattern_IPv6Seg),  # 1:2:3:4:5:6:7:8
    '({0}:){{1,7}}:'.format(Pattern_IPv6Seg),  # 1::                                 1:2:3:4:5:6:7::
    '({0}:){{1,6}}:{0}'.format(Pattern_IPv6Seg),  # 1::8               1:2:3:4:5:6::8   1:2:3:4:5:6::8
    '({0}:){{1,5}}(:{0}){{1,2}}'.format(Pattern_IPv6Seg),  # 1::7:8             1:2:3:4:5::7:8   1:2:3:4:5::8
    '({0}:){{1,4}}(:{0}){{1,3}}'.format(Pattern_IPv6Seg),  # 1::6:7:8           1:2:3:4::6:7:8   1:2:3:4::8
    '({0}:){{1,3}}(:{0}){{1,4}}'.format(Pattern_IPv6Seg),  # 1::5:6:7:8         1:2:3::5:6:7:8   1:2:3::8
    '({0}:){{1,2}}(:{0}){{1,5}}'.format(Pattern_IPv6Seg),  # 1::4:5:6:7:8       1:2::4:5:6:7:8   1:2::8
    '{0}:((:{0}){{1,6}})'.format(Pattern_IPv6Seg),  # 1::3:4:5:6:7:8     1::3:4:5:6:7:8   1::8
    ':((:{0}){{1,7}}|:)'.format(Pattern_IPv6Seg),  # ::2:3:4:5:6:7:8    ::2:3:4:5:6:7:8  ::8       ::
    # fe80::7:8%eth0     fe80::7:8%1  (link-local IPv6 addresses with zone index)
    'fe80:(:{0}){{0,4}}%[0-9a-zA-Z]{{1,}}'.format(Pattern_IPv6Seg),
    # ::255.255.255.255  ::ffff:255.255.255.255  ::ffff:0:255.255.255.255 (IPv4-mapped IPv6 addresses and IPv4-translated addresses)
    '::(ffff(:0{{1,4}}){{0,1}}:){{0,1}}{0}'.format(Pattern_IPv6Seg),
    # 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33 (IPv4-Embedded IPv6 Address)
    '({0}:){{1,4}}:{0}'.format(Pattern_IPv6Seg),
]))
Pattern_IPv4Network = '(?P<network4>{}(/\d+)*)'.format(Pattern_IPv4Address)
Pattern_IPv6Network = '(?P<network6>{}(/\d+)*)'.format(Pattern_IPv6Address)


def is_ip(ip):
    if not isinstance(ip, str):
        return False
    p = re.compile('^{}$'.format(Pattern_IP))
    if p.match(ip):
        return True
    else:
        return False


def collect_ip(s, uniq=True, sort=True):
    '''提取ip'''
    ips = []
    if isinstance(s, str):
        ips = [x.group('ip') for x in re.finditer(Pattern_IP, s)]
        if uniq:
            ips = list(set(ips))
    return sort_ips(ips) if sort else ips


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
