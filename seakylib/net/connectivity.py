#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2021/7/20 10:35

from ..os.cmd import execute
import re
import xml.etree.ElementTree as ET
import nmap


def fping(dest=None, group=False, cmd_path='/usr/sbin/fping', option='-i 10 -r 1', cmd=None):
    if not cmd:
        cmd = '{} {}{} {}'.format(cmd_path, option, ' -g' if group else '',
                                  ' '.join(dest) if isinstance(dest, list) else dest)
    p = execute(cmd)
    fping_result = []
    for x in p.stdout.split('\n'):
        if not x:
            continue
        ip, *_ = x.split()
        r = True if re.search('is alive', x, re.I) else False
        fping_result.append({'ip': ip, 'alive': r, 'output': x})
    return {'cmd': cmd, 'result': fping_result}


def nmap2(dest=None, cmd_path='/bin/nmap', ports=None, no_ping=False, no_scan=False, cmd=None):
    if not cmd:
        opt = '-oX -'
        if no_ping:
            opt += ' -Pn'
        if no_scan:
            opt += ' -sP'
        if ports:
            opt += ' -p {}'.format(ports)
        cmd = '{} {} {}'.format(cmd_path, opt, dest)
    p = execute(cmd)
    root = ET.fromstring(p.stdout)
    hosts = []
    for host in root.findall(path='host'):
        d = host.find('status').attrib
        d.update(host.find('address').attrib)
        ports = []
        for port in host.find('ports').findall('port'):
            e = port.attrib
            e.update(port.find('state').attrib)
            e.update(port.find('service').attrib)
            ports.append(e)
        d['ports'] = ports
        hosts.append(d)
    return {'cmd': cmd, 'result': hosts}
