#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/9/4 18:22

'''
    修改 vi $(python -c 'import puresnmp;print(puresnmp.__path__[0])')/transport.py
        BUFFER_SIZE = 4096 # 4 KiB -> 20480

    修改 vi $(python -c 'import puresnmp;print(puresnmp.__path__[0])')/api/pythonic.py
        line 115, func multiwalk(), timeout=2 -> 10
    1.7版本已可以自定义，默认为6
'''

from collections import OrderedDict, defaultdict
from inspect import getfullargspec

from puresnmp import bulkget, bulkwalk
from puresnmp.x690.util import tablify

from .mibs import MIB
from ..func.base import MyClass
from ..func.parser import ArgParseClass
from ..func.string import bytes_decode, replace


# bulkwalk没有timeout参数，重写
# def bulkwalk(ip, community, oids, bulk_size=10, port=161, timeout=5):
# from puresnmp.api import raw
# from puresnmp.pdu import VarBind
#     # type: (str, str, List[str], int, int) -> Generator[VarBind, None, None]
#     """
#     puresnmp.api.pythonic 1.6.3  没有timeout
#     """
#
#     result = multiwalk(
#         ip, community, oids, port=port,
#         fetcher=raw._bulkwalk_fetcher(bulk_size), timeout=timeout)  # pylint: disable=protected-access
#     for oid, value in result:
#         yield VarBind(oid, value)


class Snmp(MyClass):
    def __init__(self, ip, community, port=None, timeout=None, bulk_size=None, *args, **kwargs):
        '''
        :param ip:
        :param community:
        :param port:
        :param timeout:
        :param bulk_size:    有些设备只能调到50以下，否则无响应，但timeout太小对于大数据耗时太长
        :param args:
        :param kwargs:
        '''
        MyClass.__init__(self, *args, **kwargs)
        self.ip = ip
        self.community = community
        self.timeoout = timeout or 10
        self.bulk_size = bulk_size or 10
        self.snmp_auth = {'ip': ip, 'community': community, 'port': port or 161}

    def lookup_num_oid(self, name):
        '''
        查找oid，可以被重写
        :param name:
        :return:
        '''
        if 'mib' not in self.cache:
            self.cache['mib'] = {}
            for k, v in MIB.items():
                self.cache['mib'].update(v)
        return self.cache['mib'].get(name, name)

    def bulkget(self, *args, **kwargs):
        '''
        :param args:
        :param kwargs:
        :return:
        '''
        for x in ['scalar_oids', 'repeating_oids']:
            if kwargs.get(x):
                # 把scalar_oids, repeating_oids转成 {'x.x.x.x.x': name}, 方便返回
                if isinstance(kwargs[x], dict):
                    continue
                oids = [kwargs[x]] if isinstance(kwargs[x], str) else kwargs[x]
                oids = {self.lookup_num_oid(y): y for y in oids}
                if x == 'scalar_oids':
                    # scalar oid要去掉.0
                    oids = {(k[:-2] if k.endswith('.0') else k): v for k, v in oids.items()}
                kwargs[x] = oids
        return self._bulkget(*args, **kwargs)

    def _bulkget(self, scalar_oids=None, repeating_oids=None, max_list_size=1000, timeout=None, keys=None,
                 ret_raw=False):
        '''
        scalar_oids, repeating_oids 必须是 dict
        :param scalar_oids:
            {'1.3.6.1.2.1.1.1': 'name1', '1.3.6.1.2.1.2.1': 'name2'} -> {'name1': value1, 'name2': value2}
        :param repeating_oids:  会返回OrderedDict, 有max_list_size限制, 建议用bulkwalk
        :param max_list_size:
        :param timeout:
        :param keys: 替换snmp oid中的item为字符串，优先使用oids dict中的value
            {'1': 'xxx', '2': 'yyy'}
        :param rat_raw:  返回原始的数据
        :return:
        '''
        raw = bulkget(scalar_oids=scalar_oids.keys() if isinstance(scalar_oids, dict) else scalar_oids,
                      repeating_oids=repeating_oids.keys() if isinstance(repeating_oids, dict) else repeating_oids,
                      max_list_size=max_list_size,
                      timeout=timeout or self.timeoout, **self.snmp_auth)
        d = raw._asdict()
        for k, v in d.items():
            for k1, v1 in v.items():
                v[k1] = bytes_decode(v1)
        if ret_raw:
            return d
        d1 = defaultdict(OrderedDict)
        if not keys:
            keys = {}
        for k, v in d.items():
            if k == 'scalars' and scalar_oids:
                for k1, v1 in v.items():
                    name = scalar_oids.get(k1) or scalar_oids.get(k1[:-2]) or k1
                    name = keys.get(name, name)
                    d1[k][name] = v1
            elif repeating_oids:
                # 比较乱，待整理，用bulkwalk先
                # oid中的.在replace函数中容易被误匹配, 需要escape
                pats = [(x + '.', '') for x in repeating_oids]
                for k1, v1 in v.items():
                    index, oid = replace(k1, pats=pats, default='', ret_with_pat=True, _any=True, escape=True)
                    name = repeating_oids.get(oid[:-1]) or oid[:-1]
                    key = keys.get(name, name)
                    if index not in d1[k]:
                        d1[k][index] = {}
                    d1[k][index].update({key: v1})
        return d1

    def bulkwalk(self, oids, *args, **kwargs):
        if isinstance(oids, str):
            oids = [oids]
        oids = {self.lookup_num_oid(y): y for y in oids}
        return self._bulkwalk(oids=oids, *args, **kwargs)

    def _bulkwalk(self, oids, keys=None, index=None, num_base_nodes=0, ret_raw=False, orderby=None, is_hex=False,
                  to_str=True, proc=None, bulk_size=None, timeout=None):
        '''
        :param oids: dict
        :param keys: 替换snmp oid为字符串
            {'1': 'xxx', '2': 'yyy'}
        :param index:   数字, 返回dict, 以index为key, 会忽略空值

        :param num_base_nodes:
        :param ret_raw: 返回迭代器, 因为有时需要详细的oid, 但tablify返回不带oid
            for row in raw:
                print('%s: %r' % row)
        :param orderby: 排序
        :param to_str: 返回str，否则可能转成int或float
        :param proc: (func, *args), 不使用tablify，proc, 返回(key, value)组成字典
        :param timeout: puresnmp的bulkwalk有bug，不能传递timeout
        :return:
        '''
        # 一些puresnmp版本中bulkwalk不包含timeout，需要检查
        d = {'timeout': timeout or self.timeoout} if 'timeout' in getfullargspec(bulkwalk).args else {}
        raw = bulkwalk(oids=oids.keys(), bulk_size=bulk_size or self.bulk_size, **d, **self.snmp_auth)
        if proc:
            func, *args = proc
            d = OrderedDict()
            for x in raw:
                k, v = func(x, *args)
                d[k] = v
            return d
        if ret_raw:
            # [(k, bytes_decode(v, to_type=to_type)) for k, v in raw]
            return raw
        lst = tablify(raw, num_base_nodes=num_base_nodes)
        if orderby:
            if isinstance(orderby, str):
                lst.sort(key=lambda x: x[orderby])
            elif hasattr(orderby, '__call__'):
                lst.sort(key=orderby)
        if keys:
            if '0' not in keys:
                keys['0'] = 'id'
            _lst = []
            for i, x in enumerate(lst):
                d = {keys.get(k, k): bytes_decode(x[k], to_type='str' if to_str else None, is_hex=is_hex) for k, v in
                     x.items()}
                _lst.append(d)
            if not index or index not in keys:
                return _lst
            _dict = OrderedDict()
            _index = keys[index]
            for i, x in enumerate(_lst):
                if not x[_index]:
                    continue
                _dict[x[_index]] = x
            return _dict
        else:
            return [{k: bytes_decode(v, to_type='str' if to_str else None) for k, v in x.items()} for i, x in
                    enumerate(lst)]


class SnmpArgParse(ArgParseClass):
    def __init__(self, *args, **kwargs):
        ArgParseClass.__init__(self, *args, **kwargs)

    def add_snmp(self, group='SNMP Args'):
        self.add('--ip', require=True, help='', group=group)
        self.add('--community', require=True, help='', group=group)
        self.add('--port', type=int, default=161, help='', group=group)
        self.add('--timeout', type=int, default=10, help='', group=group)
        self.add('--bulk_size', type=int, default=10, help='', group=group)


if __name__ == '__main__':
    sn = Snmp('192.168.2.63', 'xxxxxxxx')
    ret = sn.bulkget(scalar_oids=['1.3.6.1.2.1.1.1', '1.3.6.1.2.1.2.1'],
                     repeating_oids=['1.3.6.1.2.1.31.1.1.1.1', '1.3.6.1.2.1.31.1.1.1.18'],
                     keys={'1.3.6.1.2.1.1.1': 'descr', '1.3.6.1.2.1.2.1': 'port',
                           '1.3.6.1.2.1.31.1.1.1.1': 'ifName', '1.3.6.1.2.1.31.1.1.1.18': 'ifAlias'})
    sn.bulkwalk(oids=['1.3.6.1.2.1.47.1.1.1.1.2', '1.3.6.1.2.1.47.1.1.1.1.5'],
                keys={'2': 'descr', '5': 'clas', '7': 'name', '11': 'sn', '13': 'model'},
                index='2', orderby='2')
