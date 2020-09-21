#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2020/8/25 14:29

import json
import re
from multiprocessing import Manager, pool

from redis import Redis, ConnectionPool
from redis.sentinel import Sentinel

from ..func.log import SimpleLog

SCAN_COUNT = 10000
PIPELINE_N = 10000


class RedisC(Redis):
    def __init__(self, host=None, port=6379, db=0, password=None, sentinels=None, service_name=None,
                 decode_responses=True, socket_timeout=None, log=None, *args, **kwargs):
        if sentinels:
            sentinel = Sentinel(sentinels=sentinels)
            master = sentinel.discover_master(service_name=service_name)
            host, port = master
        self.server = {'host': host, 'port': port, 'db': db, 'password': password, 'decode_responses': decode_responses,
                       'socket_timeout': socket_timeout}
        self.log = log if log else SimpleLog()
        self.pool = ConnectionPool(*args, **self.server, **kwargs)
        Redis.__init__(self, connection_pool=self.pool)
        # Redis.__init__(self, *args, **self.server, **kwargs)
        self.cache_result = {}

    def show_server(self):
        '''
        显示 redis server 信息
        :return:
        '''
        msg = 'Redis: {host}:{port}/{db}'.format(**self.server)
        if self.log:
            self.log.info(msg)
        else:
            print(msg)

    def count_keys(self, prefix, count=SCAN_COUNT, ret_keys=False, limit=None, key_with_prefix=True):
        '''
        统计 keys 数量
        :param prefix:
        :param count:
        :param ret_keys:
        :param limit:
        :param key_with_prefix: 可以返回剥离prefix的key
        :return:
        '''
        msg = 'fetch keys like "{}"'.format(prefix)
        self.log.debug('start {}'.format(msg))
        keys = []
        empty = True
        for i, k in enumerate(self.scan_iter(prefix + '*', count=count)):
            if limit and i >= limit:
                break
            empty = False
            if ret_keys:
                key = k if key_with_prefix else re.sub('^{}'.format(prefix), '', k)
                keys.append(key)
        d = {'n': 0 if empty else i, 'prefix': prefix}
        if ret_keys:
            d.update({'keys': keys})
        self.log.debug('finish {}'.format(msg))
        return d

    def get_prefix(self, prefix, n=PIPELINE_N, count=SCAN_COUNT, ret_dict=False, unjson=False, limit=None,
                   key_with_prefix=False, only_value=False, cache_save=True, cache_load=False, **kwargs):
        '''
        :param prefix:
        :param n:
        :param count:
        :param ret_dict:
        :param unjson:
        :param limit:
        :param key_with_prefix:
        :param only_value:
        :param cache_save:   # 为了减少重复查询，保存一些中间结果
        :param cache_load:
        :param kwargs:
        :return:
        '''
        # 查询参数不同，保存的data也不一样
        kwargs_id = {'ret_dict': ret_dict, 'unjson': unjson, 'limit': limit, 'key_with_prefix': key_with_prefix,
                     'only_value': key_with_prefix}
        kwargs_id_s = ','.join(['{}={}'.format(k, kwargs_id[k]) for k in sorted(list(kwargs_id.keys()))])
        query_id = '{}_{}'.format(prefix, kwargs_id_s)
        if cache_load and query_id in self.cache_result:
            data = self.cache_result[query_id]
        else:
            keys = self.count_keys(prefix=prefix, count=count, ret_keys=True, limit=limit, key_with_prefix=False)[
                'keys']
            data = self.pipe_get(keys, n=n, ret_dict=ret_dict, prefix=prefix, unjson=unjson,
                                 key_with_prefix=key_with_prefix, only_value=only_value, **kwargs)
            if cache_save:
                self.cache_result[query_id] = data
        return data

    @staticmethod
    def _merge_dups(data, elements=None, drop_dup=True, split=','):
        '''
        :param data:
        :param elements:
        :param drop_dup: 合并时丢弃重复
        :param split:
        :return:
        '''
        # data为list时才有可能存在重复
        if isinstance(data, list):
            d = {}
            for i, x in enumerate(data):
                if isinstance(x, (tuple, list)):
                    k, v = x
                elif isinstance(x, dict) and elements:
                    k, v = x[elements[0]], x[elements[1]]
                else:
                    raise Exception('obj {} is not a list/dict'.format(x))
                if k not in d:
                    d[k] = [str(v)]
                else:
                    if drop_dup and str(v) in d[k]:
                        continue
                    d[k].append(str(v))
            for k, v in d.items():
                d[k] = split.join(v)
        else:
            return data

    def pipe_set(self, data, n=PIPELINE_N, columns=None, transaction=True, shard_hint=None, ex=None, ret=True,
                 raise_on_error=True, group=False, drop_dup=True, group_split=',',
                 json_value=False, prefix='', ret_raw=False, result_prefix='', silenct=False, **kwargs):
        '''
        :param data:    list/dict, 数据
                [(k1, v1), (k2, v2), ...]
                [{'c1': k1, 'c2': v1}, {'c1': k2, 'c2': v2}, ...], columns=['c1', 'c2]
                {k1: v1, k2: v2, ...}
                {w1: {'c1': k1, 'c2': v1}, w2: {'c1': k2, 'c2': v2}, ...}, columns=['c1', 'c2]
        :param n:   每次更新数量
        :param columns:    如果data是list，而obj是一个字典，可以指定 k, v 的keys，
                            如果data是dict，而obj的value是一个字典，elements也会应用
        :param transaction:
        :param shard_hint:
        :param ex:  有效时间，单位s
        :param ret: 返回set结果
        :param raise_on_error:
        :param group:    分组原始数据并合并
        :param drop_dup:    分组并过滤重复的数据
        :param group_split: 分组的分隔符
        :param json_value:  是否将value json化
        :param prefix:  放入redis时，k加上前缀
        :param ret_raw:  是否返回原始返回值
        :param result_prefix:  返回结果字典时是否添加前缀，用于合并多个结果
        :param silenct:
        :param kwargs:
        :return:
        '''

        def json_it(v):
            if not isinstance(v, (str, bytes)) and json_value:
                return json.dumps(v)
            else:
                return v

        msg = 'set {} "{}" items'.format(len(data), prefix)
        if not silenct:
            self.log.debug('start {}'.format(msg))

        if group:
            data = self._merge_dups(data, drop_dup=drop_dup, split=group_split)

        rs = []
        with self.pipeline(transaction=transaction, shard_hint=shard_hint) as pipe:
            i = 0
            if isinstance(data, list):
                for x in data:
                    if isinstance(x, (tuple, list)):
                        # [(k, v)]
                        k, v = x
                    elif isinstance(x, dict) and columns:
                        # [{col1:k, col2:v, ...}]
                        k, v = x[columns[0]], x[columns[1]]
                    pipe.set(prefix + k, json_it(v), ex=ex, **kwargs)
                    if i and (i % n == 0 or i + 1 == len(data)):
                        r = pipe.execute(raise_on_error=raise_on_error)
                        if ret:
                            rs.extend(r)
                    i += 1
            elif isinstance(data, dict):
                for k, v in data.items():
                    if columns and isinstance(v, dict):
                        # {k: {col1:k1, col2:v1, ...}, ...}
                        k1, v1 = v[columns[0]], v[columns[1]]
                    else:
                        # {k: v, ...}
                        k1, v1 = k, v
                    pipe.set(prefix + k1, json_it(v1), ex=ex, **kwargs)
                    if i and (i % n == 0 or i + 1 == len(data)):
                        r = pipe.execute(raise_on_error=raise_on_error)
                        if ret:
                            rs.extend(r)
                    i += 1
            else:
                raise Exception('data is not a list/dict when pipe set')
        d = {'input': len(data), 'set': len(rs), 'success': len([x for x in rs if x])}
        if ret_raw:
            d['raw'] = rs
        if not silenct:
            self.log.debug('finish {}'.format(msg))
        return {result_prefix + '_' + k: v for k, v in d.items()} if result_prefix else d

    def mp_pipe_set(self, data, chunk=100000, processes=None, **kwargs):
        '''
        redis server是单线程的，多进程写入没什么性能提高
        :param data:
        :param chunk:
        :param processes:
        :param kwargs:
        :return:
        '''
        result = Manager().list()
        thread_pool = pool.ThreadPool(processes)
        margs = []
        for i in range(0, len(data), chunk):
            d = {'data': data[i:i + chunk]}
            d.update(kwargs)
            margs.append({'data': d, 'storage': result, 'i': i})
        thread_pool.map(self._mp_pipe_set_wrap, margs)
        thread_pool.close()
        thread_pool.join()
        return result

    def _mp_pipe_set_wrap(self, margs):
        print(margs['i'])
        r = self.pipe_set(**margs['data'])
        margs['storage'].extend(r)

    def pipe_get(self, keys, n=PIPELINE_N, prefix='', transaction=True, shard_hint=None, ret_dict=False,
                 raise_on_error=True, unjson=False, columns=None, key_with_prefix=False, only_value=False,
                 silence=False):
        '''
        默认返回 [(k1, v1), (k2, v2), ...]
        :param keys:
        :param n:
        :param prefix:  查询时自动添加前缀，返回时剥离
        :param transaction:
        :param shard_hint:
        :param ret_dict:    返回 {k1: v1, k2: v2, ...}
        :param raise_on_error:
        :param unjson:
        :param columns: 将每一个 record 转化为字典
            columns is None -> [value1, value2, ...]
            columns = [key_name, value_name] -> [{key_name: key, value_name: value}, ...]
        :param key_with_prefix: 返回时, key包含prefix
        :param only_value: 返回不包含key的 value list
        :param silence:
        :return:
        '''

        def unjson_it(v):
            if unjson and isinstance(v, str):
                return json.loads(v)
            else:
                return v

        msg = 'fetch value with {} keys.'.format(len(keys))
        if not silence:
            self.log.debug('start {}'.format(msg))

        rs = []
        rsd = {}
        if not keys:
            return rsd if ret_dict else rs
        with self.pipeline(transaction=transaction, shard_hint=shard_hint) as pipe:
            offset = 0
            for i, k in enumerate(keys, 1):
                pipe.get(prefix + k)
                if i == len(keys) or (i % n == 0):
                    r = pipe.execute(raise_on_error=raise_on_error)
                    if only_value:
                        rs.extend([unjson_it(x) for x in r] if unjson else r)
                    else:
                        for j, v in enumerate(r):
                            if isinstance(columns, list):
                                # columns = [key_name, value_name] -> [{key_name: key, value_name: value}, ...]
                                value = {columns[0]: keys[offset + j], columns[1]: unjson_it(v)}
                            else:
                                # {value1, value2, ...}
                                value = unjson_it(v)
                            key = prefix + keys[offset + j] if key_with_prefix else keys[offset + j]
                            if ret_dict:
                                rsd[key] = value
                            else:
                                rs.append(value if columns else (key, unjson_it(v)))
                    offset = i
        if not silence:
            self.log.debug('finish fetch value with {} keys.'.format(len(rs) or len(rsd)))
        return rsd if ret_dict else rs

    def append_ex(self, k, v, ex=None):
        '''
        默认 append 不改变原ex
        :param k:
        :param v:
        :param ex:
        :return:
        '''
        r = self.append(k, v)
        if ex:
            self.expire(k, ex)
        return r

    def pipe_delete(self, keys=None, prefix='', n=PIPELINE_N, count=SCAN_COUNT, transaction=True, shard_hint=None,
                    raise_on_error=True):
        '''
        批量删除
        :param keys:
        :param prefix:
        :param n:
        :param count:
        :param transaction:
        :param shard_hint:
        :param raise_on_error:
        :return:
        '''
        rs = []
        if not keys and prefix:
            d = self.count_keys(prefix=prefix, ret_keys=True, count=count)
            keys = d['keys']
        if not keys:
            return rs
        msg = 'delete {} "{}" items'.format(len(keys), prefix)
        self.log.debug('start {}'.format(msg))
        with self.pipeline(transaction=transaction, shard_hint=shard_hint) as pipe:
            for i, k in enumerate(keys):
                pipe.delete(k)
                if i and (i % n == 0 or i + 1 == len(keys)):
                    r = pipe.execute(raise_on_error=raise_on_error)
                    rs.extend(r)
        d = {'input': len(keys), 'delete': len(rs), 'success': len([x for x in rs if x])}
        self.log.debug('finish {}'.format(msg))
        return d

    def sync_to(self, rds_to, prefix, limit=None):
        return self._sync_redis(rds_from=self, rds_to=rds_to, prefix=prefix, limit=limit)

    def sync_from(self, rds_from, prefix, limit=None):
        return self._sync_redis(rds_from=rds_from, rds_to=self, prefix=prefix, limit=limit)

    def _sync_redis(self, rds_from, rds_to, prefix, limit=None):
        inst_to = rds_to(log=self.log) if isinstance(rds_to, type) else rds_to  # weather rds_to is a class
        inst_from = rds_from(log=self.log) if isinstance(rds_from, type) else rds_from  # weather rds_to is a class
        name_from = inst_from.__class__.__name__
        name_to = inst_to.__class__.__name__
        rs = []
        if name_from == name_to:
            self.log.warn('{} -> {}, skip sync for target is same as source'.format(name_from, name_to))
            return
        if isinstance(prefix, str):
            prefix = [prefix]

        for pr in prefix:
            msg = 'sync "{}" from {} to {}'.format(pr, name_from, name_to)
            self.log.debug('start {}'.format(msg))
            data = inst_from.get_prefix(pr, limit=limit)
            r = inst_to.pipe_set(data=data, prefix=pr)
            rs.append(r)
            self.log.debug('finish {}'.format(msg))
        return rs
