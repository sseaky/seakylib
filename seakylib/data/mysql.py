#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2018/9/17 15:56


import os
import re
import traceback
from collections import OrderedDict
from functools import wraps

import pandas
import pandas as pd
from sqlalchemy import Float
from sqlalchemy import INTEGER as INTEGER1, FLOAT, create_engine, UniqueConstraint
from sqlalchemy import event
from sqlalchemy import exc
from sqlalchemy.dialects.mysql import BIGINT, INTEGER, SMALLINT, TINYINT
from sqlalchemy.orm import sessionmaker

from ..data.pd import set_none
from ..func.string import change_type, add_quote
from ..func.time import datetime_to_string


# sqlacodegen --outfile=librenms.py mysql+pymysql://test:test@xx.xx.xx.xx:3306/testdb --tables <table>


def catch_sql_exception(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        inst = args[0]
        retry = inst.retry + 1
        while retry > 0:
            try:
                return f(*args, **kwargs)
            except AssertionError as e:
                return False, e
            except Exception as e:
                error = e
                inst.session.rollback()
            inst.db_session = new_session(conn_str=inst.cls_conn_str)
            retry -= 1
        return False, '{0}, {1}, {2}, {3}'.format(traceback.format_exc() if getattr(inst, 'verbose') else error,
                                                  f.__name__, args, kwargs)

    return wrap


def check_writable(f):
    @wraps(f)
    def wrap(self, *args, **kwargs):
        if hasattr(self.model, 'readonly') and self.model.readonly:
            return False, 'readonly on {}'.format(self.model.__tablename__)
        return f(self, *args, **kwargs)

    return wrap


def obj2dict(obj, key=None, cols=None):
    if isinstance(obj, dict) or not obj:
        return obj
    elif isinstance(obj, list):
        d = {}
        l = []
        if hasattr(obj[0], '__table__'):
            cols = [co.name for co in obj[0].__table__.columns]
        for i, ob in enumerate(obj):
            d1 = obj2dict(ob, cols=cols)
            if key in cols:
                d[d1[key]] = d1
            else:
                l.append(d1)
        return d if key in cols else l
    elif hasattr(obj, '__table__'):
        if not cols:
            cols = [co.name for co in obj.__table__.columns]
        return {co: getattr(obj, co) for co in cols}


def engine(conn_str=None, echo=False, **kwargs):
    '''
    :param conn_str:
    :param echo:
        if real, new session with new engine will create a real new session, REAL is REQUIRED in multi process mode
        otherwise, func assigns the exist _engine, which may reuse session existed
    :param kwargs:
    :return:
    '''

    _engine = create_engine(conn_str, echo=echo, pool_pre_ping=True, pool_recycle=120, pool_size=10, **kwargs)

    @event.listens_for(_engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(_engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )

    return _engine


def new_session(conn_str=None, echo=False, **kwargs):
    _Session = sessionmaker(bind=engine(conn_str=conn_str, echo=echo, **kwargs))
    return _Session()


class MyModel:
    cls_conn_str = ''
    retry = 0

    def __init__(self, model=None, db_session=None, **kwargs):
        '''
        :param model:   Model of table
        :param db_session: db_session
        :param retry: 重试次数，sqlalchamy会抛出too many connection的异常，用于重试
        # :param engine_kw: kwargs of self.engine, 貌似没用，需要修改
        :param kwargs:
            adapt_length:   更改时适应column字符串长度
        '''
        self.cache = {'quote_cols': {}}
        self.kwargs = kwargs
        self.model = model
        self.db_session = db_session
        if model:
            self.init_model()
        self.pk = None
        self.verbose = kwargs.get('verbose')

    def init_model(self):
        self.cols = {x.name: x for x in self.model.__table__.columns}
        self.cols_unicon = [(col for col in x.columns) for x in self.model.__table_args__ if
                            isinstance(x, UniqueConstraint)] if hasattr(self.model, '__table_args__') else []
        self.cols_name, self.cols_notnullable, self.cols_unique = list(), list(), list()
        for name, co in self.cols.items():
            self.cols_name.append(name)
            if not co.nullable:
                self.cols_notnullable.append(co.name)
            if co.unique or co.primary_key:
                self.cols_unique.append(co.name)
            if co.primary_key:
                self.pk = co.name
        # last_cols for update last_<>
        self.cache['last_cols'] = []
        for co in self.cols_name:
            last_co = 'last_{}'.format(co)
            if last_co in self.cols_name:
                self.cache['last_cols'].append(co)

    @property
    def session(self):
        if self.db_session:
            _session = self.db_session
        else:
            d = self.kwargs.get('engine_kw', {})
            if 'conn_str' not in d and self.cls_conn_str:
                d['conn_str'] = self.cls_conn_str
            self.db_session = new_session(**d)
            _session = self.db_session
        return _session

    @property
    def engine(self):
        d = self.kwargs.get('engine_kw', {})
        if 'conn_str' not in d and self.cls_conn_str:
            d['conn_str'] = self.cls_conn_str
        return engine(**d)

    def add_quote_by_col_type(self, value, name):
        '''
        判断是否要在sql中加""
        :param value:
        :param name:
        :return:
        '''
        if value is None:
            return 'null'
        if not name in self.cache['quote_cols']:
            types = (Float, FLOAT, BIGINT, TINYINT, SMALLINT, INTEGER, INTEGER1)
            no_quote = isinstance(self.cols[name].type, types)
            self.cache['quote_cols'][name] = no_quote
        else:
            no_quote = self.cache['quote_cols'][name]
        if isinstance(value, str):
            value = value.replace('"', '\\"')
            if self.kwargs.get('adapt_length') and hasattr(self.cols[name].type, 'length'):
                value = value[:self.cols[name].type.length]
        return ('{}' if no_quote else '"{}"').format(value)

    @catch_sql_exception
    def get_obj(self, data=None, key='id', condition='', retdict=False):
        '''
        :param data:    dict of data
        :param key:
        :param condition:
        :return:
        '''

        def make_condition(col, v):
            return '{0}={1}'.format(col.name, v if col.type in [INTEGER, FLOAT] else "'{}'".format(v))

        if data:
            if key in data:
                result = self.session.query(self.model).filter(
                    getattr(self.model, key) == data[key]).first()
                if retdict:
                    result = obj2dict(result)
                return True, result

            for k, v in data.items():
                if k in self.cols_unique:
                    result = self.session.query(self.model).filter(getattr(self.model, k) == v).first()
                    if retdict:
                        result = obj2dict(result)
                    return True, result
            for x in self.cols_unicon:
                for col in x:
                    if col.name not in data.keys():
                        continue
                condition = ' and '.join([make_condition(y, data[y.name]) for y in x])
        if not condition:
            return False, 'no condition provided on get_obj.'
        sql = 'select * from {0} where {1}'.format(self.model.__tablename__, condition)
        result = self.session.execute(sql).first()
        return True, result

    @check_writable
    @catch_sql_exception
    def update_one(self, data, ex_obj=None, key='id', expandable=False):
        '''
        使用self.update代替
        :param data:
        :param ex_obj:
        :param key:
        :param expandable:
        :return:
        '''
        if ex_obj is None:
            status, ex_obj = self.get_obj(data)
            if not status:
                return status, ex_obj
        if not ex_obj:
            if expandable:
                return self.add(data, expandable=False, check=False)
            else:
                return False, '{} is not exist.'.format(getattr(ex_obj, key))
        flag_change = False
        if isinstance(ex_obj, dict):
            if key not in ex_obj:
                return False, 'key {0} is not in model when update {1}'.format(key, ex_obj)
            changes = []
            for k, v in data.items():
                if k in ex_obj and k in self.cols_name and v != ex_obj[k]:
                    changes.append('`{0}`={1}'.format(k, add_quote(v)))
                    flag_change = True
            if changes:
                sql = 'update `{0}` set {1} where `{2}`={3}'.format(self.model.__tablename__, ','.join(changes),
                                                                    key, add_quote(ex_obj[key]))
                self.session.execute(sql)
                # self.flush()
                self.commit()
            if not data.get(key):
                data[key] = ex_obj[key]
        else:
            for k, v in data.items():
                if k in self.cols_name and v != getattr(ex_obj, k):
                    flag_change = True
                    setattr(ex_obj, k, v)
            if flag_change:
                self.session.commit()
            if not data.get(key):
                data[key] = getattr(ex_obj, key)
        return True, {key: data[key], 'changed': flag_change}

    # def update(self, data_new, *args, **kwargs):
    #     # return self.update_df(data_new, *args, **kwargs)
    #     return self.update_batch(data_new, *args, **kwargs)

    def _update_compare_dict(self, data_new, data_old, key, cols=None, func_skip=None, last=True, fuzz_digit=True):
        '''

        :param data_new:
        :param data_old:
        :param key:
        :param cols:
        :param func_skip: 忽略记录函数
        :param last:    如果表中有last_字段，update时同时更新
        :param fuzz_digit: 是否模糊字符串和数字，mysql select时，可混用
        :return:
        '''
        if isinstance(data_new, pd.DataFrame):
            if data_new.index.name != key:
                data_new.index = data_new[key]
            data_new = data_new.to_dict(orient='index')
        if isinstance(data_old, pd.DataFrame):
            if data_old.index.name != key:
                data_old.index = data_old[key]
            data_old = data_old.to_dict(orient='index')
        items_new = []
        items_need_update = []
        changed_cols = []  # Align the changed keys
        items_skip = []
        only_change_last = 0

        for k, v in data_new.items():
            if hasattr(func_skip, '__call__') and func_skip(v):
                items_skip.append(v)
                continue
            if k not in data_old:
                items_new.append(v)
                continue
            if last:
                # 更新last_col
                for c in self.cache['last_cols']:
                    if c in data_old[k].keys() and 'last_{}'.format(c) not in v:
                        v['last_{}'.format(c)] = data_old[k][c]
            changed_real = False
            changed_last = False
            for k1, v1 in v.items():
                if cols and k1 not in cols:
                    continue
                if k1 in data_old[k] and k1 in self.cols_name:
                    if v1 and data_old[k][k1] is None:  # 原数据为None，需要替换
                        pass
                    elif v1 == data_old[k][k1]: # 数据不变
                        continue
                    elif type(v1) == type(data_old[k][k1]) or not fuzz_digit:  # 模糊类型
                        pass
                    elif change_type(v1, to_type=type(data_old[k][k1])) == data_old[k][k1]:
                        continue
                    if k1 not in changed_cols:
                        changed_cols.append(k1)
                    if k1.startswith('last_'):
                        changed_last = True
                    else:
                        changed_real = True
            if not changed_real and not changed_last:
                continue
            if changed_last and not changed_real:
                only_change_last += 1
            items_need_update.append((v, data_old[k]))
        items_miss = []
        for k, v in data_old.items():
            if k not in data_new:
                items_miss.append(v)
        return {'data_new': data_new, 'data_old': data_old,
                'items_new': items_new, 'items_need_update': items_need_update, 'only_change_last': only_change_last,
                'items_miss': items_miss, 'items_skip': items_skip,
                'changed_cols': changed_cols}

    def _update_compare_df(self, data_new, data_old, key, cols, func_skip, to_str):
        if isinstance(data_new, dict):
            data_new = pd.DataFrame(data_new).T
        if isinstance(data_old, dict):
            data_old = pd.DataFrame(data_old).T

        df_new_copy = data_new.sort_values(by=key, ascending=True, inplace=False)
        df_old_copy = data_old.sort_values(by=key, ascending=True, inplace=False)

        if to_str:
            df_new_copy = df_new_copy.astype(str)
            df_old_copy = df_old_copy.astype(str)
        for i, x in enumerate([df_new_copy, df_old_copy]):
            if x.index.name != key:
                x.index = x[key]
                # 把key作为index，需要修改index.name
                x.index.name = '{}_{}'.format(x.index.name, i)
        df1 = df_new_copy[[key]].merge(df_old_copy[[key]], indicator=True, how='outer', on=key)
        # df1 = data_new[[key]].merge(data_old[[key]], indicator=True, how='outer', on=key)
        compare = {n: g[key].tolist() for n, g in df1.groupby('_merge')}
        if cols:
            common_columns = [cols] if isinstance(cols, str) else cols
        else:
            common_columns = list(set(df_new_copy.keys()).intersection(set(df_old_copy.keys())))
        common_index = compare['both']
        # 相同的index和columns才能比较
        df_diff = df_new_copy[common_columns].loc[common_index] != df_old_copy[common_columns].loc[common_index]
        df_diff = df_diff[df_diff].dropna(axis=1, how='all', inplace=False).dropna(axis=0, how='all', inplace=False)

        if func_skip and hasattr(func_skip, '__call__'):
            df_diff1 = df_diff.apply(func_skip, 1)
            list_diff = df_diff1[df_diff1 == False].index.tolist()
            items_skip = df_new_copy.loc[df_diff1[df_diff1 == True].index.tolist()].to_dict(orient='index')
        else:
            list_diff = df_diff.index.tolist()
            items_skip = []

        changed_cols = df_diff.keys().tolist()
        items_need_update = []
        for x in list_diff:
            items_need_update.append((df_new_copy.loc[x].to_dict(), df_old_copy.loc[x].to_dict()))
        items_new = df_new_copy.loc[compare['left_only']]
        items_new = set_none(items_new).to_dict(orient='index')
        items_miss = df_new_copy.loc[compare['right_only']].to_dict(orient='index')

        # items_new = df_new_copy.loc[df_diff.index].to_dict(orient='index')

        return {'data_new': data_new, 'data_old': data_old,
                'items_new': items_new, 'items_need_update': items_need_update,
                'items_miss': items_miss, 'items_skip': items_skip,
                'changed_cols': changed_cols}

    @check_writable
    @catch_sql_exception
    def update(self, data_new, data_old=None, key=None, cols=None, force_condition=None,
               action_add=True, action_delete=False, action_mark_miss=False, func_skip=None,
               param_query=None, param_add=None, param_update=None, param_delete=None,
               with_detail=False, with_sqls=False,
               timed=True, last=True,
               use_df=False, df_to_str=False,
               block=2000, dryrun=False, ret_str=False, fuzz_digit=True, **kwargs):
        '''
        :param data_new:    dict or list
        :param data_old:    dict or condition
        :param key: 需要一个unique的列作为key, 这个col也将作为query和delete的key
        :param cols: 指定比较某列, 如果为None, 自动发现
        :param force_condition: 强制条件，key may not the real unique, which is only valid in some spec condition.
            'ifName = xxxx and device = yyyy', extra_cond is 'device = yyyy'
        :param action_add: 添加新项
        :param action_delete: 删除缺失项
        :param action_mark_miss:  str, 如果mark_miss设置, 将不删除项, 转而设置col(mark_miss)=1
        :param func_skip:    func, 如果返回True, 则忽略更新
        :param param_query:    {condition=, key=}, query的参数
        :param param_add:
        :param param_update:
        :param param_delete:
        :param with_detail:  返回详细列表
        :param with_sqls:  返回详细sql
        :param timed:  添加更改时间，time_update
        :param last:  如果有last_<col>存在，自动更新
        :param use_df:  使用df方法
        :param df_to_str:  使用df时，两个df merge时, 类型不同会报错, 强制转成str
        :param block: 限制每次更新的项
        :param dryrun: 不实际执行
        :param ret_str: 返回结果string, 而不是OrderDict, 直接打印dict会乱序
        :param fuzz_digit: 比较时自动匹配str和int，mysql中使用int和str能能用
        :return:
        '''
        if not key:
            return False, 'no unique_key given.'
        if data_old is None:
            if param_query is None:
                param_query = {}
            if 'key' not in param_query:
                param_query['key'] = key
            is_ok, data_old = self.query(**param_query)
            assert is_ok, data_old
        if not param_add:
            param_add = {}
        if not param_update:
            param_update = {}
        if key and key not in param_update:
            param_update['key'] = key
        if not param_delete:
            param_delete = {}
        if not use_df and isinstance(data_new, pandas.core.frame.DataFrame):
            data_new = data_new.to_dict(orient='records')
        if isinstance(data_new, list):
            data_new = {x[key]: x for x in data_new}
        if isinstance(data_old, list):
            data_old = {x[key]: x for x in data_old}
        if use_df:
            d = self._update_compare_df(data_new, data_old, key, cols, func_skip, df_to_str)
        else:
            d = self._update_compare_dict(data_new, data_old, key, cols, func_skip, last=last, fuzz_digit=fuzz_digit)
        items_new, items_need_update, items_miss = d['items_new'], d['items_need_update'], d['items_miss']
        changed_cols = d['changed_cols']
        now = datetime_to_string()
        if items_new:
            if action_add and not dryrun:
                is_ok, msg = self.add_all(items_new, timed=timed, now=now, dryrun=dryrun, **param_add)
                assert is_ok, msg

        sqls = []
        if items_need_update:
            i = 0
            while True:
                if i > len(items_need_update):
                    break
                sub = items_need_update[i:i + block]
                is_ok, sql = self._update_case(changed_cols, sub, key, force_condition, timed=timed, now=now,
                                               dryrun=dryrun)
                assert is_ok, sql
                sqls.append(sql)
                i += block
        if items_miss:
            if action_mark_miss:
                cond = 'where {0} in ({1})'.format(key,
                                                   ','.join([add_quote(v[key]) for v in items_miss])) + \
                       'and {}'.format(force_condition) if force_condition else ''
                sql = 'update {0} set {1}=1 {2}'.format(self.model.__tablename__, action_mark_miss, cond)
                sqls.append(sql)
                if not dryrun:
                    r = self.session.execute(sql)
                    self.commit()
            elif action_delete:
                # self.session.query(self.model.id.in_((n,n,n))).all().delete()
                is_ok, result = self.delete(data=items_miss, batch=True, key=key,
                                            force_condition=force_condition,
                                            **param_delete)
                assert is_ok, result
                sqls.append(result['sql'])
        if timed and 'time_check' in self.cols_name:
            cond = 'where {0} in ({1})'.format(key, ','.join(add_quote(list(data_new.keys())))) + (
                ' and {}'.format(force_condition) if force_condition else '')
            sql2 = 'update {0} set `{1}`="{2}" {3}'.format(self.model.__tablename__, 'time_check', now, cond)
            if not dryrun:
                r = self.session.execute(sql2)
                self.commit()

        l = [('input', len(data_new)),
             ('exist', len(data_old) - len(items_miss)),
             ('skiped', len(d['items_skip'])),
             ('nochange', len(data_old) - len(items_miss) - len(items_need_update)),
             ('changed', len(items_need_update)),
             ('only_changed_last', d['only_change_last']),
             ('dryrun', dryrun),
             ('updated', len(items_need_update) if not dryrun else 0),
             ('action_add', action_add),
             ('new', len(items_new)),
             ('added', len(items_new) if action_add and not dryrun else 0),
             ('miss', len(items_miss)),
             ('action_delete', action_delete),
             ('deleted', len(items_miss) if action_delete and not dryrun else 0),
             ('action_mark_miss', action_mark_miss),
             ('marked', len(items_miss) if action_mark_miss and not dryrun else 0),
             ]

        if ret_str:
            r = ', '.join(['{}:{}'.format(k, v) for k, v in l])
        else:
            r = OrderedDict()
            for k, v in l:
                r[k] = v
            if with_detail:
                for k in ['items_new', 'items_miss', 'items_need_update', 'items_skip']:
                    r[k] = d[k]
            if with_sqls:
                r['sql'] = sqls

        return True, r

    def _update_case(self, cols, lst, key, force_condition, timed=True, now=None, time_col='time_update',
                     dryrun=False):
        '''
        :param cols:    cols should be modified
        :param lst:     [(data_new, data_old)]
        :param key:     index
        :param force_condition:
        :param timed:
        :param now:
        :param dryrun:
        :param time_col:
        :return:
        '''
        d = {c: [] for c in cols if c != key}
        l = []
        for i, x in enumerate(lst):
            new, old = x
            for c in cols:
                if c == key:
                    continue
                d[c].append('WHEN {0} THEN {1}'.format(self.add_quote_by_col_type(new[key], key),
                                                       self.add_quote_by_col_type(new[c], c)))
            l.append(self.add_quote_by_col_type(new[key], key))
        st = ',\n'.join([' `{0}` = case `{1}` '.format(k, key) + ' '.join(v) + ' END' for k, v in d.items()])
        cond = 'where {0} in ({1})'.format(key, ','.join(l)) + (
            ' and {}'.format(force_condition) if force_condition else '')
        sql = 'update {0}\n set {1} {2}'.format(self.model.__tablename__, st, cond)
        if not dryrun:
            r = self.session.execute(sql)
            self.commit()
        if timed and time_col in self.cols_name:
            sql1 = 'update {0} set `{1}`="{2}" {3}'.format(self.model.__tablename__, time_col, now, cond)
            if not dryrun:
                r = self.session.execute(sql1)
                self.commit()
        return True, sql

    @check_writable
    @catch_sql_exception
    def add(self, data, expandable=False, check=True):
        '''

        :param data:
        :param expandable:
        :param check: assure the data is not in data, called by update() with expandable set to False
        :return:
        '''
        if check:
            status, ex_obj = self.get_obj(data)
        else:
            ex_obj = None
        if ex_obj:
            if expandable:
                return self.update_one(data, ex_obj=ex_obj, expandable=False)
            else:
                return False, ex_obj.id
        data1 = {k: v for k, v in data.items() if k in self.cols_name}
        obj = self.model(**data1)
        self.session.add(obj)
        self.commit()
        return True, obj.id if hasattr(obj, 'id') else getattr(obj, self.cols_unique[0]) if self.cols_unique else ''

    @check_writable
    @catch_sql_exception
    def add_all(self, datas, block=1000, timed=True, now=None, time_col='time_insert', dryrun=False, **kwargs):
        '''
        :param datas:
        :param block:   分割块
        :param timed:   自动添加时间，time_insert
        :param now:   当前时间
        :param time_col:
        :param kwargs:
        :return:
        '''
        if timed and time_col in self.cols_name:
            if not now:
                now = datetime_to_string()
            for i, x in enumerate(datas):
                if time_col not in x.keys():
                    x[time_col] = now
                if 'time_update' in self.cols_name and 'time_update' not in x.keys():
                    x['time_update'] = now
        if block <= 0:
            block = 1000
        i = 0
        ids = []
        while True:
            _datas = datas[i * block:(i + 1) * block]
            if not _datas:
                break
            is_ok, _ids = self._add_all(_datas, **kwargs)
            assert is_ok, _ids
            ids.extend(_ids)
            i += 1
        return True, ids if kwargs.get('get_ids') else 'add all done.'

    @check_writable
    @catch_sql_exception
    def _add_all(self, datas, mode='save', get_ids=True):
        '''
        :param datas:
        :param mode: 'add' is operated one by one, return obj. 'save' is batch insert into data, no return by default
        :param get_ids: return ids of data
        :return:
        '''
        _datas = []
        for i, x in enumerate(datas):
            _datas.append({k: x.get(k) for k in self.cols_name})

        objs = [self.model(**_data) for _data in _datas]
        if mode == 'add':
            self.session.add_all(objs)
        elif mode == 'bulk':
            # python3 中会报 "string argument without an encoding"
            self.session.bulk_save_objects(objs)
        elif mode == 'save':

            cols = [k for k, v in datas[0].items() if k in self.cols_name]
            sql = 'insert into {} ({}) values {}'.format(
                self.model.__tablename__, ','.join(add_quote(cols, quote='`')),
                ','.join(['({})'.format(','.join(self.add_quote_by_col_type(x[k], k) for k in cols)) for x in datas]))
            # sql = sql.replace('None', 'null')
            self.session.execute(sql)
        self.commit()

        ids = []
        kw = None
        if not get_ids:
            return True, 'add all done.'
        if mode == 'save':
            for k in datas[0].keys():
                if datas[0][k] and k in self.cols_unique:
                    kw = k
                    break
            if not kw:
                return False, 'no unique key provided when query new items.'
            quote = False if isinstance(datas[0][kw], (int, float)) else True
            is_ok, resp = self.query(
                condition='{0} in ({1})'.format(kw,
                                                ','.join([('"{}"' if quote else '{}').format(x[k]) for x in datas])),
                key=kw)
            for i, x in enumerate(datas):
                id = resp.get(x[kw], {}).get('id')
                x['id'] = id
                ids.append(id)
        elif mode == 'add':
            for i, obj in enumerate(objs):
                datas[i]['id'] = obj.id
                ids.append(obj.id)
        return True, ids

    @check_writable
    @catch_sql_exception
    def delete(self, data=None, skip_miss=True, batch=True, key=None, condition=None, force_condition=None,
               dryrun=False):
        '''
        :param data:    can be dict/list of datas, data, obj
        :param skip_miss:   skip error that data not in data
        :param batch:   batch delete mode
        :param key:  set batch identifier in batch mode
        :param condition:  search condition when data is not given
        :param force_condition:  restrain the sql in the condition
        :param dryrun:
        :return:
        '''
        if isinstance(condition, str):
            if not re.match(r'where', condition.strip(), re.I):
                condition = 'where ' + condition
            sql1 = 'select * from {0} {1}'.format(self.model.__tablename__, condition)
            is_ok, items = self.query(sql=sql1)
            assert is_ok, items
            sql = 'delete from {0} {1}'.format(self.model.__tablename__, condition)
            self.session.execute(sql)
            self.commit()
            return True, {'msg': 'delete with condition done.', 'sql': sql, 'items': items}

        if batch:
            assert isinstance(data, (list, dict)), 'obj must be list or dict in batch delete mode.'
            for name, col in self.cols.items():
                if key == col.name:
                    pattern = '{0}' if isinstance(col.type, (INTEGER, FLOAT)) else '"{0}"'
                    break
            assert pattern, '{0} is not in model {1} when exec delete.'.format(key, self.model)
            if isinstance(data, list):
                values = ','.join(pattern.format(x[key]) for x in data)
            elif isinstance(data, dict):
                values = ','.join(pattern.format(v[key]) for k, v in data.items())
            sql = 'delete from `{0}` where `{1}` in ({2})'.format(self.model.__tablename__, key, values)
            sql += 'and {}'.format(force_condition) if force_condition else ''
            sql1 = re.sub('^delete from', 'select * from', sql, re.I)
            is_ok, items = self.query(sql=sql1)
            assert is_ok, items
            if not dryrun:
                self.session.execute(sql)
                # self.flush()
                self.commit()
            return True, {'msg': 'batch delete done.', 'sql': sql, 'items': items}
        else:
            if isinstance(data, dict):
                status, ex_obj = self.get_obj(data)
                if not ex_obj:
                    return skip_miss, 'data not exist.'
                obj = ex_obj
            else:
                obj = data
            if isinstance(obj, self.model):
                self.session.delete(obj)
                self.commit()
            return True, {'msg': 'obj delete done.', 'sql': '', 'items': obj.id}

    def commit(self):
        self.session.flush()
        self.session.commit()

    def close(self):
        self.session.close()

    @catch_sql_exception
    def search(self, q='', col=None, key=None, joint='-'):
        '''
        用self.query代替！！
        :param q: condition.
        :param key: can be str or list
        :param joint: if key is a list, the key of dict returned is concatenated by joint
        :return: return dict if key is set, else list
        '''
        if col is None:
            col_str = '*'
        elif isinstance(col, str):
            col_str = '`{}`'.format(col)
        elif isinstance(col, list):
            col_str = ','.join('`{}`'.format(x) for x in col)
        else:
            return False, 'wrong col name'
        sql = "select {0} from `{1}` {2} {3}".format(col_str, self.model.__tablename__, 'where' if q else '',
                                                     q)
        result = self.session.execute(sql)
        rows = result.fetchall()
        if not isinstance(rows, list):
            return False, 'search fail. SQL <{}>'.format(sql)

        if not key:
            datas = []
            if rows and len(rows) > 0:
                cols = rows[0]._parent.keys
                for i, row in enumerate(rows):
                    datas.append({col: row[j] for j, col in enumerate(cols)})
        else:
            datas = {}
            if rows and len(rows) > 0:
                cols = rows[0]._parent.keys
                keys = [key] if isinstance(key, str) else key
                keys_id = []
                for k in keys:
                    if k not in cols:
                        return False, '{0} is not in cols {1}'.format(k, ','.join(cols))
                    keys_id.append(cols.index(key))
                for i, row in enumerate(rows):
                    # k1 may be int, which can't be join
                    k1 = row[keys_id[0]] if len(keys_id) == 1 else joint.join(str(row[k]) for k in keys_id)
                    datas[k1] = {col: row[j] for j, col in enumerate(cols)}
        return True, datas

    @catch_sql_exception
    def _query1(self, sql, key=None, joint='-'):
        '''
        query1使用传统方式，可以在query2中返回同样的类型
        :param sql
        :param key: 可以是str或list
        :param joint: if key is a list, the key of dict returned is concatenated by joint
        :return: return dict if key is set, else list
        '''
        result = self.session.execute(sql)
        rows = result.fetchall()
        columns = result.keys()
        if not key:
            datas = []
            if rows and len(rows) > 0:
                cols = rows[0]._parent.keys
                for i, row in enumerate(rows):
                    datas.append({col: row[j] for j, col in enumerate(cols)})
        else:
            datas = {}
            if rows and len(rows) > 0:
                cols = rows[0]._parent.keys
                keys = [key] if isinstance(key, str) else key
                keys_id = []
                for k in keys:
                    if k not in cols:
                        return False, '{0} is not in cols {1}'.format(k, ','.join(cols))
                    keys_id.append(cols.index(key))
                for i, row in enumerate(rows):
                    k1 = joint.join(str(row[k]) for k in keys_id)
                    datas[k1] = {col: row[j] for j, col in enumerate(cols)}
        return True, datas

    def _read_sql_query(self, sql, index_col=None, coerce_float=True, parse_dates=None):
        # pd.read_sql_query不受self.session控制
        result = self.session.execute(sql)
        rows = result.fetchall()
        columns = result.keys()
        df = pd.DataFrame.from_records(rows, columns=columns, coerce_float=coerce_float)
        df = pd.io.sql._parse_date_columns(df, parse_dates)
        if index_col is not None:
            df.set_index(index_col, inplace=True)

        return df

    def _query2(self, sql, ret_df=False, key=None, joint='-', apply_func=None, **kwargs):
        '''
        query2使用pandas方式
        :param sql:
        :param key:  str or list. different with 'index_col' in read_sql_query is this param will reserve the index in value.
        :param joint: if key is a list, the key of dict returned is concatenated by joint
        :param apply_func: apply函数，生成key
        :param kwargs: read_sql_query的kwargs
            index_col=['device_id]
            parse_dates=['finance_pay_date']
        :return:
        如果列col是int，但有None，该col会被返回成float
        '''
        df = self._read_sql_query(sql, **kwargs)
        df = set_none(df)
        cols = df.columns.tolist()
        if ret_df:
            if apply_func:
                df.index = df.apply(apply_func, axis=1)
            if key:
                df.index = df.apply(lambda x: joint.join(x[k] for k in key) if isinstance(key, list) else x[key],
                                    axis=1)
            return True, df
        else:
            if not key:
                return True, df.to_dict(orient='record')
            else:
                if isinstance(key, list):
                    d = {joint.join([str(v[i]) for i in key]): v for k, v in df.to_dict(orient='index').items()}
                else:
                    d = {v[key]: v for k, v in df.to_dict(orient='index').items()}
                return True, d

    @catch_sql_exception
    def query(self, sql=None, table=None, condition='', cols=None, limit=None, orderby=None, use_pd=True,
              with_sql=False, **kwargs):
        '''
        使用_query2时, 如果sql中有%, 需要使用%%
        query2('table', '', ['a', ('b', 'b1')]) -> select a, b as b1 from table
        :param sql:   如果sql为空，则用后面的args组合sql
        :param table:   __tablename__
        :param condition:   <where> xxxx
        :param cols:    需要查询的columns， ['a', ('b', 'b1')]
        :param kwargs:
            ret_df  是否返回dataframe
        :param use_pd: 使用pandas(query2)，read_sql_query(sql, con=self.engine, **kwargs)，con不受控制
        :return:
        '''
        if not sql:
            if not table:
                table = self.model.__tablename__
            if condition and not condition.strip().lower().split()[0] in ['where', 'left', 'right', 'join']:
                condition = 'where ' + condition
            if not cols:
                cols1 = '*'
                cols2 = None
            else:
                cols1 = ', '.join('{0} as {1}'.format(x[0], x[1]) if isinstance(x, tuple) else x for x in cols)
                cols2 = [x[1] if isinstance(x, tuple) else x for x in cols]

            sql = 'select {0} from {1} {2} {3} {4}'.format(cols1, table, condition,
                                                           'order by {}'.format(orderby) if orderby else '',
                                                           'limit {}'.format(limit) if limit else '')
        is_ok, result = self._query2(sql, **kwargs) if use_pd else self._query1(sql, **kwargs)
        if with_sql:
            return is_ok, {'data': result, 'sql': sql}
        else:
            return is_ok, result

    @catch_sql_exception
    def call_procedure(self, name, *args):
        '''
        :param name:
        :param args:
        :return:
        '''
        sql = 'call {}({})'.format(name, ','.join(add_quote(args, to_str=False)))
        res = self.session.execute(sql)
        try:
            res = res.fetchall()
        except:
            res = 'no return'
        self.commit()
        return True, res

    def create_table(self):
        # for x in self.model.metadata.tables.values():
        return self.model.metadata.create_all(self.engine, tables=[self.model.__table__])
        # return None, to_sql可以直接创建
