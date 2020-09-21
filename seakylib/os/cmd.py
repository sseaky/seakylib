#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/9/24 10:49

'''
execute_sql_remote(sql, db_host='localhost', db_port=3306, db_user, db_pass, db_name,
    ssh_host, ssh_port=22, ssh_user, ssh_pkey/ssh_keyfile)
'''

import os
import re
from pathlib import Path
from subprocess import run, PIPE, STDOUT

from ..func.base import MyClass

PRINT_ERROR = True


def execute(cmd, stdout=PIPE, stderr=STDOUT, encoding='utf-8', shell=False, *args, **kwargs):
    '''
    subprocess.run(args, *, stdin=None, input=None, stdout=None, stderr=None, shell=False, timeout=None, check=False)
        stdout: subprocess.PIPE
        stderr: subprocess.PIPE / subprocess.STDOUT
        shell:  True-cmd可以为str，False-cmd需要是list
        check:  True-如果返回不为0，抛出异常
        env:    {}
        cwd:    /
    :param cmd:
    :return:    subprocess.CompletedProcess
        args
        returncode
        stdout: binary
        stderr: binary
    '''
    if isinstance(cmd, str):
        shell = True
    p = run(cmd, stdout=stdout, stderr=stderr, shell=shell, *args, **kwargs)
    if isinstance(p.stdout, bytes):
        p.stdout = p.stdout.decode(encoding)
    if isinstance(p.stderr, bytes):
        p.stderr = p.stderr.decode(encoding)
    return p


def make_ssh_cmd(cmd, ssh_host, ssh_port=22, ssh_user=None, ssh_keyfile=None, env=None, escape='`"', ssh_options=None,
                 ssh_quite=False, **kwargs):
    '''
    :param ssh_host:
    :param cmd:
    :param ssh_port:
    :param ssh_user:
    :param ssh_keyfile:
    :param env:
    :param escape:  替换 ` "
    :param ssh_options:  {'StrictHostKeyChecking': 'no', 'UserKnownHostsFile': '/dev/null'}
    :param ssh_quite:  ssh -q, 对于执行sql，warning信息会干扰结果的自动解析
    :param kwargs:
    :return:
    '''
    if isinstance(escape, str):
        for es in list(escape):
            cmd = re.sub(r'(?<!\\){}'.format(es), '\\{}'.format(es), cmd)
    cmd1 = 'ssh '
    if ssh_quite:
        cmd1 += '-q '
    cmd1 += "-p {} ".format(ssh_port)
    if ssh_user:
        cmd1 += "-l {} ".format(ssh_user)
    if ssh_keyfile:
        # -i 加引号在win10下会找不到keyfile
        cmd1 += "-i {} ".format(ssh_keyfile)
    if ssh_options is None:
        ssh_options = {'StrictHostKeyChecking': 'no'}
    if isinstance(ssh_options, dict):
        for k, v in ssh_options.items():
            cmd1 += '-o {}={} '.format(k, v)
    cmd1 += ssh_host
    if isinstance(env, dict):
        cmd = ''.join("export {}='{}' && ".format(k, v) for k, v in env.items()) + cmd
    cmd2 = '{} "{}"'.format(cmd1, cmd)
    return cmd2


def make_cmd_sql(sql, db_user, db_pass, db_name, db_host='localhost', db_port=3306, **kwargs):
    '''
    :param sql: 语句中都使用双引号
    :param db_user:
    :param db_pass:
    :param db_name:
    :param db_host:
    :param db_port:
    :return:
    '''
    cmd = 'export MYSQL_PWD=\'{}\' && mysql -h {} -P {} -u {} {} -e \'{}\''.format(db_pass, db_host, db_port, db_user,
                                                                                   db_name, sql)
    return cmd


def make_cmd_ssh_sql(**kwargs):
    return make_ssh_cmd(cmd=make_cmd_sql(**kwargs), **kwargs)


def execute_sql(sql, print_error=PRINT_ERROR, columns_func=False, **kwargs):
    '''
    执行命令行sql
    :param sql:
    :param print_error:
    :param columns_func:
    :param kwargs:
    :return:
    '''
    p = execute(make_cmd_sql(sql, **kwargs))
    p.sql = sql
    if p.stdout.startswith('ERROR '):
        if print_error:
            print(p.stdout)
    return parse_execute_sql_result(p, columns_func=columns_func)


def execute_ssh_cmd(cmd, ssh_temp_key_dir=None, **kwargs):
    '''
    执行远程ssh命令
    :param ssh_temp_key_dir: 临时保存key的目录
    :param kwargs:
        ssh_pkey, 可以传入key字串，如www-data调用时受权限限制，需要临时保存key
    :return:
    '''
    temp_keyfile = False
    if kwargs.get('ssh_pkey'):
        if not ssh_temp_key_dir:
            ssh_temp_key_dir = Path(os.environ['HOME']) / '.ssh'
        ssh_temp_key_dir = Path(ssh_temp_key_dir)
        if not ssh_temp_key_dir.exists():
            ssh_temp_key_dir.mkdir()
        ssh_keyfile = '{}/cmd_temp_key'.format(ssh_temp_key_dir)
        open(ssh_keyfile, 'w').write(kwargs['ssh_pkey'])
        execute('chmod 600 {}'.format(ssh_keyfile))
        kwargs['ssh_keyfile'] = ssh_keyfile
        temp_keyfile = True

    p = execute(make_ssh_cmd(cmd=cmd, **kwargs))
    if temp_keyfile:
        os.remove(ssh_keyfile)
    return p


def execute_sql_remote(sql, print_error=PRINT_ERROR, columns_func=False, **kwargs):
    '''
    通过ssh查询远程数据库，提供make_cmd_sql，make_cmd_ssh的参数, sql语句中使用"
    :param sql:
    :param print_error:
    :param columns_func:
    :param kwargs:
    :return:
    '''
    kwargs['ssh_quite'] = True
    p = execute_ssh_cmd(cmd=make_cmd_sql(sql, **kwargs), **kwargs)
    p.sql = sql
    if p.stdout.startswith('ERROR '):
        if print_error:
            print(p.stdout)
    return parse_execute_sql_result(p, columns_func=columns_func)


def parse_execute_sql_result(p, columns_func=None):
    if p.returncode == 0:
        p.result = []
        if p.stdout:
            p.result.extend(parse_sql_result(p.stdout, columns_func=columns_func))
    return p


def parse_sql_result(s, columns_func=None):
    '''
    解析命令行的sql结果，也可以从文件读取
    :param s:
    :param columns_func:
    :return:
    '''
    l = []
    for i, line in enumerate(s.split('\n')):
        if i == 0:
            keys = line.split('\t')
            continue
        if not line.strip():
            continue
        l.append({keys[j]: None if v == 'NULL' else v for j, v in enumerate(line.split('\t'))})
    data = column_map(data=l, columns_func=columns_func)
    return data


def column_map(data, columns_func=None, to_sql=False):
    '''
    转换sql查询结果的col类型
    :param data:    [{}, {}, ...]
    :param columns_func:    {'col1': int, 'col2': func, ...}
    :param to_sql:    构造sql时，没有指定，一律视为str，加上""
    :return:
    '''

    def to_sql_func(v):
        if v is None:
            return 'NULL'
        if isinstance(v, str):
            if v.startswith('"'):
                return v
            v.replace('"', '\"')
            return '"{}"'.format(v)
        else:
            return str(v)

    if not isinstance(columns_func, dict):
        return data
    # 如果直接在data上面改，多次重复可能会导致异常
    data1 = []
    for i, d in enumerate(data):
        d1 = {}
        for k, v in d.items():
            d1[k] = v
            if columns_func and k in columns_func:
                if v is None:
                    d1[k] = None
                else:
                    d1[k] = columns_func[k](v)
            if to_sql:
                d1[k] = to_sql_func(v)
        data1.append(d1)
    return data1


class OSSQL(MyClass):
    def __init__(self, db_user=None, db_pass=None, db_name=None, db_host='localhost', db_port=3306,
                 ssh_host=None, ssh_port=22, ssh_user=None, ssh_keyfile=None, env=None,
                 escape='`"', ssh_options=None, ssh_quite=False, *args, **kwargs):
        MyClass.__init__(self, *args, **kwargs)
        self.db_auth = {'db_user': db_user, 'db_pass': db_pass, 'db_name': db_name, 'db_host': db_host,
                        'db_port': db_port}

        if ssh_host:
            self.ssh_auth = {'ssh_host': ssh_host, 'ssh_port': ssh_port, 'ssh_user': ssh_user,
                             'ssh_keyfile': ssh_keyfile,
                             'env': env, 'escape': escape, 'ssh_options': ssh_options, 'ssh_quite': ssh_quite}
        else:
            self.ssh_auth = None
        self.table_columns_info = {}

    def show_server(self):
        msg = 'MySQL://{db_user}@{db_host}:{db_port}/{db_name}'.format(**self.db_auth)
        if self.ssh_auth:
            msg += ' via SSH://{ssh_user}@{ssh_host}:{ssh_port}'.format(**self.ssh_auth)
        self.log.info(msg)

    def exec(self, sql, print_error=PRINT_ERROR, columns_func=None, dry_run=False, silence=False):
        '''
        :param sql:
        :param print_error:
        :param columns_func:
        :param dry_run:
        :param silence:  不显示debug信息，主要对于一些批量操作
        :return:
        '''
        if not silence:
            self.log.debug(sql)
        if dry_run:
            return True
        if self.ssh_auth:
            r = execute_sql_remote(sql, print_error=print_error, columns_func=columns_func, **self.db_auth,
                                   **self.ssh_auth)
        else:
            r = execute_sql(sql, print_error=print_error, columns_func=columns_func, **self.db_auth)
        return r

    def query(self, sql, columns_func=None, auto=True):
        '''

        :param sql:
        :param columns_func:
        :param auto:
        :return:
        '''
        if auto:
            m = re.search('select (?P<cols>[\s\S]+?) from\s+(?P<table_name>\S+)($|\s+)',
                          re.sub('[\s\r\n]+', ' ', sql), re.I)
            cols, tn = m.group('cols'), m.group('table_name')
            tn = tn.replace('`', '').split('.')
            if len(tn) == 2:
                db_name, table_name = tn
            else:
                db_name, table_name = None, tn[0]
            if re.search('^count', cols, re.I):
                columns_func = None
            else:
                columns_func = self.auto_generate_columns_func(table_name=table_name, db_name=db_name,
                                                               columns_func=columns_func)
        p = self.exec(sql, columns_func=columns_func)
        return p

    def flush(self, *args, **kwargs):
        '''
        整表更新
        :param args:
        :param kwargs:
        :return:
        '''
        kwargs['truncate'] = True
        return self.insert(*args, **kwargs)

    def insert(self, table_name, data, db_name=None, columns=None, columns_func=None, chunk=1000, limit=None,
               truncate=False):
        '''
        可能会报 Argument list too long 错误，需要减小chunk
        :param table_name:
        :param data:
        :param db_name:
        :param columns:  可以从 data[0] 中提取
        :param columns_func:    处理value，默认添加"", {'col2': int, 'col4: func}
        :param chunk:
        :param limit:
        :param truncate:
        :return:
        '''
        if not columns:
            if isinstance(data, list) and isinstance(data[0], dict):
                table_cols = self.get_table_columns_info(table_name=table_name).keys()
                columns = [x for x in list(data[0].keys()) if x in table_cols]
        assert columns, 'no columns given when perform insert'

        if db_name is None:
            db_name = self.db_auth['db_name']
        sql = 'INSERT INTO `{}`.`{}` ( {} ) VALUES'.format(db_name, table_name,
                                                           ','.join(['`{}`'.format(x) for x in columns]))

        if isinstance(data, dict):
            item0 = data[data.keys()[0]]
            if isinstance(item0, (list, tuple)):
                data1 = [{c: v[i] for i, c in enumerate(columns)} for k, v in data.items()]
            elif isinstance(item0, dict):
                data1 = [v for k, v in data.items()]
        elif isinstance(data, list):
            item0 = data[0]
            if isinstance(item0, (list, tuple)):
                data1 = [{c: v[i] for i, c in enumerate(columns)} for v in data]
            elif isinstance(item0, dict):
                data1 = data
        else:
            assert False, 'data is not acceptable.'

        columns_func = self.auto_generate_columns_func(table_name=table_name, columns_func=columns_func)

        data1 = column_map(data1, columns_func, to_sql=True)
        if isinstance(limit, int):
            data1 = data1[:limit]

        if truncate:
            cmd = 'TRUNCATE {}'.format(table_name)
            self.exec(cmd)

        self.log.debug('insert {} items into table {}.{}'.format(len(data), db_name, table_name))

        for i in range(0, len(data1), chunk):
            _data = data1[i:i + chunk]
            records = []
            for j, x in enumerate(_data):
                vs = []
                for k, col in enumerate(columns):
                    vs.append(x[col])
                records.append('({})'.format(','.join(vs)))
            _sql = sql
            _sql += ', '.join(records)
            self.exec(_sql, print_error=True, silence=True)
        return True

    def get_table_columns_info(self, table_name, db_name=None):
        if db_name is None:
            db_name = self.db_auth['db_name']
        full_name = '{}_{}'.format(db_name, table_name)
        if self.table_columns_info.get(full_name):
            info = self.table_columns_info[full_name]
        else:
            sql = 'select * from information_schema.`COLUMNS` where TABLE_SCHEMA = "{}" and TABLE_NAME = "{}"'.format(
                db_name, table_name)
            p = self.query(sql, auto=False)
            info = p.result
            self.table_columns_info[full_name] = info

        cols = {x['COLUMN_NAME']: x for x in info}
        return cols

    def auto_generate_columns_func(self, table_name, db_name=None, columns_func=None):
        '''
        自动产生columns map
        :param table_name:
        :param db_name:
        :param columns_func:
        :return:
        '''
        cols = self.get_table_columns_info(table_name=table_name, db_name=db_name)
        if not columns_func:
            columns_func = {}
        for col, v in cols.items():
            if v['DATA_TYPE'] == 'int':
                if col not in columns_func:
                    columns_func[col] = int
            # elif v['DATA_TYPE'] == 'varchar':
            #     if col not in columns_func:
            #         columns_func[col] = int
        return columns_func

    def count_table(self, table_name):
        # self.log.debug('count table {}'.format(table_name))
        return int(self.query('select count(*) as n from {}'.format(table_name)).result[0]['n'])
