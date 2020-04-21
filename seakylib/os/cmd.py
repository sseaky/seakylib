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
        cmd1 += "-i '{}' ".format(ssh_keyfile)
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


def execute_sql(**kwargs):
    '''
    执行命令行sql
    :param kwargs:
    :return:
    '''
    p = execute(make_cmd_sql(**kwargs))
    return parse_execute_sql_result(p)


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


def execute_sql_remote(sql, **kwargs):
    '''
    通过ssh查询远程数据库，提供make_cmd_sql，make_cmd_ssh的参数, sql语句中使用"
    :param kwargs:
        sql
    :return:
    '''
    kwargs['ssh_quite'] = True
    p = execute_ssh_cmd(cmd=make_cmd_sql(sql, **kwargs), **kwargs)
    return parse_execute_sql_result(p)


def parse_execute_sql_result(p):
    if p.returncode == 0:
        p.result = []
        if p.stdout:
            p.result.extend(parse_sql_result(p.stdout))
    return p


def parse_sql_result(s):
    '''
    解析命令行的sql结果，也可以从文件读取
    :param s:
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
    return l
