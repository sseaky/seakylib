#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/9/24 10:49

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


def make_cmd_ssh(ssh_host, cmd, ssh_port=22, ssh_user=None, ssh_key=None, env=None, **kwargs):
    '''
    :param host:
    :param cmd:
    :param port:
    :param ssh_user:
    :param ssh_key:
    :param env:
    :param args:
    :param kwargs:
    :return:
    '''
    # escape cmd中的"为\"
    cmd = cmd.replace('"', '\\"')
    cmd1 = "ssh -p {} ".format(ssh_port)
    if ssh_user:
        cmd1 += "-l {} ".format(ssh_user)
    if ssh_key:
        cmd1 += "-i '{}' ".format(ssh_key)
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
    return make_cmd_ssh(cmd=make_cmd_sql(**kwargs), **kwargs)
