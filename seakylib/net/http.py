#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/6/25 10:00

import json
import random
import re
import warnings
from functools import wraps
from pathlib import Path
from urllib import parse

import requests
from bs4 import BeautifulSoup

from ..func.base import MyClass
from ..func.mrun import MultiRun
from ..func.parser import ArgParseClass

warnings.filterwarnings("ignore")

UA = {'ie': 'Mozilla/5.0 (MSIE 10.0; Windows NT 6.1; Trident/5.0)',
      'chrome': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
      'firefox': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1',
      'iphone': 'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5',
      'android': 'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
      'wx': 'Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/11B554a MicroMessenger/6.2.1'}

HTTP_TIMEOUT = 40
HTTP_RETRY = 3


class Http(MyClass):
    def __init__(self, browser_random=False, browser='chrome', http_timeout=HTTP_TIMEOUT, ssl_verify=False,
                 http_proxy=None, http_headers=None,
                 http_retry=HTTP_RETRY, *args, **kwargs):
        '''
        :param ua_random:   随机UA
        :param ua:   ie/chrome/firefox/iphone/android/wx
        :param http_timeout:
        :param ssl_verify:
        :param http_proxy:  str, '127.0.0.1:8087', socks代理使用net.proxy库
        :param args:
        :param kwargs:
        '''
        MyClass.__init__(self, *args, **kwargs)
        self.kwargs = kwargs
        self.control['browser'] = browser
        self.control['browser_random'] = browser_random
        self.control['http_timeout'] = http_timeout
        self.control['http_retry'] = http_retry
        self.control['ssl_verify'] = ssl_verify
        self.control['http_proxy'] = {'http': http_proxy, 'https': http_proxy} if isinstance(http_proxy, str) else None

        self.session = requests.session()
        if not browser_random:
            self.session.headers.update({'User-Agent': UA.get(browser, 'chrome')})
        else:
            tag, ua = random.choice(list(UA.items()))
            self.session.headers.update({'User-Agent': ua})
        if isinstance(http_headers, dict):
            self.session.headers.update(http_headers)
        # self.status = {}  # for some vars preserve
        self.url_root = kwargs.get('url_root')

    def __getattr__(self, item):
        return self.__dict__.get(item, self.kwargs.get(item))

    def fetch(self, url, ret_bs=True, method='GET', timeout=None,
              retry=3, retry_code=None, retry_not_200=False,
              ret_raw=False, ret_dic=False, ret_json=False, charset=None, **kwargs):
        '''
        :param url:
        :param ret_bs: 返回bs4 obj
        :param ret_raw: 返回原始数据
        :param method: GET、POST
        :param retry: 重试次数
        :param retry_code: 重试某些code
        :param retry_not_200: 重试非200
        :param ret_dic: 返回url, args, result的字典
        :param charset:
        :param kwargs:
            get-params, post-data
        :return:
        '''
        d = {'verify': self.default('ssl_verify'), 'timeout': timeout or self.default('http_timeout'),
             'proxies': self.default('http_proxy')}
        retries = retry or self.default('http_retry')
        d.update(kwargs)
        if not re.search(r'^http', url, re.I) and self.url_root:
            url = re.sub('/*$', '', self.url_root) + '/' + re.sub('^/*', '', url)
        flag = False
        retry_code = retry_code or []
        code_reason = ''
        for i in range(retry):
            try:
                _raw = self.session.post(url, **d) if method == 'POST' else self.session.get(url, **d)
                code = _raw.status_code
                if retry_not_200 and code != 200:
                    code_reason = code
                    continue
                elif code in retry_code:
                    code_reason = code
                    continue
                else:
                    flag = True
                    url = _raw.url
                    break
            except Exception as e:
                error = e
        if not flag:
            if code_reason:
                raise Exception('fetch {} fail, status_code={}. {}'.format(url, code_reason, d))
            else:
                raise Exception('fetch {} error, {}. {}'.format(url, error, d))
        if ret_raw:
            self.fetch_after(_raw, None, None)
            return {'result': _raw, 'url': url, 'kwargs': d} if ret_dic else _raw
        if ret_json:
            return _raw.json()
        _raw = _raw.content
        if not charset:
            m = re.search('charset=\W*(?P<charset>\w+)', _raw[:200].decode(errors='ignore'))
            charset = m.groupdict().get('charset', 'utf-8') if m else 'utf-8'
        if charset == 'gb2312':
            charset = 'cp936'
        _content = _raw.decode(encoding=charset, errors='ignore')
        bs = BeautifulSoup(_content, features=self.kwargs.get('features', 'html.parser'))
        self.fetch_after(_raw, _content, bs)
        ret = bs if ret_bs else _content
        return {'result': ret, 'url': url, 'kwargs': d} if ret_dic else ret

    def multi_job(self, *args, **kwargs):
        '''
        多进程的job
        :return:
        '''
        return True, self.fetch(*args, **kwargs)

    def multi_fetch(self, kws, process_num=20, process_time=None, inline=False):
        '''
        :param kws: [{'local_graph_id': xxx}]
        :param process_num: [{'local_graph_id': xxx}]
        :param process_time: [{'local_graph_id': xxx}]
        :param inline: 错误继续
        :return:
        '''
        mr = MultiRun(func=self.multi_job, func_kws=kws, log=self.log, add_log_to_common_kw=False,
                      process_num=process_num,
                      verbose=self.verbose, debug=self.debug)
        is_ok, results = mr.run(mrun_save=False, process_timeout=process_time, inline=inline)
        return is_ok, results

    def fetch_after(self, *args):
        '''
        每次交互后，可能需要执行的动作，如获取token
        :param args:
        :return:
        '''
        return True

    def get(self, *args, **kwargs):
        return self.fetch(method='GET', *args, **kwargs)

    def post(self, *args, **kwargs):
        return self.fetch(method='POST', *args, **kwargs)

    def save_stat(self, fn=None):
        fn = fn or 'status.json'
        json.dump(
            {'cookies': self.session.cookies.get_dict()
             # , 'status': self.status
             },
            open(str(fn), 'w'), sort_keys=True, indent=True)

    def load_stat(self, fn=None):
        fn = fn or 'status.json'
        if Path(fn).exists():
            d = json.load(open(str(fn)))
            self.session.cookies.update(d['cookies'])
            # self.status.update(d['status'])
            return True

    def login(self, load=True, save=True):
        '''登陆'''
        if load and self.load_stat():
            if self.login_verify():
                self.cache['login'] = True
                return True
        if self.login_action():
            save and self.save_stat()
            self.cache['login'] = True
            return True

    def login_action(self):
        '''
        实际登陆过程，重写
        :return:
        '''
        pass

    def login_verify(self):
        '''
        self.login()时，验证load数据
        :return:
        '''
        pass

    def job(self):
        pass

    def run(self):
        self.job()


def login_check(f):
    @wraps(f)
    def wrap(self, *args, **kwargs):
        if self.cache.get('login'):
            return f(self, *args, **kwargs)
        else:
            return False

    return wrap


def url2list(url):
    # 转dict需要注意相同的key会被覆盖
    return parse.parse_qsl(url)


class HttpArgParse(ArgParseClass):
    def __init__(self, *args, **kwargs):
        ArgParseClass.__init__(self, *args, **kwargs)

    def add_http(self, group='Http', http_timeout=HTTP_TIMEOUT, http_retry=HTTP_RETRY, browser='chrome'):
        self.add('--http_timeout', type=int, default=http_timeout, help='http超时时间，{}'.format(http_timeout), group=group)
        self.add('--http_retry', type=int, default=http_retry, help='重试次数, {}'.format(http_retry), group=group)
        self.add('--browser', default=browser,
                 help='浏览器，ie/chrome/firefox/iphone/android/wx'.format(http_retry), group=group)


if __name__ == '__main__':
    pass
