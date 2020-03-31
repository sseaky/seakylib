#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/20 14:57

import re


def comma_digit(s, tp=int):
    if str_is_number(s, tp, exp=True):
        return '{:,}'.format(int(s))
    return s


def str_is_number(s, tp=None, exp=True):
    '''

    :param s:
    :param tp: 指定int/float，None两者都行
    :param exp: 扩展，如果s是int/float，也可以返回true
    :return:
    '''
    if not isinstance(s, str):
        if exp and isinstance(s, int) and tp in (None, int):
            return True
        if exp and isinstance(s, float) and tp in (None, float):
            return True
        return False
    if tp == int:
        return re.match('^\-{,1}\d+\.{0,0}\d+$', s)
    elif tp == float:
        return re.match('^\-{,1}\d+\.{1,1}\d+$', s)
    else:
        return re.match('^\-{,1}\d+\.{,1}\d+$', s)


def str_is_email_address(s):
    if isinstance(s, str):
        return re.search('^(?P<id>.+)@(?P<domain>[\w\d]+\.([\w\d]+\.){0,}\w+$)', s)


def str2list(s, sep=',', presave_blank=False, wrapper='', filter=None):
    '''
    分割字符串，添加引号
    :param s:
    :param presave_blank:   是否保留空的item
    :param wrapper: '/"
    :param filter:  过滤回调函数，True过滤
    :return:
    '''
    if s is None:
        return []
    elif isinstance(s, str):
        l = []
        for x in s.split(sep):
            y = x.strip()
            if not presave_blank and not y:
                continue
            if filter and hasattr(filter, '__call__') and not filter(y):
                continue
            l.append('{0}{1}{0}'.format(wrapper, y))
        return l
    elif isinstance(s, list):
        return ['{0}{1}{0}'.format(wrapper, str(y)) for y in s]
    else:
        return s


def list2str(l, sep=',', wrapper='', *args, **kwargs):
    if isinstance(l, list):
        return sep.join(['{0}{1}{0}'.format(wrapper, x) for x in l])
    elif isinstance(l, str):
        l1 = str2list(l, sep=sep, *args, **kwargs)
        return list2str(l1, sep=sep, wrapper=wrapper)
    else:
        return l


def arg2list(obj):
    if obj is None:
        return []
    elif not isinstance(obj, list):
        return [obj]
    return obj


def bytes_decode(v, enconding='utf-8', errors='strict', **kwargs):
    '''
    convert v to spec type, default str, puresnmp use it.
    :param v:
    :param to_type:
    :param enconding:
    :param errors:
    :return:
    '''
    if isinstance(v, bytes):
        try:
            v = bytes.decode(v, encoding=enconding, errors=errors)
        except Exception as e:
            # if v is hex bytes
            v = v.hex()
    return change_type(v, **kwargs)


def change_type(v, to_type=None, default=None, strip=True):
    '''
    转换类型，如果有指定，但转换不了，返回default或原值，如果无指定，刚自动匹配
    :param v:
    :param _type:
    :param default: 默认返回，None则返回原值
    :param strip:
    :return:
    '''
    if to_type:
        try:
            if to_type in [str, 'str']:
                return str(v).strip() if strip else str(v)
            elif to_type in [int, 'int']:
                return int(v)
            elif to_type in [float, 'float']:
                return float(v)
        except Exception as e:
            return default if default is not None else v
    else:
        if str_is_number(v, int):
            return int(v)
        elif str_is_number(v, float):
            return float(v)
        return v


def replace(s, pats=None, default='', ret_with_pat=False, _any=False, flags=0, escape=False):
    '''
    :param s:
    :param pats:    替换特征, [(old, new)], 如果
    :param default:  如果传入的pats列表是str, 则用default的值进行替换
    :param ret_with_pat:  是否返回匹配列表
    :param _any:  匹配任意结束
    :param flags:  re flags
    :param escape:  传入的pats需要强制escape
    :return:
    '''
    if not pats:
        pats = [(r'[\r\n]+', '\n')]
    if isinstance(pats[0], str):
        pats = [(x, default) for x in pats]
    if escape:
        pats = [(re.escape(x1), x2) for x1, x2 in pats]
    _pats = '' if _any else []
    s1 = s
    for pat, rep in pats:
        s = re.sub(pat, rep, s, flags=flags)
        # 如果_any==True, 返回的_pats为str, 否则是list
        if s != s1:
            pat = pat.replace('\\', '')
            s1 = s
            if _any:
                _pats = pat
                break
            else:
                _pats.append(pat)
    if ret_with_pat:
        return s, _pats
    else:
        return s


def windows_filename(s, full=False, space=True):
    '''
    windows命名
    :param s:
    :param full: 用全角代替, 否则用_代替
    :param space: 是否替换空格
    :return:
    '''
    pats = [(r'\\', '＼'), (r'/', '／'), (r':', '：'), (r'\*', '＊'), (r'\?', '？'), (r'"', '＂'), (r'<', '＜'),
            (r'>', '＞'), (r'\|', '｜')]
    if space:
        pats.append((' ', '_'))
    if not full:
        pats = [(x[0], '_') for x in pats]
    return replace(s, pats=pats)


def sort_port(x, base=100):
    '''排序端口 1/0/1 1/0/10'''
    m = re.findall('\d+', x)
    if m:
        return sum(int(n) * (base ** i) for i, n in enumerate(m[::-1]))
    else:
        return 0


def format_output(data, column=None, show_title=True, fmt=None, default='-', sep=',', sort_by=None, sort_reverse=False):
    '''
    :param data:
    :param column:  展示的项，如果record没有col，使用func(item)或default
        ['col1', 'col2', ...]
        [('col1', 'title1), 'col2', ...]
        [('col1', 'title1, func), 'col2', ...]
        [{'key': 'col1', 'title': 'title1, 'func': func}, 'col2', ...]
    :param show_title:  显示标题，title见column
    :param fmt: 要展示的格式
    :param default:
    :param sep:
    :param sort_by: 以column某一列排序
    :param sort_reverse: 以column某一列排序
    :param item_show: 某一个col的值对应的字典，{'item1': {1:'one', '2':'two', '3': func}}
    :return:
    '''
    s = []
    _title = []
    _column = []
    if column:
        for x in column:
            if isinstance(x, dict):
                _column.append(x['key'])
                _title.append(x.get('title') or x['key'])
            elif isinstance(x, (tuple, list)):
                _column.append(x[0] if isinstance(x, (tuple, list)) else x)
                _title.append(x[1] if isinstance(x, (tuple, list)) else x)
    if show_title:
        s.append(sep.join(_title))
    _data = [data] if not isinstance(data, list) else data
    if sort_by:
        _data = sorted(_data, key=lambda v: v[sort_by], reverse=sort_reverse)
    for x in _data:
        if fmt:
            s.append(fmt.format(**x))
        elif _column:
            _s = []
            for i, col in enumerate(_column):
                if column:
                    if isinstance(column[i], (list, tuple)) and len(column[i]) == 3 and hasattr(column[i][2],
                                                                                                '__call__'):
                        _s.append(column[i][2](x))
                    elif isinstance(column[i], dict) and column[i].get('func') and hasattr(column[i]['func'],
                                                                                           '__call__'):
                        _s.append(column[i]['func'](x))
                if len(_s) == i:
                    if col in x:
                        _s.append(x[col])
                    else:
                        _s.append(default)
            s.append(sep.join(str(z) for z in _s))
        else:
            s.append(str(x))
    return '\n'.join(s)
