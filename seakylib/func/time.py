#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/14 11:56

import re
from datetime import datetime

# pip3 install python-dateutil
from dateutil.relativedelta import relativedelta

fmt1 = '%Y-%m-%d %H:%M:%S'
fmt2 = '%Y%m%d%H%M%S'
fmt3 = '%m%d'


# def datetime_now():
#     return datetime.now()

def last_peroid(per='days'):
    return datetime.now() - relativedelta(**{per: 1})


def last_month():
    return last_peroid(per='months')


def datetime_to_string(dt=None, fmt=None):
    if not fmt:
        fmt = fmt1
    if not dt:
        dt = datetime.now()
    return dt.strftime(fmt)


def _string_to_datetime(s, fmt):
    return datetime.strptime(s, fmt)


def string_to_datetime(s=None, fmt=None):
    '''
    :param s:   "2019-01-01 12:00:00" / 20190101125959 / 20190101
    :param fmt:
    :return:
    '''
    if fmt == 1:
        fmt = fmt1
    elif fmt == 2:
        fmt = fmt2
    if s is None:
        return datetime.now()
    elif isinstance(s, str):
        if fmt:
            return _string_to_datetime(s, fmt)
        elif re.search('^\d+:\d+:\d+$', s):  # 12:59:59
            return string_to_datetime('{} {}'.format(datetime_to_string().split(' ')[0], s))
        elif re.search('^\d+-\d+-\d+ \d+:\d+:\d+$', s):  # ［20］19-01-01 12:00:00
            if len(s.split('-')[0]) == 2:
                s = '20' + s
            return _string_to_datetime(s, fmt=fmt1)
        elif len(s) == 14:  # 20190101125959
            return _string_to_datetime(s, fmt=fmt2)
        elif len(s) == 12:  # 20190101125959
            return _string_to_datetime('20' + s, fmt=fmt2)
        elif len(s) == 8:  # 20190101
            return string_to_datetime(s + '000000', fmt=fmt2)
        elif len(s) == 6:  # 190101
            if int(s[2]) >= 2:  # 2020.01
                return string_to_datetime(s + '01000000', fmt=fmt2)
            else:  # 20.01.01
                return string_to_datetime('20' + s + '000000', fmt=fmt2)
        elif len(s) == 4:  # 1902
            return _string_to_datetime(s, fmt=fmt3)
        else:
            return int(s)
    else:
        return s


def datetime_to_timestamp(dt=None):
    if not dt:
        dt = datetime.now()
    return int(datetime.timestamp(dt))


def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(timestamp)


def timestamp_to_string(timestamp, fmt=None):
    if not fmt:
        fmt = fmt1
    return datetime_to_string(timestamp_to_datetime(timestamp), fmt=fmt)


def string_to_timestamp(s, fmt=None):
    return int(datetime_to_timestamp(string_to_datetime(s, fmt=fmt)))


def format_datetime_string(s, fmt_in=None, fmt_out=None):
    if not fmt_out:
        fmt_out = fmt1
    return datetime_to_string(string_to_datetime(s, fmt=fmt_in), fmt=fmt_out)
