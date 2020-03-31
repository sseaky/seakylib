#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/11/25 11:48


def fence(s, offset=None):
    '''
    栅栏密码，分割成相同长度的字串，然后按位置连接
    fence(s='Teieeemrynwetemryhyeoetewshwsnvraradhnhyartebcmohrie')
    '''
    if isinstance(offset, int):
        factors = [offset]
    else:
        factors = [i for i in range(2, len(s)) if len(s) % i == 0]
    return {factor: ''.join(s[i::factor] for i in range(factor)) for factor in factors}
