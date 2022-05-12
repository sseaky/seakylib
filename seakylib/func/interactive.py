#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2021/4/2 16:46


def confirm(message='Are you sure? [y/n] ', choice_true='y', message_false=None, strict_no=False, can_blank=True):
    c = input(message).strip()
    if len(c) == 0 and not can_blank:
        return confirm(message=message, message_False=message_false, choice_true=choice_true, strict_no=strict_no,
                       can_blank=can_blank)
    if c.lower() == choice_true:
        return True
    else:
        if message_false:
            print(message_false)
        return False
