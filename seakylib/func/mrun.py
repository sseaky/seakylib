#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/4/20 14:44

import queue
import time
import traceback
from collections import OrderedDict
from multiprocessing import Process, Manager
from random import randint

from ..data.mysql import MyModel
from ..data.mysql import new_session
from ..func.base import MyClass, catch_exception
from ..func.parser import ArgParseClass
from ..os.info import get_caller
from ..os.oper import dump_data, load_data


class MultiRun(MyClass):
    def __init__(self, func, func_kws, process_num=5, func_common_kw=None, func_common_kw_with_obj=None,
                 process_db_session_enable=False, process_db_session_kw=None, show_process=False, show_job_result=False,
                 mark_start_time=False, add_log_to_common_kw=True, **kwargs):
        '''
        :param func: 处理函数，需要返回 status, result
        :param func_kws:    需要处理的参数
        :param process_num: 进程数，如果要连db，需要注意数据库connection的限制
        :param inline: 单进程运行
        :param func_common_kw: 函数通用参数, 不能放入log object
        :param func_common_kw_with_obj: 函数通用参数, 可放入object，如log，因为object不能被dump，所以单独处理
        :param process_db_session_enable: 每个process创建db_session传给job，而不是由job自己创建
        :param process_db_session_kw: db_session_kw创建参数 {'conn_str':''}
        :param show_process: 显示process调试
        :param show_job_result: 显示每个job的result
        :param mark_start_time: 给job传入start_time以统计时间
        :param add_log_to_common_kw:
        :return
        '''
        MyClass.__init__(self, **kwargs)
        self.func = func
        self.func_kws = func_kws
        self.process_num = process_num if isinstance(process_num, int) else process_num
        self.func_common_kw = {} if func_common_kw is None else func_common_kw
        self.func_common_kw_with_obj = {} if func_common_kw_with_obj is None else func_common_kw_with_obj
        self.process_db_session_enable = process_db_session_enable
        self.process_db_session_kw = process_db_session_kw
        self.show_process = show_process
        self.show_job_result = show_job_result
        self.mark_start_time = mark_start_time
        if add_log_to_common_kw and 'log' not in self.func_common_kw_with_obj:
            self.func_common_kw_with_obj['log'] = self.log

    def before_run(self):
        self.func_kws_now = self.func_kws

    def job(self, process_i, inline=False):
        self.show_process_debug('process {} start.'.format(process_i))
        if self.process_db_session_enable and self.process_db_session_kw:
            db_session = new_session(**self.process_db_session_kw)
            self.show_process_debug('process {} create data session {}.'.format(process_i, db_session))
        else:
            db_session = None
        i = 0
        while True:
            try:
                start_time = time.time()
                if not inline:
                    v = self.q_input.get_nowait()
                else:
                    v = self.q_input[i]
                    i += 1
                self.show_process_debug('process {} get {} job, kwarg: {}'.format(process_i, v['order_in'], v['kw']))
                if db_session:
                    self.func_common_kw_with_obj.update({'db_session': db_session})
                st = {'start_time': start_time} if self.mark_start_time else {}
                try:
                    is_ok, result = self.func(**v['kw'], **self.func_common_kw_with_obj, **st)
                except Exception as e:
                    self.log.info(traceback.format_exc())
                    is_ok, result = False, str(e)
                elapsed_time = round(time.time() - start_time, 2)
                v.update({'is_ok': is_ok, 'result': result, 'timer': elapsed_time})
                if self.show_job_result:
                    _log = self.log.info if is_ok else self.log.error
                    _log('{} | time: {}'.format(result, elapsed_time))
                self.show_process_debug('process {} do {} job done. result: {}'.format(process_i, v['order_in'],
                                                                                       {'is_ok': is_ok,
                                                                                        'result': result}))
                if not inline:
                    self.q_output.put(v)
                else:
                    self.q_output.append(v)

            except queue.Empty:
                break
            except IndexError:
                break
            except Exception as e:
                self.log.info(traceback.format_exc())
                break
        if db_session:
            db_session.close()
        self.show_process_debug('process {} end.'.format(process_i))

    @catch_exception()
    def run(self, load=False, save=True, retry_fail=0, func_retry_spec=None, func_change=None, *args, **kwargs):
        '''
        :param load:
        :param save:
        :param retry:   重复失败的结果的次数
        :param func_retry_spec:   如果func_retry_spec是函数，则要返回True才重试，如果为None重试所有失败
        :param func_change:   可在重试时调整某些参数
        :param args:
        :param kwargs:
        :return:
        '''
        assert isinstance(retry_fail, int), 'retry_fail must be int.'
        self.func_kws_now = self.func_kws
        result_path = self.path_temp / '{}_mrun_result.json'.format(get_caller().stem)
        if load and result_path.exists():
            is_ok, self.results = load_data(result_path)
            return is_ok, self.results
        is_ok, results = self._run(*args, **kwargs)
        assert is_ok, results

        # 重复运行
        i = 0
        _results = results
        presave_orders = [i for i, x in enumerate(results)]
        # idx_order = {x['idx']: i for i, x in enumerate(results)}
        while i < retry_fail:
            i += 1
            fails = []
            orders = []
            for j, x in enumerate(_results):
                ori_order = presave_orders[j]
                if x['is_ok']:
                    continue
                flag = False
                if not func_retry_spec:
                    flag = True
                elif hasattr(func_retry_spec, '__call__') and func_retry_spec(x):
                    flag = True
                if not flag:
                    continue
                kw = self.func_kws[ori_order]
                if func_change and hasattr(func_change, '__call__'):
                    kw = func_change(x)
                fails.append(kw)
                orders.append(ori_order)
            if not fails:
                break
            self.func_kws_now = fails
            self.log.info('retry {} failed tasks in {} times.'.format(len(fails), i))
            is_ok, _results = self._run(*args, **kwargs)
            assert is_ok, _results
            order_out_last = max([x.get('order_out', 0) for x in results])
            for j, x in enumerate(_results):
                ori_order = presave_orders[j]
                d = {'retry': i, 'pre_kw': self.func_kws[ori_order]}
                d.update({k: v for k, v in x.items() if k in ['is_ok', 'result', 'timer', 'kw']})
                results[ori_order].update(d)
                if 'order_out' not in results[ori_order]:
                    results[ori_order]['order_out'] = x['order_out'] + order_out_last
            presave_orders = orders

        if save:
            dump_data(results, result_path)
        self.results = results
        return is_ok, results

    def _run(self, inline=False, process_timeout=None):
        '''
        :param process_timeout: 进程超时
        :param load: 读取结果
        :return:
            {
            'is_ok': False,
            'miss': True,
            'result': 'result is not exist.',
            'order_in': n,
            'order_out': n,
            'kw': {}
            }
        '''
        self.inline = inline
        if not inline:
            m = Manager()
            self.q_input = m.Queue()
            self.q_output = m.Queue()
        else:
            self.q_input = []
            self.q_output = []
        assert isinstance(self.func_kws_now, list), 'func_kws is not list.'
        idxes = OrderedDict()
        for i, kw in enumerate(self.func_kws_now, 1):
            idx = id(kw)
            kw.update(self.func_common_kw)
            d = {'order_in': i, 'idx': idx, 'kw': kw}
            idxes[idx] = d
            if not inline:
                self.q_input.put(d)
            else:
                self.q_input.append(d)

        if inline:
            self.job(1, inline=True)
        else:
            process_num = min(self.process_num, len(self.func_kws_now))
            self.show_debug('start {} process.'.format(process_num))
            ps = []
            for process_i in range(1, process_num + 1):
                process = Process(target=self.job, args=([process_i, False]))
                ps.append(process)
                process.start()
            for i, p in enumerate(ps):
                p.join(timeout=process_timeout)
            if process_timeout:
                for i, p in enumerate(ps, 1):
                    if p.is_alive():
                        self.show_process_debug('terminate process {}.'.format(i))
                        p.terminate()

        outputs = {}
        if not inline:
            for i in range(1, self.q_output.qsize() + 1):
                d = self.q_output.get_nowait()
                d['order_out'] = i
                outputs[d['idx']] = d
        else:
            for i, d in enumerate(self.q_output):
                d['order_out'] = i + 1
                outputs[d['idx']] = d
        results = [outputs.get(idx, {'is_ok': False, 'miss': True, 'result': 'result is not exist.',
                                     'order_in': d['order_in'],
                                     'kw': d['kw']}) for idx, d in idxes.items()]
        return True, results

    def show_process_debug(self, *obj):
        self.show_by_flag(self.show_process, *obj)

    def update_results(self, model, key, datas=None, sql=None, last_cols=None, timed=True, ret_str=True):
        '''
        :param model: 结果表
        :param datas: 运行结果, 默认取self.results
        :param key: 主键
        :param sql: 如果无sql，则返回表格数据
        :param last_cols:   [col1, col2], 更新时需要保留的上一次状态。model中需要有 col1_last, col2_last字段
        :param timed:   记录时间
        :param ret_str:
        :return:
        '''
        if model.__class__ == MyModel:
            mm = model
        else:
            mm = MyModel(model, db_session=self.db_session)
        if not datas:
            datas = self.results
        if not last_cols:
            last_cols = [col.replace('_last', '') for col in mm.cols_name if col.endswith('_last')]
        if sql:
            is_ok, data_old = mm.query(sql=sql, key=key)
        else:
            is_ok, data_old = mm.query(key=key)
        for i, v in enumerate(datas):
            k = key(v) if hasattr(key, '__call__') else v[key]
            if k in data_old:
                _keys = list(v.keys())
                for col in _keys:
                    if col not in mm.cols_name:
                        continue
                    if col in last_cols:
                        v['{}_last'.format(col)] = data_old[k][col]
            count_fail_col = 'failed'
            if 'is_ok' in v and count_fail_col in mm.cols_name:
                if k in data_old:
                    _count = data_old[k][count_fail_col]
                    if not _count:
                        _count = 0
                else:
                    _count = 0
                if not v['is_ok']:
                    v[count_fail_col] = _count + 1
                else:
                    v[count_fail_col] = _count
            if 'is_ok' in v:
                v['is_ok'] = 1 if v['is_ok'] else 0

        is_ok, result = mm.update(data_new=datas, data_old=data_old, key=key, timed=timed, ret_str=ret_str)
        return is_ok, result


class MrunArgParse(ArgParseClass):
    def __init__(self, process_num=60, process_timeout=60, *args, **kwargs):
        ArgParseClass.__init__(self, *args, **kwargs)
        self.process_num = process_num
        self.process_timeout = process_timeout

    def add_multi(self, group='Multi Process'):
        self.add('--process_num', type=int, default=self.process_num, help='进程数量，{}'.format(self.process_num),
                 group=group)
        self.add('--process_timeout', type=int, default=self.process_timeout,
                 help='进程超时时间, default {}s'.format(self.process_timeout), group=group)
        self.add('--inline', action='store_true', default=False, help='串行模式', group=group)
        self.add('--show_process', action='store_true', default=False, help='显示进程操作过程', group=group)
        self.add('--retry_fail', type=int, default=0, help='重试失败次数，默认0', group=group)

    # def add_all(self):
    #     self.add_base(self)
    #     self.add_multi()


if __name__ == '__main__':
    def test(i):
        # must return is_ok, message
        t = randint(0, 3)
        time.sleep(t)
        msg = 'i am {}, waiting {}.'.format(i, t)
        print(msg)
        return True, msg


    mr = MultiRun(func=test, func_kws=[{'i': x} for x in range(5)], process_num=3,
                  add_log_to_common_kw=True)
    print(mr.run(inline=False))
