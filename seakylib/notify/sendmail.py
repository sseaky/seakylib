#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/8/6 14:49

import hashlib
import smtplib
from copy import deepcopy
from email import encoders
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from ..func.base import MyClass, run_func, func_done
from ..func.string import str_is_number, comma_digit


class Mail(MyClass):
    '''
    使用本地服务器时，设置 /etc/hosts为FQDN，否则远端会拒绝连接。
    127.0.0.1   xxxxx.cn xxxxx
    很多服务器也会拒绝虚拟的FQDN
    运行 hostname -f
    sudo dpkg-reconfigure exim4-config
    '''

    def __init__(self, server='localhost', port=25, username=None, password=None, conn_mode='plain', **kwargs):
        MyClass.__init__(self, **kwargs)
        self.smtp = None
        self.server, self.port, self.username, self.password, self.conn_mode = server, port, username, password, conn_mode
        self.login()

    def login(self):
        try:
            if self.server == 'localhost':
                smtp = smtplib.SMTP()
                smtp.connect(self.server, self.port)
            else:
                if self.conn_mode == 'tls':
                    # tls加密方式，通信过程加密，邮件数据安全，使用正常的smtp端口
                    smtp = smtplib.SMTP(self.server, self.port)
                    smtp.set_debuglevel(True)
                    smtp.ehlo()
                    smtp.starttls()
                    smtp.ehlo()
                    smtp.login(self.username, self.password)
                elif self.conn_mode == 'ssl':
                    # 纯粹的ssl加密方式，通信过程加密，邮件数据安全, smtps 465
                    smtp = smtplib.SMTP_SSL(self.server, self.port)
                    smtp.ehlo()
                    smtp.login(self.username, self.password)
                else:
                    smtp = smtplib.SMTP()
                    smtp.connect(self.server, self.port)
                    smtp.login(self.username, self.password)
            self.smtp = smtp
            return True
        except Exception as e:
            self.log_error(e)

    def check_path(self, fn):
        if Path(fn).exists():
            return True
        else:
            self.log_error('{} is not exist.'.format(fn))

    def send(self, from_addr=None, to_addrs=None, **kwargs):
        '''公共server会验证from_addr'''
        from_addr = str(Header(from_addr or self.from_addr, 'utf-8'))
        to_addrs = to_addrs or self.to_addrs
        if self.cc_addrs:
            to_addrs = to_addrs + self.cc_addrs
        if self.bcc_addrs:
            to_addrs = to_addrs + self.bcc_addrs
        if not self.cache.get('error'):
            try:
                r = self.smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=self.msg.as_string(), **kwargs)
                return True
            except Exception as e:
                pass
        else:
            print('error occur.')

    def create_message(self, from_addr, to_addrs, subject, cc_addrs=None, bcc_addrs=None, charset='utf-8'):
        '''to_addrs, from_addr for display in body'''
        if isinstance(to_addrs, str):
            to_addrs = [to_addrs]
        if isinstance(cc_addrs, str):
            cc_addrs = [cc_addrs]
        if isinstance(bcc_addrs, str):
            bcc_addrs = [bcc_addrs]
        self.from_addr = from_addr
        self.to_addrs = to_addrs
        self.cc_addrs = cc_addrs
        self.bcc_addrs = bcc_addrs
        msg = MIMEMultipart()
        # msg['From'] = Header(from_addr, charset=charset)
        msg['From'] = from_addr
        msg['To'] = ', '.join(to_addrs)
        msg['Subject'] = Header(subject, charset=charset)
        if cc_addrs:
            msg['Cc'] = ', '.join(cc_addrs)
        self.msg = msg

    def add_text(self, s, _subtype='html', _charset='utf-8'):
        self.msg.attach(MIMEText(s, _subtype, _charset))

    def add_image(self, fn, cid):
        '''如果要在正文中显示图片，需要在在正文中的html中引用 <img src="cid:XXXXX">, 然后为图片添加 Content-ID: <XXXXX>, 有<>。
        filename与content-id没有关系，任意，不写也行
<html lang="utf-8">
    <body>
        <div>
            <div><table border="1"><tbody>
                <tr><td><b>row1</b></td><td>{}</td></tr>
                <tr><td><b>row2</b></td><td>{}</td></tr>
            </tbody></table></div>
            <div><a href="{graph_url}"><img src="cid:{}"></a></div>
            <br>
        </div><br>
        <div>Generated by: x.x.x.x:~/project/cacti/Report.py</div>
    </body>
</html>
        '''
        if self.check_path(fn):
            mime = MIMEBase('image', 'png', filename=Path(fn).name)
            mime.add_header('Content-Disposition', 'inline', filename=Path(fn).name)
            mime.add_header('Content-ID', '<{0}>'.format(cid))
            mime.add_header('X-Attachment-Id', '{0}'.format(cid))
            mime.set_payload(open(str(fn), 'rb').read())
            encoders.encode_base64(mime)
            self.msg.attach(mime)
            return True

    def add_attach(self, fn=None, content=None, disp_name=''):
        if fn:
            if self.check_path(fn):
                att = MIMEApplication(open(fn, 'rb').read())
                att.add_header('Content-Disposition', 'attachment', filename=disp_name or Path(fn).name)

        elif content:
            att = MIMEApplication(content)
            att.add_header('Content-Disposition', 'attachment',
                           filename=disp_name or '{}.att'.format(hashlib.md5(content).hexdigit()))
        else:
            self.log_error('no attach found.')
            return
        self.msg.attach(att)
        return True

    def quit(self):
        if self.smtp:
            self.smtp.close()


def html_row_alternated_bg(row, i=0):
    row['tr_css'].update({'background-color': ['#FFFFFF', '#F0F0F0'][i % 2]})


def html_row_focus(row, col=None, thres=None, color='#FF0000'):
    if col not in row:
        return False, '{} not in row date'.format(col)
    if str_is_number(row[col]):
        # 强制转float
        value = float(row[col])
        if isinstance(thres, (int, float)):
            if value > thres:
                row['tr_css'].update({'color': color})
        else:
            if value > row[col] > thres[0]:
                row['tr_css'].update({'color': color})
    return True, func_done()


def html_row_to_tr(row, keys=None, css=None, comma=False):
    if not css:
        css = {}
    if isinstance(row, list):
        s_css = (' style="{}"'.format(';'.join(['{}: {}'.format(k, v) for k, v in css.items()])))
        s = '<tr{}>{}</tr>'.format(s_css, ''.join(['<td>{}</td>'.format(x) for x in row]))
    elif isinstance(row, dict):
        s_attr = (' ' + ';'.join(['{}="{}"'.format(k, v) for k, v in row['tr_attr'].items()]))
        css.update(row.get('tr_css') or {})
        s_css = (' style="{}"'.format(';'.join(['{}: {}'.format(k, v) for k, v in css.items()])))
        s = '<tr{}{}>{}</tr>'.format(s_attr, s_css, ''.join(
            ['<td>{}</td>'.format(comma_digit(row[k]) if comma and 'id' not in k else row[k]) for k in keys]
        ))
    else:
        s = ''
    return s


def html_df_to_table(df, titles=None, common_css=None, makeup=None, table_style='', comma=False):
    '''
    在df设置tr_attr, tr_css可以设置表格tr的属性
    :param df:    dict or dataframe
    :param titles:  [col1, col2, ...] or [(col1, disp1), (col2, disp2), ...], 可裁剪df
    :param common_css:  dict
    :param table_style:   "margin: auto;"
    :param makeup:    关注某字段
    :param comma:    逗号分割数字
    :return:
    '''

    html = list()
    html.append('''<div style="font-family:微软雅黑,Verdana,&quot;Microsoft Yahei&quot;,SimSun,sans-serif;font-size:14px; line-height:1.6;">
<table border=1 style="border: 1px groove black; table-layout: fixed;{}"><tbody>'''.format(table_style))
    if not titles:
        titles = [(x, x) for x in df.columns.to_list()]
    elif len(titles[0]) == 1:
        titles = [(x, x) for x in titles]

    if not common_css:
        common_css = {'text-align': 'center'}

    css_header = deepcopy(common_css)
    css_header.update({'background-color': '#FFFFC0', 'font-weight': 'bold'})
    html.append(html_row_to_tr(row=[x[1] for x in titles], css=css_header))

    if not makeup:
        makeup = []
    elif not isinstance(makeup, list):
        makeup = [makeup]

    for i, row in enumerate(df.to_dict('records')):
        if not row.get('tr_attr'):
            row['tr_attr'] = {}
        if not row.get('tr_css'):
            row['tr_css'] = {}
        row['tr_css'].update(common_css)
        html_row_alternated_bg(row, i)
        for func in makeup:
            if isinstance(func, tuple) and isinstance(func[1], dict):
                func[1]['row'] = row
            is_ok, result = run_func(func)
        html.append(html_row_to_tr(row, keys=[x[0] for x in titles], comma=comma))

    html.append('</tbody></table></div>')
    ret = '\n'.join(html)
    return ret


if __name__ == '__main__':
    pass