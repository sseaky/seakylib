#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/6/18 14:01

import json
from socketserver import StreamRequestHandler, ForkingTCPServer

from ..func.base import MyClass

ForkingTCPServer.allow_reuse_address = True


class MyRequestHandle(StreamRequestHandler):
    # log = make_logger('Server', write=True)
    log = None
    encoding = 'utf-8'

    def handle(self):
        self.log = self.server_instance.log
        self.log.info('Got connection from {}'.format(self.client_address))
        for line in self.rfile:
            line = line.decode(self.encoding)
            self.log.info('receive from {} {}'.format(self.client_address, repr(line)))
            self.job(line)

    def job(self, recv):
        self.reply('receive ' + recv)

    def reply(self, message):
        if isinstance(message, (list, dict)):
            message = json.dumps(message).encode(self.encoding)
        if isinstance(message, bytes):
            message = message.decode(self.encoding)
        self.wfile.write(message.encode(self.encoding))
        self.log.info('send to {} {}'.format(self.client_address, repr(message)))


class MyTCPServer(MyClass):
    def __init__(self, port, handle, *args, **kwargs):
        MyClass.__init__(self, *args, **kwargs)
        self.port = port
        self.handle = handle
        self.handle.server_instance = self

    def run(self):
        serv = ForkingTCPServer(('', self.port), self.handle)
        self.log.info('start server on port {}'.format(self.port))
        serv.serve_forever()


if __name__ == '__main__':
    ms = MyTCPServer(port=20000, handle=MyRequestHandle)
    ms.run()
