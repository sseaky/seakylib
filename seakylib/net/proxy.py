#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: Seaky
# @Date:   2019/10/10 10:31

import socket

# pip install SocksiPy-branch
import socks
from sshtunnel import SSHTunnelForwarder


def set_socks_proxy(addr=None, port=None, rdns=True, username=None, password=None,
                    proxytype=socks.PROXY_TYPE_SOCKS5):
    '''
    使用已有socks5
    proxytype -    The type of the proxy to be used. Three types
            are supported: PROXY_TYPE_SOCKS4 (including socks4a),
            PROXY_TYPE_SOCKS5 and PROXY_TYPE_HTTP
    addr -        The address of the server (IP or DNS).
    port -        The port of the server. Defaults to 1080 for SOCKS
            servers and 8080 for HTTP proxy servers.
    rdns -        Should DNS queries be preformed on the remote side
            (rather than the local side). The default is True.
            Note: This has no effect with SOCKS4 servers.
    username -    Username to authenticate with to the server.
            The default is no authentication.
    password -    Password to authenticate with to the server.
            Only relevant when username is also provided.
    '''
    defaultproxy = {'proxytype': proxytype, 'addr': addr, 'port': port, 'rdns': rdns,
                    'username': username, 'password': password}
    socks.setdefaultproxy(**defaultproxy)
    socket.socket = socks.socksocket


def unset_socks_proxy():
    socks.setdefaultproxy(None)
    socket.socket = socks.socksocket


class Tunnel:
    def __init__(self, ssh_address_or_host, ssh_username, ssh_password=None, ssh_pkey=None,
                 ssh_private_key_password=None, **kwargs):
        '''
        建立ssh隧道
        :param ssh_address_or_host: ('x.x.x.x', 22)
        :param ssh_username:
        :param ssh_password:
        :param ssh_pkey:
        :param ssh_private_key_password:
        :param kwargs:
        '''
        self.kwargs = kwargs
        self.kwargs.update({'ssh_address_or_host': ssh_address_or_host,
                            'ssh_username': ssh_username, 'ssh_password': ssh_password,
                            'ssh_pkey': ssh_pkey, 'ssh_private_key_password': ssh_private_key_password})

    def setup(self, remote_bind_address, local_bind_address=None):
        '''
        :param remote_bind_address: (x.x.x.x, port)
        :param local_bind_address: 随机分配, server.local_bind_port
        :return:
        '''
        server = SSHTunnelForwarder(
            remote_bind_address=remote_bind_address,
            local_bind_address=local_bind_address,
            **self.kwargs
        )
        self.server = server
        return server
