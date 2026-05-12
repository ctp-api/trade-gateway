#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: ctp
@FileName   : util.py
@Date       : 2025/9/4 17:57
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 公共函数
"""

def prepare_address(address: str) -> str:
    """
    如果没有协议，则帮助程序会在前面添加 tcp:// 作为前缀。

    If there is no protocol, the helper prefixes it with tcp:// .
    :param address: 行情服务器地址 Market server address
    :return: 返回带协议的服务器地址 Returns the server address with protocol
    """
    if not any(address.startswith(scheme) for scheme in ["tcp://", "ssl://", "socks://"]):
        return "tcp://" + address
    return address