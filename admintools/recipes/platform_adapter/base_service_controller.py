#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME, SKV_META_SERVER_ROLE_NAME


class BaseServiceController:
    def __init__(self, logger, module):
        self.logger = logger
        self.module = module

    """
    以下是外部调用接口
    """
    def restart_replica_server(self, fqdn):
        self._instance_restart(SKV_REPLICA_SERVER_ROLE_NAME, fqdn)

    def stop_replica_server(self, fqdn):
        self._instance_stop(SKV_REPLICA_SERVER_ROLE_NAME, fqdn)

    def start_replica_server(self, fqdn):
        self._instance_start(SKV_REPLICA_SERVER_ROLE_NAME, fqdn)

    def stop_all_replica_server(self):
        self._role_stop(SKV_REPLICA_SERVER_ROLE_NAME)

    def start_all_replica_server(self):
        self._role_start(SKV_REPLICA_SERVER_ROLE_NAME)

    def restart_meta_server(self, fqdn):
        self._instance_restart(SKV_META_SERVER_ROLE_NAME, fqdn)

    def start_meta_server(self, fqdn):
        self._instance_start(SKV_META_SERVER_ROLE_NAME, fqdn)

    def stop_meta_server(self, fqdn):
        self._instance_stop(SKV_META_SERVER_ROLE_NAME, fqdn)

    def stop_all_meta_server(self):
        self._role_stop(SKV_META_SERVER_ROLE_NAME)

    def start_all_meta_server(self):
        self._role_start(SKV_META_SERVER_ROLE_NAME)

    def start_skv(self):
        self._module_start()

    def stop_skv(self):
        self._module_stop()

    def skv_is_alive(self):
        """返回true/false 是否活着"""
        return self._module_is_alive()

    """以下几个接口是平台相关的 需要各个平台实现(sp 2.0/sp 2.1)"""
    def _module_start(self):
        """启动整个模块"""
        raise Exception('please implement this method')

    def _module_stop(self):
        """停止整个模块"""
        raise Exception('please implement this method')

    def _module_is_alive(self):
        """查看单个模块是否活着"""
        raise Exception('please implement this method')

    def _role_stop(self, role):
        """停止单个角色所有实例"""
        raise Exception('please implement this method')

    def _role_start(self, role):
        """启动单个角色所有实例"""
        raise Exception('please implement this method')

    def _instance_stop(self, role, fqdn):
        """停止单个实例"""
        raise Exception('please implement this method!')

    def _instance_start(self, role, fqdn):
        """启动单个实例"""
        raise Exception('please implement this method!')

    def _instance_restart(self, role, fqdn):
        """重启单个实例"""
        raise Exception('please implement this method!')
