#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys

from hyperion_client.module_service import ModuleService
from skv_common import SKV_PRODUCT_NAME

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_service_controller import BaseServiceController


class CaptainServiceController(BaseServiceController):
    def __init__(self, logger, module):
        self.module_service = ModuleService()
        self.product = SKV_PRODUCT_NAME
        self.module = module
        self.logger = logger

    def _module_start(self):
        """启动整个模块"""
        self.module_service.start(self.product, self.module, logger=self.logger)

    def _module_stop(self):
        """停止整个模块"""
        self.module_service.stop(self.product, self.module, logger=self.logger)

    def _module_is_alive(self):
        """查看单个模块是否活着"""
        return 'ALIVE' == self.module_service.status(self.product, self.module, logger=self.logger)

    def _role_stop(self, role):
        """停止单个角色所有实例"""
        self.module_service.stop(self.product, self.module, role, logger=self.logger)

    def _role_start(self, role):
        """启动单个角色所有实例"""
        self.module_service.start(self.product, self.module, role, logger=self.logger)

    def _instance_restart(self, role, fqdn):
        """重启单个实例"""
        self.module_service.restart(self.product, self.module, role, fqdn, logger=self.logger)

    def _instance_start(self, role, fqdn):
        """启动单个实例"""
        self.module_service.start(self.product, self.module, role, fqdn, logger=self.logger)

    def _instance_stop(self, role, fqdn):
        """停止单个实例"""
        self.module_service.stop(self.product, self.module, role, fqdn, logger=self.logger)
