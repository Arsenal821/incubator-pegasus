#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief


skv client config 检测任务
"""

from skv_maintenance_workers.base_worker import BaseWorker


class SkvFillClientConfigWorker(BaseWorker):
    """暂无需要补充的配置 保留路径 便于后续补充"""
    def is_state_abnormal(self):
        abnormal = False
        return abnormal

    def diagnose(self):
        self.fill_client_conf()

    def repair(self):
        self.fill_client_conf()

    def self_remedy(self):
        return True

    def fill_client_conf(self):
        pass
