#!/bin/env python
# -*- coding: UTF-8 -*-

from skv_maintenance_workers.base_worker import BaseWorker


class SkvFillServerConfigWorker(BaseWorker):
    """暂无需要补充的配置 保留路径 便于后续补充"""
    def is_state_abnormal(self):
        abnormal = False
        return abnormal

    def diagnose(self):
        self.fill_server_conf()

    def self_remedy(self):
        return True

    def repair(self):
        self.fill_server_conf()

    def fill_server_conf(self):
        pass
