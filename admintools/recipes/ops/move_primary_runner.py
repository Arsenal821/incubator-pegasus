#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

这个操作会将某个replica server上的primary全部降级
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def move_primary(module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
    """对外接口"""
    runner = MovePrimaryRunner(module_name, logger, replica_addr, print_progress_fun, check_interval_seconds, check_timeout_seconds)
    runner.run()


class MovePrimaryRunner(AsyncWaitRunner):
    """通过工具批量将某个replica server上的primary全部降级"""
    def __init__(self, module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
        self.print_fun = print_progress_fun if print_progress_fun is not None else logger.info
        self.api = SkvAdminApi(logger, module_name)
        self.replica_addr = replica_addr

        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """异步操作"""
        primary_count = self.execute_check()
        self.print_fun('start move %d primary replica on %s' % (primary_count, self.replica_addr))
        self.api.move_all_primary_on_host(self.replica_addr)

    def execute_check(self):
        """执行一次检查"""
        return self.api.get_primary_count_on_server(self.replica_addr)

    def check_wait_done(self, primary_count):
        """检查结果 是否可以结束等待 primary_count是execute_check()的结果 返回True/False"""
        return primary_count == 0

    def print_progress(self, primary_count):
        """打印进度 primary_count是execute_check()的结果"""
        self.print_fun('%d primary replica on %s' % (primary_count, self.replica_addr))
