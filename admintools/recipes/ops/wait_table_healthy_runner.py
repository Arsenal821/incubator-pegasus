#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

等待table healthy
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def wait_table_healthy(module_name, logger, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
    """对外接口 等待所有表都healthy"""
    runner = WaitTableHealthyRunner(module_name, logger, print_progress_fun, check_interval_seconds, check_timeout_seconds)
    runner.run()


class WaitTableHealthyRunner(AsyncWaitRunner):
    def __init__(self, module_name, logger, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
        self.module_name = module_name
        self.logger = logger
        self.print_fun = print_progress_fun if print_progress_fun else logger.info
        self.api = SkvAdminApi(logger, module_name)
        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """纯等待 什么都不做"""
        result = self.execute_check()
        self.print_progress(result)

    def execute_check(self):
        """执行一次检查 返回不healthy的app个数 三元组"""
        unhealthy_table_count = self.api.get_unhealthy_app_count()
        read_unhealthy_table_count = self.api.get_read_unhealthy_app_count()
        write_unhealthy_table_count = self.api.get_write_unhealthy_app_count()
        return unhealthy_table_count, read_unhealthy_table_count, write_unhealthy_table_count

    def check_wait_done(self, execute_result):
        """检查结果 是否可以结束等待 是否所有replica server的状态符合预期
        execute_result是个三元组"""
        unhealthy_table_count, _, _ = execute_result
        if unhealthy_table_count == 0:
            self.print_fun('all table healthy!')
            return True
        return False

    def print_progress(self, execute_result):
        """打印进度
        execute_result是个三元组"""
        unhealthy_table_count, read_unhealthy_table_count, write_unhealthy_table_count = execute_result
        self.print_fun('still %d unhealthy: %d read unhealthy %d write unhealthy' % (
            unhealthy_table_count, read_unhealthy_table_count, write_unhealthy_table_count))
