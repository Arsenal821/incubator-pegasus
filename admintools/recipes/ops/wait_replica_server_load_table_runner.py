#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

重启的时候等待replica server加载表
"""
import traceback

from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def wait_replica_server_load_table(module_name, logger, replica_server, replica_count, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None, ):
    """对外接口 等待replica server列表里面的每个replica server状态符合预期"""
    runner = WaitReplicaServerLoadTableRunner(module_name, logger, replica_server, replica_count, print_progress_fun, check_interval_seconds, check_timeout_seconds)
    runner.run()


class WaitReplicaServerLoadTableRunner(AsyncWaitRunner):
    def __init__(self, module_name, logger, replica_server, replica_count, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
        self.module_name = module_name
        self.logger = logger
        self.print_fun = print_progress_fun if print_progress_fun else logger.info
        self.api = SkvAdminApi(logger, module_name)
        self.replica_server = replica_server
        self.replica_count = replica_count if replica_count else 0

        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """纯等待 什么都不做"""
        table_count = self.execute_check()
        self.print_progress(table_count)

    def execute_check(self):
        """执行一次检查 返回加载replica个数"""
        try:
            return self.api.get_serving_replica_count(self.replica_server, 10000)
        except Exception:
            self.logger.debug('failed to get replica count, return 0')
            self.logger.debug(traceback.format_exc())
            return 0

    def check_wait_done(self, table_count):
        """检查结果 是否可以结束等待
        之前判断了是否所有表都加载了，有不一致的情况
        现改为是否加载到表
        返回True/False"""
        return int(table_count) != 0 or self.replica_count == 0

    def print_progress(self, table_count):
        """打印进度"""
        self.print_fun("%s loading %s/%s" % (self.replica_server, table_count, self.replica_count))
