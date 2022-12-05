#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

等待replica server变成alive
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def wait_replica_server(module_name, logger, replica_server_list=None, expect_status=None, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None, ):
    """对外接口 等待replica server列表里面的每个replica server状态符合预期"""
    runner = WaitReplicaServerRunner(module_name, logger, replica_server_list, expect_status, print_progress_fun, check_interval_seconds, check_timeout_seconds)
    runner.run()


class WaitReplicaServerRunner(AsyncWaitRunner):
    def __init__(self, module_name, logger, replica_server_list=None, expect_status=None, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
        self.module_name = module_name
        self.logger = logger
        self.print_fun = print_progress_fun if print_progress_fun else logger.info
        self.api = SkvAdminApi(logger, module_name)
        self.replica_server_list = replica_server_list if replica_server_list else self.api.get_all_replica_server()
        self.expect_status = expect_status if expect_status else 'ALIVE'

        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """纯等待 什么都不做"""
        replica_server_to_status = self.execute_check()
        self.print_progress(replica_server_to_status)

    def execute_check(self):
        """执行一次检查 返回不符合预期的节点数+错误信息"""
        unexpect_status_dict = {}
        total_count = 0
        for addr in self.replica_server_list:
            status = self.api.get_replica_server_status(addr)
            if status != self.expect_status:
                if status not in unexpect_status_dict:
                    unexpect_status_dict[status] = 0
                unexpect_status_dict[status] += 1
                total_count += 1
        unexpect_msg = ','.join(['%s: %d' % (status, cnt) for status, cnt in unexpect_status_dict.items()])
        return total_count, unexpect_msg

    def check_wait_done(self, result):
        """检查结果 是否可以结束等待 是否所有replica server的状态符合预期
        replica_server_to_status是execute_check()的结果 是一个dict
        返回True/False"""
        total_count, _ = result
        if total_count == 0:
            self.print_fun('%d replica server all %s' % (len(self.replica_server_list), self.expect_status))
            return True
        else:
            return False

    def print_progress(self, result):
        """打印进度
        replica_server_to_status是execute_check()的结果 是一个dict"""
        total_count, unexpect_msg = result
        self.print_fun('waiting %d replica server %s' % (total_count, unexpect_msg))
