#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

将所有副本标记inactive 并等待
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def inactive_replica(module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=10, retry_times=3):
    """对外接口"""
    runner = InactiveReplicaRunner(module_name, logger, replica_addr, print_progress_fun, check_interval_seconds, check_timeout_seconds, retry_times)
    runner.run()


class InactiveReplicaRunner(AsyncWaitRunner):
    """通过工具批量将某个replica server上的primary全部变成inactive"""
    def __init__(self, module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=10, retry_times=3):
        self.print_fun = print_progress_fun if print_progress_fun is not None else logger.info
        self.api = SkvAdminApi(logger, module_name)
        self.replica_addr = replica_addr

        super().__init__(check_interval_seconds, check_timeout_seconds, retry_times)

    def async_do(self):
        """异步操作"""
        replica_count = self.execute_check()
        self.print_fun('start inactive all %d replica on %s' % (replica_count, self.replica_addr))
        self.api.inactive_all_replica_on_host(self.replica_addr)

    def execute_check(self):
        """执行一次检查"""
        return self.api.get_active_replica_count_on_host(self.replica_addr)

    def check_wait_done(self, replica_count):
        """检查结果 是否可以结束等待 replica_count是execute_check()的结果 返回True/False"""
        return replica_count == 0

    def print_progress(self, replica_count):
        """打印进度 replica_count是execute_check()的结果"""
        self.print_fun('%d active replica on %s' % (replica_count, self.replica_addr))

    def on_timeout(self):
        # inactive partition即使timeout也不报错 因为 事实证明 实在很玄学。。。
        self.print_fun('inactive partition timeout. ignore this.')
        return
