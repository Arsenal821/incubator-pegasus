#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

kill partition and wait
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def kill_partition(module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=10, retry_times=3):
    """对外接口"""
    runner = KillPartitionRunner(module_name, logger, replica_addr, print_progress_fun, check_interval_seconds, check_timeout_seconds, retry_times)
    runner.run()


class KillPartitionRunner(AsyncWaitRunner):
    """发送kill partition命令关闭所有replica 以触发flush操作"""
    def __init__(self, module_name, logger, replica_addr, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=10, retry_times=3):
        self.print_fun = print_progress_fun if print_progress_fun is not None else logger.info
        self.logger = logger
        self.api = SkvAdminApi(logger, module_name)
        self.replica_addr = replica_addr

        super().__init__(check_interval_seconds, check_timeout_seconds, retry_times)

    def async_do(self):
        """异步操作"""
        serving_replica_count, opening_replica_count, closing_replica_count = self.execute_check()
        left_count = serving_replica_count + opening_replica_count + closing_replica_count
        self.print_fun('start to kill %d partitions on %s' % (left_count, self.replica_addr))
        self.api.inactive_all_replica_on_host(self.replica_addr)

    def execute_check(self):
        """执行一次检查"""
        serving_replica_count = self.api.get_serving_replica_count(self.replica_addr)
        opening_replica_count = self.api.get_opening_replica_count(self.replica_addr)
        closing_replica_count = self.api.get_closing_replica_count(self.replica_addr)
        self.logger.debug('execute result: %d,%d,%d' % (serving_replica_count, opening_replica_count, closing_replica_count))
        return serving_replica_count, opening_replica_count, closing_replica_count

    def check_wait_done(self, execute_result):
        """检查结果 是否可以结束等待 execute_result 是个三元组"""
        serving_replica_count, opening_replica_count, closing_replica_count = execute_result
        return serving_replica_count + opening_replica_count + closing_replica_count == 0

    def print_progress(self, execute_result):
        """打印进度 execute_result是个三元组"""
        serving_replica_count, opening_replica_count, closing_replica_count = execute_result
        left_count = serving_replica_count + opening_replica_count + closing_replica_count
        self.print_fun('still %d replica; %d serving, %d opening, %d closing' % (
            left_count, serving_replica_count, opening_replica_count, closing_replica_count))

    def on_timeout(self):
        # kill partition即使timeout也不报错
        # kill_partition是希望在升级前，关闭这台机器上服务的所有partition，
        # 判断中的28可以认为是超时的设置，如果很长时间仍有partition没有close，也不再尝试了。
        # 你说的只执行一次kill_partition，后面replica个数会重新再涨回来，这个现象是正常的，
        # 升级的时候为了不让partition迁回来，会把add_secondary_max_count_for_one_node参数设置成0，也能在脚本中找到
        self.print_fun('kill partition timeout. ignore this.')
        return
