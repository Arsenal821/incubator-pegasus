#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

负载均衡操作
"""
from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi
from skv_common import is_hubble_installed


class BalanceType:
    """balancer表示把各节点个数调匀的过程。在目前的pegasus实现中，balancer过程大概可以用四点来概括：

1. 尽量通过角色互换来做到primary均衡
2. 如果1做不到让primary变均匀，通过拷数据来做到primary均衡
3. 在2做完后，通过拷数据做到secondary的均衡
4. 分别针对每个表做1-2-3的动作

    Pegasus提供了一些控制参数给些过程可以提供更精细的控制：
    """
    ONLY_PRIMARY_BALANCER = 'meta.lb.only_primary_balancer'  # 对于每个表，只进行1和2(减少copy secondary带来的数据拷贝)
    ONLY_MOVE_PRIMARY = 'meta.lb.only_move_primary'  # 对于每个表，primary调节的时候只考虑方法1(减少copy primary带来的数据拷贝)
    BALANCER_IN_TURN = 'meta.lb.balancer_in_turn'  # 各个表的balancer用串行的方式做，而不是并行进行(用于调试，观察系统行为)
    ALL = [ONLY_PRIMARY_BALANCER, ONLY_MOVE_PRIMARY, BALANCER_IN_TURN]


def balance_and_wait(module_name, logger, balance_type=None, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
    """对外接口 发起负载均衡并等待返回"""
    runner = BalanceRunner(module_name, logger, balance_type, print_progress_fun, check_interval_seconds, check_timeout_seconds)
    runner.run()


def balance_no_wait(module_name, logger, balance_type=None, print_progress_fun=None):
    """对外接口 发起负载均衡然后返回 不等待"""
    runner = BalanceRunner(module_name, logger, balance_type, print_progress_fun)
    runner.start()


def check_balance(module_name, logger, print_progress_fun=None):
    """对外接口 发起负载均衡后调用本接口检查 返回true/false"""
    runner = BalanceRunner(module_name, logger, print_progress_fun=print_progress_fun)
    return runner.check()


class BalanceRunner(AsyncWaitRunner):
    """发起balance"""
    def __init__(self, module_name, logger, balance_type=None, print_progress_fun=None, check_interval_seconds=1, check_timeout_seconds=None):
        self.print_fun = print_progress_fun if print_progress_fun is not None else logger.info
        self.api = SkvAdminApi(logger, module_name)
        self.balance_type = balance_type
        self.logger = logger
        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """异步操作"""
        # 检查是不是集群中有需要 decommission 的节点
        # 目前 server 端的 balance 并不支持跳过 blacklist 标记的节点 balance
        black_list = self.api.get_replica_server_black_list()
        if len(black_list) > 0:
            raise Exception('some replica_server(%s) are decommissioning, now not support balance!' % str(black_list))
        # saas 环境开启限流, 防止加节点时大量拷贝secondary副本导致带宽打满
        if is_hubble_installed():
            self.api.set_add_secondary_max_count_for_one_node(1)
        else:
            # 历史环境这里 balance 均已经设置为 1，所以需要改回 DEFAULT
            self.api.set_add_secondary_max_count_for_one_node('DEFAULT')
        self.print_fun('sending balance command...')
        self.api.set_meta_level(self.api.META_LEVEL_LIVELY)
        if self.balance_type:
            if self.balance_type not in BalanceType.ALL:
                raise Exception('invalid balance type %s: candidates %s' % (self.balance_type, BalanceType.ALL))
            self.api._send_remote_command_to_meta_server(self.balance_type, 'true')
            self.logger.info('set balance type %s' % self.balance_type)

    def execute_check(self):
        """执行一次检查"""
        return self.api.get_balance_operation_count()

    def check_wait_done(self, res):
        """检查是否balance完成"""
        return 'total=0' in res

    def print_progress(self, res):
        """打印进度 res是execute_check()的结果"""
        if self.check_wait_done(res):
            prefix = 'balance done'
        else:
            level = self.api.get_meta_level()
            if level == SkvAdminApi.META_LEVEL_LIVELY:
                prefix = 'balance in progress'
            elif level == SkvAdminApi.META_LEVEL_STEADY:
                prefix = 'balance not started'
            else:
                prefix = 'balance status unknown'
        self.print_fun(prefix + ': ' + res)

    def on_finish(self, res):
        self.print_fun(res)
        # 完成后需要关闭设置steady
        self.api.set_meta_level(self.api.META_LEVEL_STEADY)
        self.api.set_add_secondary_max_count_for_one_node('DEFAULT')
