#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

负载均衡管理工具
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_admin_api import SkvAdminApi
from recipes import balance_no_wait, balance_and_wait, check_balance, BalanceType, nonstandard_balance, \
    nonstandard_check_balance
from skv_common import SKV_MODULE_NAME_LIST, get_skv_cluster_type, SKVClusterType


class BalanceTool(BaseTool):
    example_doc = '''
skvadmin balance start -m skv_offline # 开始balance
'''

    def init_parser(self, subparser):
        balance_subparsers = subparser.add_subparsers(dest='balance_op')
        stop_balance_subparser = balance_subparsers.add_parser('stop', help='stop skv cluster data balance')
        stop_balance_subparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')

        start_balance_subparser = balance_subparsers.add_parser('start', help='start skv cluster data balance')
        start_balance_subparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')
        start_balance_subparser.add_argument(
            '--nowait', action='store_true', help='if set, will only send balance command. please use skvadmin balance check to check progress')
        start_balance_subparser.add_argument(
            '--balance_type', choices=BalanceType.ALL, help='set balance type')

        check_balance_subparser = balance_subparsers.add_parser('check', help='check skv cluster data balance progress')
        check_balance_subparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')

        self.parser = subparser

    def do(self, args):
        # 单机不支持
        cluster_type = get_skv_cluster_type(args.module)
        if cluster_type == SKVClusterType.ONE_NODE:
            raise Exception('this operation requires cluster!')
        if args.balance_op == 'stop':
            return self._stop_balance(args.module)
        elif args.balance_op == 'start':
            return self._start_balance(cluster_type, args.module, args.nowait, args.balance_type)
        elif args.balance_op == 'check':
            return self._check_balance(cluster_type, args.module)
        else:
            self.parser.print_help()

    def _stop_balance(self, module):
        api = SkvAdminApi(self.logger, module)
        api.set_meta_level(api.META_LEVEL_STEADY)

    def _start_balance(self, cluster_type, module, nowait, balance_type):
        if cluster_type == SKVClusterType.TWO_NODE:
            if nowait or balance_type:
                self.logger.warn("only two replica_server skv cluster not support nowait or balance_type.")
            nonstandard_balance(module, self.logger)
        else:
            if nowait:
                balance_no_wait(module, self.logger, balance_type)
            else:
                balance_and_wait(module, self.logger, balance_type)

    def _check_balance(self, cluster_type, module):
        if cluster_type == SKVClusterType.TWO_NODE:
            ret = nonstandard_check_balance(module, self.logger)
        else:
            ret = check_balance(module, self.logger)
        if ret:
            return 0
        else:
            return 1
