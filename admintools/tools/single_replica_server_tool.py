#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

安全启停replica server
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from recipes import safely_restart_replica_server, safely_stop_replica_server, start_and_check_replica_server, get_skv_config_manager
from skv_common import SKV_MODULE_NAME_LIST, SKV_REPLICA_SERVER_ROLE_NAME, get_skv_cluster_type, SKVClusterType


class SingleReplicaServerTool(BaseTool):
    example_doc = '''
skvadmin single_replica_server safe_stop -m skv_offline -H hybrid03.debugresetreset19193.sensorsdata.cloud
skvadmin single_replica_server start_and_check -m skv_offline -H hybrid03.debugresetreset19193.sensorsdata.cloud
skvadmin single_replica_server safe_restart -m skv_offline -H hybrid03.debugresetreset19193.sensorsdata.cloud
'''

    def init_parser(self, subparser):
        replica_server_op_suparsers = subparser.add_subparsers(dest='replica_server_op')
        for cmd in ['safe_stop', 'start_and_check', 'safe_restart', 'prepare_stop', 'check_after_start']:
            s = replica_server_op_suparsers.add_parser(cmd, help='do %s on specific replica server' % cmd)
            s.required = True
            s.add_argument('-m', '--module', choices=SKV_MODULE_NAME_LIST, default='skv_offline', help='module name, skv_offline/skv_online', required=True)
            s.add_argument('-H', '--host', help='specify replica server host', required=True)
        self.parser = subparser

    def do(self, args):
        # 单机不支持
        if get_skv_cluster_type(args.module) == SKVClusterType.ONE_NODE:
            raise Exception('this operation requires cluster!')
        port = get_skv_config_manager(args.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_default_port()
        replica_addr = '%s:%d' % (args.host, port)
        if args.replica_server_op == 'safe_stop':
            safely_stop_replica_server(args.module, self.logger, replica_addr)
        elif args.replica_server_op == 'safe_restart':
            safely_restart_replica_server(args.module, self.logger, replica_addr)
        elif args.replica_server_op == 'start_and_check':
            start_and_check_replica_server(args.module, self.logger, replica_addr)
        else:
            self.parser.print_help()
