#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

安全重启各组件
"""
from itertools import islice
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_admin_api import SkvAdminApi
from recipes import restart_all_meta_server, restart_primary_meta_server, safely_restart_replica_server, get_skv_config_manager
from skv_common import SKV_MODULE_NAME_LIST, SKV_REPLICA_SERVER_ROLE_NAME, get_skv_cluster_type, SKVClusterType


class RestartTool(BaseTool):
    example_doc = '''
skvadmin restart all_replica_server -m skv_offline [-g GROUP_NAME]
skvadmin restart all_meta_server -m skv_offline
skvadmin restart primary_meta_server -m skv_offline
'''

    def init_parser(self, subparser):
        restart_suparsers = subparser.add_subparsers(dest='restart_type')
        # 重启全部replica server
        # 重启primary meta server
        # 重启全部meta server
        restart_type_and_comment = {
            'all_replica_server': 'safely rolling restart all replica server',
            'primary_meta_server': 'restart only primary meta server',
            'all_meta_server': 'restart all meta server'
        }
        for cmd, comment in restart_type_and_comment.items():
            s = restart_suparsers.add_parser(cmd, help=comment)
            s.required = True
            s.add_argument('-m', '--module', choices=SKV_MODULE_NAME_LIST, default='skv_offline', help='module name, skv_offline/skv_online', required=True)
            if cmd == 'all_replica_server':
                # 重启replica server如果失败 可以通过--start_index继续
                s.add_argument('--start_index', type=int, default=1, help='if set, will start from this index')
                s.add_argument('-g', '--group', type=str, required=False, help='group name')
        self.parser = subparser

    def do(self, args):
        # 单机不支持
        if get_skv_cluster_type(args.module) == SKVClusterType.ONE_NODE:
            raise Exception('this operation requires cluster!')
        if args.restart_type == 'all_replica_server':
            # 按照 start_index 逐一重启
            api = SkvAdminApi(self.logger, args.module)
            if args.group:
                skv_config_manager = get_skv_config_manager(args.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
                port = skv_config_manager.get_default_port()
                hosts = skv_config_manager.get_config_group_hosts(args.group)
                if not hosts:
                    raise Exception("no server found for group `{group}`!".format(group=args.group))
                replica_server_list = sorted(['%s:%d' % (host, port) for host in hosts])
            else:
                replica_server_list = sorted(api.get_all_replica_server())
            if args.start_index > len(replica_server_list) or args.start_index < 1:
                raise Exception('failed to restart from index %d! total %d replica servers!' % (args.start_index, len(replica_server_list)))
            for i, replica_server in islice(enumerate(replica_server_list, start=1), args.start_index - 1, None):
                self.logger.info('%d/%d restart %s' % (i, len(replica_server_list), replica_server))
                safely_restart_replica_server(args.module, self.logger, replica_server, self.logger.info, None)
        elif args.restart_type == 'all_meta_server':
            restart_all_meta_server(args.module, self.logger)
        elif args.restart_type == 'primary_meta_server':
            restart_primary_meta_server(args.module, self.logger)
        else:
            self.parser.print_help()
