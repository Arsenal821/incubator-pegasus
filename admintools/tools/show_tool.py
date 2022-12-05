#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import json
from prettytable import PrettyTable

from base_tool import BaseTool
from skv_admin_api import SkvAdminApi
from skv_common import SKV_MODULE_NAME_LIST, SKV_OFFLINE_MODULE_NAME


class ShowTool(BaseTool):
    example_doc = '''
skvadmin show node -d # show replica count on nodes
skvadmin show node -q # show avg qps/latency on nodes
skvadmin show cluster # show cluster info
'''

    def init_parser(self, subparser):
        show_subparsers = subparser.add_subparsers(dest='show_subparser_name')

        show_table_subparser = show_subparsers.add_parser(
            'table',
            help='show skv cluster tables info')
        show_table_subparser.add_argument('-m', '--module',
                                          type=str, default=SKV_OFFLINE_MODULE_NAME,
                                          choices=SKV_MODULE_NAME_LIST,
                                          help='module name, skv_offline/skv_online')
        format_group = show_table_subparser.add_mutually_exclusive_group(required=False)
        format_group.add_argument('-a', '--all',
                                  action="store_true",
                                  help='show all status table')
        format_group.add_argument('-d', '--detailed',
                                  action="store_true",
                                  help='show table healthy info')
        format_group.add_argument('-o', '--only',
                                  action="store_true",
                                  default=False,
                                  help='show table that actually doing I/O, only works with -d')

        show_node_subparser = show_subparsers.add_parser(
            'node',
            help='show skv cluster replicaServer nodes info')
        show_node_subparser.add_argument('-m', '--module',
                                         type=str, default=SKV_OFFLINE_MODULE_NAME,
                                         choices=SKV_MODULE_NAME_LIST,
                                         help='module name, skv_offline/skv_online')
        show_node_subparser.add_argument('-d', '--detailed',
                                         action="store_true",
                                         help='show replicaServer replica_count and role_count')
        show_node_subparser.add_argument('-q', '--qps',
                                         action="store_true",
                                         help='show replicaServer qps')
        show_node_subparser.add_argument('-r', '--resolve',
                                         action="store_true",
                                         help='resolve ip to hostname')

        show_cluster_subparser = show_subparsers.add_parser(
            'cluster',
            help='show skv cluster base info')
        show_cluster_subparser.add_argument('-m', '--module',
                                            type=str, default=SKV_OFFLINE_MODULE_NAME,
                                            choices=SKV_MODULE_NAME_LIST,
                                            help='module name, skv_offline/skv_online')
        show_cluster_subparser.add_argument('-r', '--resolve',
                                            action="store_true",
                                            help='resolve ip to hostname')
        self.show_parser = subparser

    def do(self, args):
        self.logger.debug(args)
        api = SkvAdminApi(self.logger, args.module)
        if args.show_subparser_name == 'node':
            if args.resolve and args.detailed:
                return self.show_execute(api, args.module, 'nodes -rd')
            elif args.resolve:
                return self.show_execute(api, args.module, 'nodes -r')
            elif args.detailed:
                return self.show_execute(api, args.module, 'nodes -d')
            elif args.qps:
                return self.show_execute(api, args.module, 'nodes -q')
            else:
                return self.show_execute(api, args.module, 'nodes')
        elif args.show_subparser_name == 'table':
            if args.detailed:
                self._show_table_in_details(api, args.module)
            elif args.only:
                self._show_table_in_details(api, args.module, only=True)
            elif args.all:
                return self.show_execute(api, args.module, 'ls -a')
            else:
                self._show_table_in_details(api, args.module)
        elif args.show_subparser_name == 'cluster':
            if args.resolve:
                return self.show_execute(api, args.module, 'cluster_info -r')
            else:
                return self.show_execute(api, args.module, 'cluster_info')
        else:
            self.show_parser.print_help()

    def show_execute(self, api, module, exec_cmd):
        output_str, _ = api._get_execute_shell_stdout_and_stderr(exec_cmd)
        if output_str.find('ERR_NETWORK_FAILURE') != -1:
            self.logger.error(module + ' is not running, ' + 'ERR_NETWORK_FAILURE')
            return 1
        self.logger.info(api._shell_output_strip(output_str))

    def _show_table_in_details(self, api, module, only=False):
        """输出每个表的分片数，unhealthy的分片数，qps和相关统计信息
这里直接用SkvAdminApi里面的_get_execute_shell_output了，其实不太好，但是考虑到是内部工具 暂时这样写了
        """
        unhealthy_app_num = 0

        # 所有分片信息
        output = api._get_execute_shell_output('ls -d -j')
        d = json.loads(output)
        app_id_to_desc = {}
        for app_id, info in d['general_info'].items():
            app_id_to_desc[app_id] = {}
            app_id_to_desc[app_id].update(info)
            app_id_to_desc[app_id].update(d['healthy_info'][app_id])
        for _, desc in app_id_to_desc.items():
            desc['partition'] = '%(fully_healthy)s/%(partition_count)s healthy' % desc
            desc['id'] = desc['app_id']
            if desc['unhealthy'] != '0':
                desc['partition'] += '[%(write_unhealthy)s w, %(read_unhealthy)s r]' % desc
                unhealthy_app_num += 1

        # 统计qps 大小等信息
        d = api.get_app_stat()
        for _, desc in app_id_to_desc.items():
            stat_info = d['app_stat'][desc['app_name']]
            desc.update(stat_info)

        # 表级别聚合
        cols = ['id', 'app_name', 'partition', 'GET', 'MGET', 'BGET', 'PUT', 'MPUT', 'INCR', 'SCAN', 'file_mb', 'file_num', 'hit_rate']
        table = PrettyTable(cols)
        for c in cols:
            if c in ['app_name', 'partition']:
                table.align[c] = 'l'  # 字符类输出左对齐 方便阅读
            else:
                table.align[c] = 'r'  # 数字类全部右对齐 方便对比数字
        table_row_count = 0
        for _, desc in app_id_to_desc.items():
            if only:
                # 检查如果完全没有流量 就忽略
                for op in SkvAdminApi.APP_QPS_STAT_OP_COLUMN_LIST:
                    if desc[op] != '0.00':
                        break
                else:
                    # 完全没有流量 所有的操作都是0.00
                    continue
            table.add_row([desc[c] for c in cols])
            table_row_count += 1
        msg = '; %d has actually doing I/O' % table_row_count if only else ''
        if table_row_count > 0:
            msg += '\n' + table.get_string()
        self.logger.info('%s total %d tables, %d unhealthy%s' % (module, len(app_id_to_desc), unhealthy_app_num, msg))
