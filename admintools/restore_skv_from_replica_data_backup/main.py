#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import argparse
import os
import socket
import sys

from stepworker.server import BaseServer, ContextProcessType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_common import SKV_MODULE_NAME_LIST, SKV_TOOLS_STEPWORKER_NAME, get_context_details, \
    SKV_TOOLS_RESTORE_FROM_BACKUP_REPLICA_OPERATION, assert_context_consistent, SKV_REPLICA_SERVER_ROLE_NAME
from recipes import check_health, get_skv_config_manager
from skv_admin_api import SkvAdminApi


class RestoreSkvFromReplicaDataBackupTool(BaseTool):
    example_doc = '''
spadmin skv restore_skv_from_replica_data_backup -m skv_offline \\
    --restore_from_backup_path_on_each_host /sensorsdata/main/packages/backup_local
'''

    def init_parser(self, subparser):
        subparser.add_argument(
            '-m', '--module',
            required=True,
            choices=SKV_MODULE_NAME_LIST,
            help='module name, skv_offline/skv_online')
        subparser.add_argument(
            '--restore_from_backup_path_on_each_host',
            required=True,
            help='replica will be restored from this path on EACH REPLICA SERVER HOST.')
        # 隐藏接口
        subparser.add_argument(
            '--all_yes',
            help=argparse.SUPPRESS,
            default=False,
            action='store_true')

    def do(self, args):
        """从backup_replica_data 备份的replica server的数据恢复skv(可以从每台机器的同一个目录 也可以从执行机上统一目录)
        高危操作 所以名字比较长 避免有人意会随便执行
        """
        context_details = {
            'execute_host': socket.getfqdn(),
            'restore_from_backup_path_on_each_host': os.path.abspath(args.restore_from_backup_path_on_each_host),
            'module': args.module,
            'operation': SKV_TOOLS_RESTORE_FROM_BACKUP_REPLICA_OPERATION,
        }
        old_context_details = get_context_details()
        if old_context_details:
            # 确认上下文是一致的
            assert_context_consistent(self.logger, old_context_details, context_details)
        else:
            # 集群必须healthy
            if not check_health(self.logger, args.module):
                raise Exception('cluster %s is unhealthy!' % args.module)
            # 检查是否已经有数据了
            self.logger.info('start counting tables...')
            api = SkvAdminApi(self.logger, args.module)
            bad_table_info = []
            for table in api.get_all_avaliable_table_name():
                cnt = api.count_table(table)
                if cnt != 0:
                    bad_table_info.append('table[%s] cnt[%d];' % (table, cnt))
            if bad_table_info:
                self.logger.error('!!!THERE ARE STILL SOME DATA IN THE CLUSTER!!!')
                for line in bad_table_info:
                    self.logger.error(line)
                self.logger.error('!!!ALL DATA IN CURRENT %s WILL BE LOST!!!' % args.module.upper())
                if not args.all_yes:
                    self.logger.warn('please enter[y] to confirm!!!')
                    response = input()
                    if response != 'y':
                        self.logger.info('response[%s] != y. Goodbye!!!' % response)
                        return 1
        # 必须有高危警告
        self.logger.error('!!!THIS COMMAND WILL RESTART %s AND CHANGE DATA!!!' % args.module.upper())
        if not args.all_yes:
            self.logger.warn('please enter[y] to confirm!!!')
            response = input()
            if response != 'y':
                self.logger.info('response[%s] != y. Goodbye!!!' % response)
                return 1

        # 执行的机器包括本机+所有replica server所在的机器
        skv_config_manager = get_skv_config_manager(args.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        hosts = list(set([socket.getfqdn()] + skv_config_manager.get_host_list()))
        step_class_path = os.path.join(os.environ['SKV_HOME'], 'admintools/restore_skv_from_replica_data_backup')
        context_type = ContextProcessType.NE_CREATE_E_CONTINUE if args.all_yes else ContextProcessType.NE_CREATE_E_ASK_MULTIPLE
        server = BaseServer(
            hosts=hosts,
            name=SKV_TOOLS_STEPWORKER_NAME,
            support_rollback=False,
            step_class_path=step_class_path,
            logger=self.logger,
            context_type=context_type,
            context_details=context_details)
        server.init_context()
        server.execute_one_by_one()
