#!/bin/env python
# -*- coding: UTF-8 -*-
"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv migrate
"""

import datetime
import os
import socket
import sys
import yaml

from hyperion_client.directory_info import DirectoryInfo

from stepworker.server import BaseServer, ContextProcessType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_context_details, SKV_TOOLS_STEPWORKER_NAME,\
    SKV_TOOLS_MIGRATE_STEPS_PATH, SKV_TOOLS_MIGRATE_OPERATION, SKV_MODULE_NAME_LIST

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool


def migrate_data(logger, module, src_cluster_hostname, ssh_port, ssh_password, skip_table_names, assign_table_names, max_batch_count):
    context_details = get_context_details()
    if context_details:
        logger.info("context details {context_details}".format(context_details=context_details))
        _check_context_details(context_details, module, src_cluster_hostname)
        migrate_parameters_file = context_details['migrate_parameters_file']
    else:
        # 为了支持 skip_table_names 的变更生成的文件
        migrate_parameters_file = os.path.join(
            DirectoryInfo().get_runtime_dir_by_product('sp'),
            '%s_migrate_skip_tables_%s' % (module, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
        )

    with open(migrate_parameters_file, 'w') as f:
        migrate_msg = {}
        migrate_msg['ssh_host'] = src_cluster_hostname
        migrate_msg['ssh_port'] = ssh_port
        migrate_msg['max_batch_count'] = max_batch_count
        migrate_msg['ssh_password'] = ssh_password
        migrate_msg['skip_table_names'] = skip_table_names if skip_table_names else ''
        migrate_msg['assign_table_names'] = assign_table_names if assign_table_names else ''

        yaml.dump(migrate_msg, f, default_flow_style=False)

    server = BaseServer(
        hosts=[socket.getfqdn()],
        name=SKV_TOOLS_STEPWORKER_NAME,
        support_rollback=False,
        step_class_path=SKV_TOOLS_MIGRATE_STEPS_PATH,
        logger=logger,
        context_type=ContextProcessType.NE_CREATE_E_ASK,
        context_details={
            'execute_host': socket.getfqdn(),
            'migrate_parameters_file': migrate_parameters_file,
            'module_name': module,
            'operation': SKV_TOOLS_MIGRATE_OPERATION,
        },
    )
    server.init_context()
    if server.execute_one_by_one() != 0:
        raise Exception("migrate error! please check log.")


def migrate_data2(logger, module, src_cluster_hostname, ssh_port, ssh_password, skip_table_names, assign_table_names, max_batch_count=500):
    context_details = get_context_details()
    if context_details:
        logger.info("context details {context_details}".format(context_details=context_details))
        _check_context_details(context_details, module, src_cluster_hostname)
        migrate_parameters_file = context_details['migrate_parameters_file']
    else:
        # 为了支持 skip_table_names 的变更生成的文件
        migrate_parameters_file = os.path.join(
            DirectoryInfo().get_runtime_dir_by_product('sp'),
            '%s_migrate_skip_tables_%s' % (module, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
        )
    with open(migrate_parameters_file, 'w') as f:
        migrate_msg = {}
        migrate_msg['max_batch_count'] = max_batch_count
        migrate_msg['ssh_host'] = src_cluster_hostname
        migrate_msg['ssh_port'] = ssh_port
        migrate_msg['ssh_password'] = ssh_password
        migrate_msg['skip_table_names'] = skip_table_names if skip_table_names else ''
        migrate_msg['assign_table_names'] = assign_table_names if assign_table_names else ''

        yaml.dump(migrate_msg, f, default_flow_style=False)

    server = BaseServer(
        hosts=[socket.getfqdn()],
        name=SKV_TOOLS_STEPWORKER_NAME,
        support_rollback=False,
        step_class_path=SKV_TOOLS_MIGRATE_STEPS_PATH,
        logger=logger,
        context_type=ContextProcessType.NE_CREATE_E_CONTINUE,
        context_details={
            'execute_host': socket.getfqdn(),
            'migrate_parameters_file': migrate_parameters_file,
            'module_name': module,
            'operation': SKV_TOOLS_MIGRATE_OPERATION,
        },
    )
    server.init_context()
    if server.execute_one_by_one() != 0:
        raise Exception("migrate error! please check log.")


def _check_context_details(context_details, module, src_cluster_hostname):
    if context_details['operation'] != SKV_TOOLS_MIGRATE_OPERATION:
        raise Exception('you have a %s skv context unfinished!' % context_details['operation'])
    if context_details['module_name'] != module:
        raise Exception('module_name different from context %s!' % context_details['module_name'])
    if socket.getfqdn() != context_details['execute_host']:
        raise Exception('please execute on {host}'.format(host=context_details['execute_host']))


class MigrateTool(BaseTool):
    example_doc = '''
!!!PLEASE EXECUTE THIS CMD ON DEST CLUSTER!!!
skvadmin migrate -m skv_offline --skip_table_names impala_historical_profile \\
    --password xxx --src_cluster_hostname hybrid01.armada.debugresetreset42217.deploy.sensorsdata.cloud
'''

    def init_parser(self, migrate_suparser):
        migrate_suparser.add_argument(
            '-m',
            '--module',
            required=True,
            choices=SKV_MODULE_NAME_LIST,
            help='指定迁移的 skv 集群 module_name.'
        )
        migrate_suparser.add_argument(
            '--src_cluster_hostname',
            required=True,
            help='原集群机器的 fqdn，任意一个即可\n'
        )
        migrate_suparser.add_argument(
            '--port',
            required=False,
            default=22,
            type=int,
            help='原集群机器的 ssh 连接端口，默认 22\n'
        )
        migrate_suparser.add_argument(
            '--password',
            required=True,
            help='原集群机器的 sa_cluster 连接 ssh 密码\n'
        )

        # 迁移表可选传参值，二选一
        value_group = migrate_suparser.add_mutually_exclusive_group(required=False)
        value_group.add_argument(
            '--skip_table_names',
            help='指定需要跳过迁移的表名.\n'
                 '防止该表在新环境中有数据引起的错误异常报出.\n'
                 '可以传多个表名，英文逗号分隔'
        )
        value_group.add_argument(
            '--assign_table_names',
            help='指定需要迁移的表名.\n'
                 '可以传多个表名，英文逗号分隔'
        )

        migrate_suparser.add_argument(
            '--max_batch_count',
            required=False,
            default=500,
            type=int,
            help='单分片 set 最大并发数，可用于控制迁移速度及资源消耗, 默认 500\n'
        )

    def do(self, args):
        migrate_data(self.logger, args.module, args.src_cluster_hostname, args.port, args.password, args.skip_table_names, args.assign_table_names, args.max_batch_count)
