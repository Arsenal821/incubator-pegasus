#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

"""

import json
import os
import sys

from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import SKV_MODULE_NAME_LIST, SKV_TOOLS_MIGRATE_OPERATION
from skv_step_worker.step_worker_connector import StepWorkerConnector


def migrate(logger, module: str, ssh_host: str, ssh_password: str, ssh_port: int = 22,
            skip_table_names: str = None, assign_table_names: str = None, max_batch_count: int = 500):
    """
    skv 迁移程序入口
    参数详情:
    logger: 标准日志函数
    module: skv_offline/skv_online
    ssh_host: 迁移原集群的主机名或 ip, 填写一个即可
    ssh_password: ssh_host 机器 ssh 的密码
    ssh_port: ssh_host 机器 ssh 的端口号
    skip_table_names: 指定需要忽略迁移的表名，逗号分割
    assign_table_names: 指定迁移表，逗号分割
    max_batch_count: 单分片 set 最大并发数，可用于控制迁移速度及资源消耗
    """
    if module not in SKV_MODULE_NAME_LIST:
        raise Exception('Invalid module(%s), please input valid module! skv_offline/skv_online!' % module)
    skip_table_names = skip_table_names.split(',') if skip_table_names else list()
    assign_table_names = assign_table_names.split(',') if assign_table_names else list()

    # context_details 记录影响上下文的参数
    context_details = {}
    context_details['module_name'] = module
    context_details['skip_table_names'] = skip_table_names
    context_details['assign_table_names'] = assign_table_names

    # params_dict 记录可以更改的参数
    params_dict = {}
    params_dict['max_batch_count'] = max_batch_count
    params_dict['password'] = ssh_password
    params_dict['port'] = ssh_port
    params_dict['host'] = ssh_host

    ssh_client = SSHConnector.get_instance(
        hostname=ssh_host,
        user='sa_cluster',
        password=ssh_password,
        ssh_port=ssh_port,
    )

    # 源端的 skv 可能属于 mothership，通过环境变量判断是否为 sp 2.1
    output = ssh_client.check_output('echo $MOTHERSHIP_HOME', logger.debug)
    if 'mothership' in output.strip():
        get_meta_server_list_cmd = ' mothershipadmin module connection_info get -m %s -f json' % module
        output = ssh_client.check_output(get_meta_server_list_cmd, logger.debug)
        src_meta_cluster_server_list = ','.join(
            json.loads(output.replace('\n', '').replace(' ', ''))['meta_server_list'])

    else:
        # 源端的 skv 可能在产品线 skv 下, 也可能在 sp 下, 这里做一下判断
        output = ssh_client.check_output('spadmin upgrader version 2>&1', logger.debug)
        product = 'sp'
        if 'skv current version' in output.strip():
            product = 'skv'
        get_meta_server_list_cmd = 'spadmin config get server -m %s -n meta_server_list -c -p %s' % (module, product)
        out = ssh_client.check_output(get_meta_server_list_cmd, logger.debug)
        # ssh 拿到的信息需要去格式化
        src_meta_cluster_server_list = ','.join(json.loads(out))

    context_details['src_meta_server_list'] = src_meta_cluster_server_list

    swc = StepWorkerConnector(logger, module, SKV_TOOLS_MIGRATE_OPERATION, context_details, params_dict)
    swc.init_context()
    swc.execute_one_by_one()
