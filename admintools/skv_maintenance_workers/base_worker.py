#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv 自动检查任务的基类
"""

import datetime
import logging
import os
import socket

from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_client.directory_info import DirectoryInfo

from skv_common import get_server_log_dir_by_role, get_program_start_log_path_by_role, APP_NAME_TO_ROLE_NAME, SKV_PRODUCT_NAME


class BaseWorker:
    def __init__(self, module_name, logger):
        self.module = module_name
        self.logger = logger

    """检查状态是否异常。异常返回 True，即需要有后续动作；正常返回 False，即无后续动作"""
    def is_state_abnormal(self):
        return False

    """执行 spadmin skv health 后的诊断输出"""
    def diagnose(self):
        pass

    """schedule job 每晚 3 点运行的任务"""
    def repair(self):
        pass

    """任务是否是自动修复的， 如果是，则返回True，且即使is_state_abnormal为True 也只是打Info日志"""
    def self_remedy(self):
        return False

    def _do_remote_servers_shell_command_and_fetch_results(self, server_related_command):
        """
        server_related_command : {
            "host1:port1" : "shell-command#1",
            "host2:port2" : "shell-command#2",
        }

    返回数据格式:
    ret = {
    'host1:port1': 'shell command result str#1',
    'host2:port2': 'shell command result str#2'
    ...
    }
    """
        ret = {}
        for server in server_related_command:
            host = socket.getfqdn(server.split(':')[0])
            connector = SSHConnector.get_instance(host)
            one_real_command = server_related_command[server]
            ret[server] = connector.check_output(one_real_command, self.logger.debug)

        return ret

    def _dump_remote_startup_log_message_to_local(self, module, role, log_shell_template, server_list):
        startup_log_path = get_program_start_log_path_by_role(module_name=module, role_name=role)
        one_start_log_command = log_shell_template % startup_log_path
        server_related_command = {}
        for replica_server in server_list:
            server_related_command[replica_server] = one_start_log_command

        self._dump_remote_shell_command_result_to_local_log(module=module, role=role, log_type="server_startup",
                                                            server_related_command=server_related_command)

    def _dump_remote_log_message_to_local(self, module, role, log_shell_template, server_list):
        if role not in APP_NAME_TO_ROLE_NAME:
            raise Exception("skv server role : {role} is illegal,"
                            "because it is not in APP_NAME_TO_ROLE_NAME : {app_to_role_name}"
                            .format(role=role, app_to_role_name=str(APP_NAME_TO_ROLE_NAME)))

        server_related_command = {}
        for replica_server in server_list:
            one_replica_log_dir = get_server_log_dir_by_role(self.module, role)
            replica_log_path = os.path.join(one_replica_log_dir,
                                            '{app_name}.log'.format(app_name=APP_NAME_TO_ROLE_NAME[role]))
            one_log_command = log_shell_template % replica_log_path
            server_related_command[replica_server] = one_log_command

        self._dump_remote_shell_command_result_to_local_log(module=module, role=role, log_type="server_fixed_size",
                                                            server_related_command=server_related_command)

    def _dump_remote_shell_command_result_to_local_log(self, module, role, log_type, server_related_command, log_level=logging.ERROR):
        log_dir = os.path.join(
            DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME),
            module,
            '{log_type}_error_message'.format(log_type=log_type)
        )
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(
            DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME),
            module,
            '{log_type}_error_message'.format(log_type=log_type),
            '{role_name}_{log_type}_log_{timestamp}.log'.format(
                role_name=role,
                log_type=log_type,
                timestamp=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            )
        )

        ret = self._do_remote_servers_shell_command_and_fetch_results(server_related_command)
        with open(log_file, 'w') as f:
            for node, reason in ret.items():
                msg = '------{role_name} {node} {log_type}------\nexec command:\n{exec_command}\noutput:\n{reason}\n\n'.format(
                    role_name=role,
                    log_type=log_type,
                    node=node,
                    exec_command=server_related_command[node],
                    reason=reason,
                )
                f.write(msg)

        msg = 'Dumped {role_name} {log_type} log on {file_name}'.format(role_name=role, log_type=log_type, file_name=log_file)
        self.logger.log(log_level, msg)
        return log_file
