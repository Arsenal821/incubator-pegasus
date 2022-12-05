#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv 实例存活状态检测任务
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import get_program_start_log_path_by_role


class SkvServerAliveWorker(BaseWorker):
    def __init__(self, module_name, role_name, logger):
        self.module = module_name
        self.logger = logger
        self.role_name = role_name

    def is_state_abnormal(self):
        if self.get_unalive_server_list():
            return True
        else:
            return False

    def diagnose(self):
        unlived_server_list = self.get_unalive_server_list()
        one_startup_log_command_template = "tail -n 10 " + "%s"
        self._dump_remote_startup_log_message_to_local(module=self.module, role="replica_server",
                                                       log_shell_template=one_startup_log_command_template,
                                                       server_list=unlived_server_list)

    def repair(self):
        unlived_server_list = self.get_unalive_server_list()
        one_log_related_command = "tail -n 10 " + get_program_start_log_path_by_role(self.module, self.role_name)
        server_related_command = dict()
        for one_server in unlived_server_list:
            server_related_command[one_server] = one_log_related_command
        ret = self._do_remote_servers_shell_command_and_fetch_results(server_related_command=server_related_command)
        for node, reason in ret.items():
            msg = '{role_name} {node} is unalive, log output {reason}'.format(
                role_name=self.role_name,
                node=node,
                reason=reason
            )
            self.logger.error(msg)

    def get_unalive_server_list(self):
        return None
