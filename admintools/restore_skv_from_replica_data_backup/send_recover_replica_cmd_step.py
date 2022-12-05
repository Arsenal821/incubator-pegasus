#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

准备文件列表，然后执行命令
"""
import os
import socket
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from skv_admin_api import SkvAdminApi
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager


class SendRecoverReplicaCmdStep(BaseRestoreStep):
    def do_update(self):
        # 1. 写入replica server列表
        recover_node_list = os.path.join(self.get_stepworker_work_dir(), 'recover_node_list')
        with open(recover_node_list, 'w+') as f:
            skv_config_manager = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
            port = skv_config_manager.get_default_port()
            for host in skv_config_manager.get_host_list():
                f.write('%s:%d\n' % (socket.gethostbyname(host), port))

        # 2. 发送recover命令
        api = SkvAdminApi(self.logger, self.module_name)
        stdout, stderr = api._get_execute_shell_stdout_and_stderr('recover -f %s' % recover_node_list)
        if 'Recover result: ERR_OK' not in stdout:
            self.logger.error('send recover from %s failed!' % recover_node_list)
            self.logger.error('stdout\n:%s\nstderr:\n%s' % (stdout, stderr))
            raise Exception('failed to send recover command!')
        self.logger.info('send recover cmd succeed')
