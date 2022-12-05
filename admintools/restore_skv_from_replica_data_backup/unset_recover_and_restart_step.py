#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

清楚recover模式 然后重启
"""
import os
import sys
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from recipes import wait_all_meta_servers_available, wait_table_healthy

from hyperion_utils.shell_utils import check_call


class UnsetRecoverAndRestartStep(BaseRestoreStep):
    def do_update(self):
        # 1. 设置recover模式
        cmd = 'skvadmin config set -m %s -r meta_server -s meta_server -n recover_from_replica_server -v false -y' % self.module_name
        check_call(cmd, self.logger.debug)

        # 2. 启动skv
        self.print_msg_to_screen('restart %s...' % self.module_name)
        cmd = 'spadmin restart -m %s -p skv -s' % self.module_name
        check_call(cmd, self.logger.debug)

        # pegasus的元数据不同步 随机等待一段时间
        self.print_msg_to_screen('wait 30s...')
        time.sleep(30)

        # 3. 等待表加载
        wait_all_meta_servers_available(self.module_name, self.logger, self.print_msg_to_screen)
        wait_table_healthy(self.module_name, self.logger, self.print_msg_to_screen)
