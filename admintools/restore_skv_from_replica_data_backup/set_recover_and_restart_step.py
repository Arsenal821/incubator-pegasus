#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys
import time

from hyperion_utils.shell_utils import check_call
from hyperion_client.module_service import ModuleService

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from skv_common import SKV_PRODUCT_NAME


class SetRecoverAndRestartStep(BaseRestoreStep):
    def do_update(self):
        # 1. 设置recover模式
        cmd = 'skvadmin config set -m %s -r meta_server -s meta_server -n recover_from_replica_server -v true -y' % self.module_name
        check_call(cmd, self.logger.debug)

        # 2. 启动skv
        self.print_msg_to_screen('start %s...' % self.module_name)
        ModuleService().start(SKV_PRODUCT_NAME, self.module_name)

        # 按理说启动后需要检查是否加载 但是recover模式所有命令都失效了 所以此处只是简单sleep
        # 有一定概率sleep也没加载完 没关系 重试就好
        time.sleep(30)
