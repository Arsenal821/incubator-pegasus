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
from skv_admin_api import SkvAdminApi
from skv_maintenance_workers.skv_server_alive_worker import SkvServerAliveWorker


class SkvMetaServerAliveWorker(SkvServerAliveWorker):
    def __init__(self, module_name, logger):
        super().__init__(
            module_name=module_name,
            role_name='meta_server',
            logger=logger,
        )

    def get_unalive_server_list(self):
        return SkvAdminApi(self.logger, self.module).get_unalive_meta_server_list()
