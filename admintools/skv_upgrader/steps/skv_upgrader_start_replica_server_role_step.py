# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import get_service_controller
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderStartReplicaServerRoleStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        get_service_controller(self.logger, self.skv_module_name).start_all_replica_server()

    def check(self):
        return True

    def rollback(self):
        pass
