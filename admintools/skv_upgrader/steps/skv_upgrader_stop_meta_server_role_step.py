# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import stop_all_meta_servers
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderStopMetaServerRoleStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        stop_all_meta_servers(
            self.skv_module_name, self.logger,
            print_progress_fun=self.print_msg_to_screen,
        )

    def check(self):
        return True

    def rollback(self):
        pass
