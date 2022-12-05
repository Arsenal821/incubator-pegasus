# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import stop_meta_server
from skv_upgrader.steps.skv_upgrader_role_host_step import SkvUpgraderRoleHostStep
from skv_common import SKV_META_SERVER_ROLE_NAME


class SkvUpgraderStopMetaServerInstanceStep(SkvUpgraderRoleHostStep):
    def __init__(self, host):
        super().__init__(SKV_META_SERVER_ROLE_NAME, host)

    def backup(self):
        pass

    def update(self):
        if self.skv_ip in self.skv_role_ip_list_map[self.skv_role_name]:
            stop_meta_server(
                self.skv_module_name, self.logger, self.skv_role_instance,
                print_progress_fun=self.print_msg_to_screen,
            )

    def check(self):
        return True

    def rollback(self):
        pass
