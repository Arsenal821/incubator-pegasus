# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import (
    wait_all_meta_servers_available,
    wait_replica_server,
    wait_table_healthy,
)
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderWaitAllHealthyStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        wait_all_meta_servers_available(self.skv_module_name, self.logger,
                                        print_progress_fun=self.print_msg_to_screen)
        wait_replica_server(self.skv_module_name, self.logger,
                            print_progress_fun=self.print_msg_to_screen)
        wait_table_healthy(self.skv_module_name, self.logger,
                           print_progress_fun=self.print_msg_to_screen)

    def check(self):
        return True

    def rollback(self):
        pass
