# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import balance_and_wait
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderBalanceAndWaitStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        for _ in range(60):
            balance_operation = self.skv_admin_api.get_balance_operation_count()
            if 'total=0' not in balance_operation:
                break
            time.sleep(1)

        balance_and_wait(
            self.skv_module_name, self.logger,
            print_progress_fun=self.print_msg_to_screen,
        )

    def check(self):
        return True

    def rollback(self):
        pass
