# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import safely_stop_replica_server
from skv_upgrader.steps.skv_upgrader_replica_server_step import SkvUpgraderReplicaServerStep


class SkvUpgraderSafelyStopReplicaServerStep(SkvUpgraderReplicaServerStep):
    def __init__(self, host):
        super().__init__(host)

    def backup(self):
        pass

    def update(self):
        self.logger.info(
            "trying to stop replica server safely, module={module}, "
            "instance={instance}".format(
                module=self.skv_module_name, instance=self.skv_role_instance,
            )
        )

        safely_stop_replica_server(
            self.skv_module_name, self.logger, self.skv_role_instance,
            print_progress_fun=self.print_msg_to_screen,
            my_meta_server_excluded=True,
        )

        time.sleep(1)

    def check(self):
        return True

    def rollback(self):
        pass
