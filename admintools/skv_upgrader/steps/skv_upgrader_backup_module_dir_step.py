# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import socket
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep

from hyperion_utils.shell_utils import check_call


class SkvUpgraderBackupModuleDirStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        cmd = "rm -rf {path}".format(path=self.skv_old_dir)
        check_call(cmd, self.logger.debug)

        cmd = "mkdir -p {path}".format(path=self.skv_old_dir)
        check_call(cmd, self.logger.debug)

        if os.path.exists(self.skv_module_dir):
            cmd = "cp -r {src} {dest}".format(src=self.skv_module_dir, dest=self.skv_old_dir)
            check_call(cmd, self.logger.debug)
        else:
            self.logger.info("{fqdn} not have dir {path}, needn\'t to backup.".format(
                fqdn=socket.getfqdn(), path=self.skv_module_dir))

    def check(self):
        return True

    def rollback(self):
        pass
