# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep

from hyperion_utils.shell_utils import check_call


class SkvUpgraderReplaceWithNewModuleDirStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        cmd = "rm -rf {path}".format(path=self.skv_module_dir)
        check_call(cmd, self.logger.debug)

        cmd = "cp -a {src} {dest}".format(src=self.skv_new_module_dir, dest=self.skv_product_dir)
        check_call(cmd, self.logger.debug)

    def check(self):
        return True

    def rollback(self):
        cmd = "rm -rf {path}".format(path=self.skv_module_dir)
        check_call(cmd, self.logger.debug)

        cmd = "cp -a {src} {dest}".format(src=self.skv_old_module_dir, dest=self.skv_product_dir)
        check_call(cmd, self.logger.debug)
