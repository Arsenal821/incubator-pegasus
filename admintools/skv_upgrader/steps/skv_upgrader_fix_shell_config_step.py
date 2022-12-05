# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import fix_shell_config, SKV_MODULE_NAME_LIST
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderFixShellConfigStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        meta_server_nodes_map = {
            name: node_list
            for name, node_list in self.other_meta_server_nodes_map.items()
        }
        meta_server_nodes_map[self.skv_module_name] = self.skv_meta_server_node_list
        fix_shell_config(SKV_MODULE_NAME_LIST, meta_server_nodes_map)

    def check(self):
        return True

    def rollback(self):
        pass
