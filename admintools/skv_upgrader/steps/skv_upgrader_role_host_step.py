# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from abc import ABC
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_upgrader.steps.skv_upgrader_host_step import SkvUpgraderHostStep
from recipes import get_skv_config_manager


class SkvUpgraderRoleHostStep(SkvUpgraderHostStep, ABC):
    def __init__(self, role_name, host):
        super().__init__(host)

        self.skv_role_name = role_name
        self.skv_role_port = get_skv_config_manager(self.skv_module_name, role_name, self.logger).get_default_port()
        self.skv_role_instance = "{host}:{port}".format(host=host, port=self.skv_role_port)
        self.skv_role_node = "{ip}:{port}".format(ip=self.skv_ip, port=self.skv_role_port)
