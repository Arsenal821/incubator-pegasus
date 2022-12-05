# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from abc import ABC
import os
import socket
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep


class SkvUpgraderHostStep(SkvUpgraderStep, ABC):
    def __init__(self, host, my_meta_server_excluded=False):
        super().__init__()

        self.skv_host = host
        self.skv_ip = socket.gethostbyname(host)
        self.my_meta_server_excluded = my_meta_server_excluded

        if my_meta_server_excluded:
            new_endpoint_list = list(filter(
                lambda endpoint: self.skv_ip != socket.gethostbyname(endpoint.split(':')[0]),
                self.skv_admin_api.meta_server_endpoint.split(',')
            ))
            self.skv_admin_api.meta_server_endpoint = ','.join(new_endpoint_list)
