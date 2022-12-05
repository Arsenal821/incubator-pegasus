# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from abc import ABC
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_upgrader.steps.skv_upgrader_role_host_step import SkvUpgraderRoleHostStep
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME


class SkvUpgraderReplicaServerStep(SkvUpgraderRoleHostStep, ABC):
    def __init__(self, host):
        super().__init__(SKV_REPLICA_SERVER_ROLE_NAME, host)
