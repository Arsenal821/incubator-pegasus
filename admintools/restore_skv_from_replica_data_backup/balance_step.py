#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

发起balance
"""
import os
import sys

skv_tools_path = os.path.join(os.environ['SKV_HOME'], 'admintools/skv_tools')
if skv_tools_path not in sys.path:
    sys.path.append(skv_tools_path)

from recipes import balance_and_wait, nonstandard_balance
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from skv_common import get_skv_cluster_type, SKVClusterType


class BalanceStep(BaseRestoreStep):

    def do_update(self):
        cluster_type = get_skv_cluster_type(self.module_name)
        if cluster_type == SKVClusterType.ONE_NODE:
            # 单机不balance
            self.logger.info('standalone cluster skip balance')
        elif cluster_type == SKVClusterType.TWO_NODE:
            # 2节点需要用nonstandard_balance
            nonstandard_balance(self.module_name, self.logger)
        else:
            balance_and_wait(self.module_name, self.logger, print_progress_fun=self.print_msg_to_screen)
