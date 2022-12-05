#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv balance 检测任务
"""

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from recipes import check_balance, balance_and_wait, nonstandard_check_balance, nonstandard_balance
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import get_skv_cluster_type, SKVClusterType


class SkvBalanceWorker(BaseWorker):
    def is_state_abnormal(self):
        if get_skv_cluster_type(self.module) == SKVClusterType.TWO_NODE:
            return not nonstandard_check_balance(self.module, self.logger)
        if get_skv_cluster_type(self.module) == SKVClusterType.GE_THREE_NODE:
            return not check_balance(self.module, self.logger)
        else:
            return False

    def diagnose(self):
        suggestion_msg = '{module} not balance, please execute [skvadmin balance start -m {module}].'.format(
            module=self.module
        )
        self.logger.warn(suggestion_msg)

    def repair(self):
        msg = '{module} not balance, execute balance operation and wait it finished'.format(module=self.module)
        self.logger.debug(msg)
        if get_skv_cluster_type(self.module) == SKVClusterType.TWO_NODE:
            nonstandard_balance(self.module, self.logger)
        else:
            balance_and_wait(self.module, self.logger)
