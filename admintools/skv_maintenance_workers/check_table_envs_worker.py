#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wanghao002(wanghao002@sensorsdata.cn)
@brief

检查表的环境变量是否合理
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_admin_api import SkvAdminApi

CHECK_TABLE_UNEXPECT_ENVS = {'rocksdb.usage_scenario': 'bulk_load', 'replica.deny_client_write': 'true'}


class CheckTableEnvsWorker(BaseWorker):
    def _is_table_envs_normal(self, verbose):
        flag = True
        # 1. 获取所有的table
        api = SkvAdminApi(self.logger, self.module)
        table_list = api.get_all_avaliable_table_name()
        # 2. 依次检查表的环境变量是否合理.(现在主要是检查Usage Scenario功能，如果存并且为 bulk_load，而非 prefer_write 或者 normal，则打出 warn 信息)
        for table in table_list:
            envs = api.get_table_env(table)
            if 'app_envs' in envs.keys():
                for (k, v) in CHECK_TABLE_UNEXPECT_ENVS.items():
                    if k in envs['app_envs'] and v == envs['app_envs'][k]:
                        flag = False
                        if verbose:
                            self.logger.warn('table [%s] has env variables [%s: %s].' % (table, k, envs['app_envs'][k]))
        return flag

    def is_state_abnormal(self):
        return not self._is_table_envs_normal(verbose=False)

    def diagnose(self):
        self._is_table_envs_normal(verbose=True)
        self.logger.warn('Please contact OP or RD to check whether it is reasonable!!!')

    def repair(self):
        self._is_table_envs_normal(verbose=True)
        self.logger.warn('Please contact OP or RD to check whether it is reasonable!!!')
