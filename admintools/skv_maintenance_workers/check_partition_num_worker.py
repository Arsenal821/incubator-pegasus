#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

检查partition个数是否正确设置
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME
from skv_admin_api import SkvAdminApi
from recipes import get_skv_config_manager


class CheckPartitionNumWorker(BaseWorker):
    def _find_nearest_power_2(self, n):
        t = 8
        while n > t and t < 128:
            t *= 2
        return t

    def _config_calculate(self, verbose=False):
        '''计算partition_num应该是多少，如果verbose=True(diagons/repair阶段), 则输出计算过程 返回修改的表->partition_num'''
        api = SkvAdminApi(self.logger, self.module)

        def check_big_table(t):
            expect_partition_count = 0
            # 平均单个replica的大小不超过5g
            skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
            max_partition_size_mb = int(skv_config_manager._get_maintenace_config(
                'check_partition_num_worker',
                'partition_size_mb_bound',
                5 * 1024))
            if float(t['file_mb']) / int(t['pcount']) > max_partition_size_mb:
                if verbose:
                    self.logger.info('%s total size %sMB, partition count %d, avg size > %dMB' % (
                        t['app_name'], t['file_mb'], int(t['pcount']), max_partition_size_mb))
                expect_partition_count = float(t['file_mb']) / max_partition_size_mb
            # 平均单个replica的qps不超过5k
            qps_dict = {k: float(t[k]) for k in SkvAdminApi.APP_QPS_STAT_OP_COLUMN_LIST}
            qps = sum(qps_dict.values())
            max_partition_qps = 5000
            if qps / int(t['pcount']) > max_partition_qps:
                if verbose:
                    self.logger.info('%s total qps %d: %s' % (t['app_name'], qps, qps_dict))
                expect_partition_count = max(expect_partition_count, qps / max_partition_qps)
            # 取为2的指数
            if expect_partition_count:
                expect_partition_count = self._find_nearest_power_2(expect_partition_count)
                if verbose:
                    self.logger.warning('expect partition number: %d, please execute the following command to check split partition time' % expect_partition_count)
                    self.logger.warning('skvadmin table partition_split -t %s -m %s -p %d --dry_run' % (
                        t['app_name'], self.module, expect_partition_count))
                    self.logger.warning('afterwards, remove --dry_run to do actual work(this requires no read/write on skv and will takes a while!!!)')
                return True
            return False
        return api.get_big_capacity_table_list(big_filter=check_big_table)

    def is_state_abnormal(self):
        big_tables = self._config_calculate(verbose=False)
        if big_tables:
            self.logger.info('contains %d big tables: %s' % (len(big_tables), big_tables))
            return True
        return False

    def diagnose(self):
        self._config_calculate(verbose=True)

    def repair(self):
        self._config_calculate(verbose=True)
