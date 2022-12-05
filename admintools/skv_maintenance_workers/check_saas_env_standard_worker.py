#!/bin/env python
# -*- coding: UTF-8 -*-

"""
# 1. 检查 saas 环境磁盘数是否合理 > 3+1
#    主要是考虑将一个较小磁盘存放 slog,其余磁盘存放数据
# 2. 检查 saas 分片数是否合理。单节点最大分片数: skv_offline <= 500, skv_offline <= 1000
#    saas 环境 skv_offline 的 global write buffer 默认为 16G，rocksdb_write_buffer 为 80M, 假设某一时刻最大有 40% 的分片同时存在写操作
#    那么就可以大概计算最多分片数： 16 * 1024 / 80 / 0.4 ～= 500   ==> 所以约定 skv_offline 最大分片数为500；
#    同样的，saas 环境 skv_online 的 global write buffer 默认为 8G，rocksdb_write_buffer 为 20M, 假设某一时刻最大有 40% 的分片同时存在写操作
#    那么就可以大概计算最多分片数： 8 * 1024 / 20 / 0.4 ～= 1000   ==> 所以约定 skv_online 最大分片数为1000；
"""
import os
import sys
import socket

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_admin_api import SkvAdminApi
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME, is_hubble_installed
from recipes import get_skv_config_manager


SECTION_NAME = 'replication'
DATA_DIR_CONFIG_NAME = 'data_dirs'
SLOG_DIR_CONFIG_NAME = 'slog_dir'
SAAS_SKV_MIN_DATA_DIRS_NUM = 3
SAAS_SKV_MAX_PARTITION_NUM = {
    'skv_offline': 500,
    'skv_online': 1000
}


class CheckSaasEnvStandardWorker(BaseWorker):
    def is_saas_env_standard(self, verbose):
        flag = True
        if not is_hubble_installed():
            self.logger.info('not need check in non SaaS env')
            return True

        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        groups = skv_config_manager.get_config_groups()
        for group in groups:
            # 分别获取 data_dirs 和 slog_dir
            data_dirs = skv_config_manager.get_final_config_value(SECTION_NAME, DATA_DIR_CONFIG_NAME, group)
            slog_dir = skv_config_manager.get_final_config_value(SECTION_NAME, SLOG_DIR_CONFIG_NAME, group)

            # 1.磁盘检查: 检查 len(data_dirs) 是否 >3;检查 slog 是否为独立的磁盘
            dd = [d.split(':')[1] for d in data_dirs.split(',')]
            if len(dd) < SAAS_SKV_MIN_DATA_DIRS_NUM:
                flag = False
                if verbose:
                    self.logger.warn('group [%s] disk num is [%d], in saas standard env, the number of data_dirs shall not be less than %s' % (group, len(dd), SAAS_SKV_MIN_DATA_DIRS_NUM))
            if slog_dir in dd:
                flag = False
                if verbose:
                    self.logger.warn('[slog_dir:%s, data_dirs:%s], %s slog_dir not independent disk space!' % (slog_dir, data_dirs, group))

        # 2.检查每个节点上的分片数
        for server_endpoint, node_info in SkvAdminApi(self.logger, self.module)._get_nodes_details()['details'].items():
            host = socket.getfqdn(server_endpoint.split(':')[0])
            partition_num = int(node_info['replica_count'])
            if partition_num > SAAS_SKV_MAX_PARTITION_NUM[self.module]:
                flag = False
                if verbose:
                    self.logger.warn('host[%s] partition num is [%d], saas %s standard partition num not less than %d' % (host, partition_num, self.module, SAAS_SKV_MAX_PARTITION_NUM[self.module]))
        return flag

    def is_state_abnormal(self):
        return not self.is_saas_env_standard(verbose=False)

    def diagnose(self):
        self.is_saas_env_standard(verbose=True)

    def repair(self):
        self.is_saas_env_standard(verbose=True)
