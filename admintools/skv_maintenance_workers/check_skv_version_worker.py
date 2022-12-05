#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

检查skv版本是否有问题
暂时占位 2.0 还没有不稳定版本
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_admin_api import SkvAdminApi
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import get_skv_cluster_type, SKVClusterType

# 这里只记录白名单
GOOD_VERSION_LIST = ['2.0.0']


class CheckSkvVersionWorker(BaseWorker):
    def is_backend_stable(self, verbose):
        """返回是否是可靠版本)"""
        # 结果类似 2.0.0 (5658fbe0bbb468613b75282b932958ea343b4888)
        skv_version = SkvAdminApi(self.logger, self.module).get_version()
        full_version = skv_version.split('(')[0].strip()
        if verbose:
            self.logger.info('current version %s, full version %s' % (skv_version, full_version))
        # 对应三位版本不稳定
        if full_version not in GOOD_VERSION_LIST:
            if verbose:
                self.logger.error('bad sensorsdata_version! stables are %s' % GOOD_VERSION_LIST)
                expect_version = GOOD_VERSION_LIST[-1]
                # 集群版 推荐热升级
                if get_skv_cluster_type(self.module) == SKVClusterType.GE_THREE_NODE:
                    self.logger.info('please upgrade to %s by [skvadmin upgrader -m %s -t hot -v %s], this command will rolling restart host by host.' % (
                        expect_version, self.module, expect_version))
                else:
                    self.logger.info('please contact csm for an upgrade, this operation requires to stop read/write to skv!')
                    self.logger.info('please upgrade to %s by [skvadmin upgrader -m %s -t cold -v %s]' % (
                        expect_version, self.module, expect_version))
            return False
        # 检查 server 端版本是否与 java client 的配置一致
        # 白名单中的 server 对应到 client conf 中的 major version 只需 >= 2.0
        from recipes.platform_adapter import _get_client_conf
        major_version = _get_client_conf(self.module, 'major_version')
        if major_version is None:
            if verbose:
                self.logger.error('missing major_version info in skv client conf, please contact skv RD for confirmation!')
            return False
        major_version_num = int(major_version.split('.')[0])
        if major_version_num < 2:
            if verbose:
                self.logger.error('skv server version: %s, client conf major_version: %s' % (full_version, major_version))
                self.logger.error('client conf major_version for skv2.0 and above version should be >= 2.0, please contact skv RD for confirmation!')
            return False
        return True

    def is_state_abnormal(self):
        return not self.is_backend_stable(verbose=False)

    def diagnose(self):
        self.is_backend_stable(verbose=True)

    def repair(self):
        self.is_backend_stable(verbose=True)
