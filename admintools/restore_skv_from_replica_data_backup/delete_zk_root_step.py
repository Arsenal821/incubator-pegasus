#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

删除cluster_root
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_restore_step import BaseRestoreStep


from hyperion_guidance.zk_connector import ZKConnector


class DeleteZkRootStep(BaseRestoreStep):
    def do_update(self):
        cluster_root = self.get_cluster_root()
        # 防御性检查：必须包含模块名 避免悲剧发生
        assert(self.module_name in cluster_root and 'skv' in cluster_root)
        self.logger.info('delete %s...' % cluster_root)
        zk = ZKConnector()
        zk.delete_by_path(cluster_root, recursive=True, ensure=False)
