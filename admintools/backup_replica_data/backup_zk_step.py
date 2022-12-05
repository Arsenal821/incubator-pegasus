#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

备份当前zk节点到一个json文件中去
"""
import json
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep

from hyperion_guidance.zk_connector import ZKConnector
from hyperion_guidance.ssh_connector import SSHConnector


class BackupZkStep(BaseBackupStep):
    def save_zk_tree(self, zk, root_path):
        data = zk.get_value_by_path(root_path)
        self.logger.info('save %s' % root_path)
        self.logger.debug(data)
        result = {'data': data, 'subs': {}}
        sub_list = zk.get_children_by_path(root_path)
        if not sub_list:
            return result
        for sub_path in sub_list:
            abs_sub_path = os.path.join(root_path, sub_path)
            sub_result = self.save_zk_tree(zk, abs_sub_path)
            if sub_result:
                result['subs'][sub_path] = sub_result
        return result

    def do_update(self):
        filename = 'zk_backup.json'
        cluster_root = self.get_cluster_root()
        zk = ZKConnector()
        data = self.save_zk_tree(zk, cluster_root)
        backup_file = os.path.join(self.get_stepworker_work_dir(), filename)
        with open(backup_file, 'w+') as f:
            f.write(json.dumps(data, indent=4))
        self.logger.info('backup zk(%s) to %s' % (cluster_root, backup_file))
        # 拷贝备份文件
        for host in self.get_replica_server_hosts():
            connector = SSHConnector.get_instance(host)
            connector.copy_from_local(backup_file, os.path.join(self.backup_path_on_each_host, filename), self.logger.debug)
