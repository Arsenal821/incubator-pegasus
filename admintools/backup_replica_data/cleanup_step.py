#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

最后清理 主要是rename checkpoint目录 便于后续自动删掉
"""
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, STATIC_REPLICA_SERVER_FILENAME

from hyperion_utils.shell_utils import check_call


class CleanupStep(BaseBackupStep):
    def do_update(self):
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return

        data_tag_to_dir = self.get_data_dir_map()
        static_replica_server_yml_file = os.path.join(self.backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)
        with open(static_replica_server_yml_file) as f:
            replica_server_statics = yaml.safe_load(f)

        for old_tag, data_dir in data_tag_to_dir.items():
            for gpid, checkpoint_info in replica_server_statics['old_tag_info'][old_tag]['gpid_to_checkpoint_info'].items():
                checkpoint_path = checkpoint_info['checkpoint_path']
                if os.path.isdir(checkpoint_path):
                    new_dir = checkpoint_path.replace('skv_backup_ckpt', 'skv_backup_checkpoint')
                    cmd = 'mv %s %s' % (checkpoint_path, new_dir)
                    check_call(cmd, self.logger.debug)
