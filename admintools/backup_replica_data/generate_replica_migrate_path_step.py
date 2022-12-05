#!/bin/env python
# -*- coding: UTF-8 -*-
"""
根据 CalculateNewReplicaMapStep 计算出的路径文化生成 scp 脚本
"""

import os
import socket
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, REPLICA_MIGRATE_WAY_FILENAME

from hyperion_utils.shell_utils import check_call


class GenerateReplicaMigratePathStep(BaseBackupStep):
    def do_update(self):
        # 本地备份无需此步骤操作
        if not self.new_replica_server_list:
            self.print_msg_to_screen('skip because not need migrate')
            return
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return
        yml_file = os.path.join(self.backup_path_on_each_host, REPLICA_MIGRATE_WAY_FILENAME)

        if not os.path.isfile(yml_file):
            raise Exception('%s not existed! The program is not complete.')

        with open(yml_file) as f:
            local_replica_migrate_way = yaml.safe_load(f)[socket.gethostbyname(socket.getfqdn())]

        scp_data_script = os.path.join(self.backup_path_on_each_host, 'scp_data.sh')
        with open(scp_data_script, 'w+') as f:
            f.write('''#!/bin/sh
set -ex

''')
        for gpid, dest_host in local_replica_migrate_way.items():
            cmd = 'scp {local_file} sa_cluster@{ip}:{remote_dir}'.format(
                local_file=os.path.join(self.backup_path_on_each_host, '%s.tar' % gpid),
                ip=dest_host,
                remote_dir=self.remote_migration_dir,
            )
            check_call('echo "{cmd}" >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)
