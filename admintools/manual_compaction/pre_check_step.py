#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys

from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_machine_system_api import get_disk_info_from_host_by_dirs
from manual_compaction.base_manual_compaction_step import BaseManualCompactionStep
from recipes import get_skv_config_manager
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME

COMPACTION_NEED_ADD_DISK_THRESHOLD = 0.8


class PreCheckStep(BaseManualCompactionStep):
    def do_update(self):
        msg = self._check_disk_used()
        if msg:
            raise Exception(msg + 'manual compaction will take fill of the disk, please check log and add disks before performing this operation!!!')

    def _check_disk_used(self):
        # 1.遍历所有节点 2. 遍历所有磁盘. 3 检查所有磁盘上的表是否可以继续manual compaction.
        # 从配置信息中获取data_dirs
        skv_config_manager = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        groups = skv_config_manager.get_config_groups()
        msg = ''
        for group in groups:
            data_dirs = skv_config_manager.get_final_config_value('replication', 'data_dirs', group)
            for host in skv_config_manager.get_config_group_hosts(group):
                disks_info = get_disk_info_from_host_by_dirs(host, data_dirs, self.logger)
                for (data_dir, disk_info) in disks_info.items():
                    table_size = self._get_table_size_by_data_dir(host, data_dir.split(":")[1])
                    take_up_pct = round((disk_info['Used'] + table_size) / disk_info['Size'], 4)
                    if take_up_pct > COMPACTION_NEED_ADD_DISK_THRESHOLD:
                        msg = msg + "The free size of disk [{disk}] on host [{host}] is {size}M, manual compaction need extra {extra}M disk space.disk usage will exceed 80%\n".format(
                            disk=disk_info['Filesystem'], host=host, size=disk_info['Available'], extra=table_size)
        return msg

    def _get_table_size_by_data_dir(self, host, dir):
        # 获取一个replica上指定磁盘上的占磁盘空间最大的表和占用空间(mb)
        connector = SSHConnector.get_instance(host)
        table_id = self.api.get_table_id_by_name(self.table_name)
        # 先检查有没有这个rdb 很多老客户是一坨机器hdd 但是只有8分片 这种情况下很容易出现某个机器上没有rdb
        cmd = 'ls -1d {}/replica/reps/{}.*.pegasus'.format(dir, str(table_id))
        ret = connector.call(cmd, self.logger.debug)
        if ret != 0:
            self.logger.info('ignore table size on %s' % host)
            return 0
        # du -scm /sensorsdata/rnddata00/skv_offline/replica/reps/1.*.pegasus/data/rdb/
        cmd = 'du -scm {}/replica/reps/{}.*.pegasus/data/rdb/'.format(dir, str(table_id))
        cmd_result = connector.check_output(cmd, self.logger.debug)
        extra_replica_mb = cmd_result.splitlines()[-1].split()[0]
        return int(extra_replica_mb)

    def check(self):
        return True

    def rollback(self):
        pass

    def backup(self):
        pass
