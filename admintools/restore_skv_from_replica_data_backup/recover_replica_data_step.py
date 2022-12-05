#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

还原数据目录
"""
import datetime
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep, GPIDS_DATA_ASSIGNATION_FILENAME

from hyperion_utils.shell_utils import check_call


class RecoverReplicaDataStep(BaseRestoreStep):
    def do_update(self):
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return

        # 1 读取本机的数据信息
        yml_file = os.path.join(self.restore_from_backup_path_on_each_host, GPIDS_DATA_ASSIGNATION_FILENAME)
        with open(yml_file) as f:
            tag_to_gpids = yaml.safe_load(f)

        data_tag_to_dir = self.get_data_dir_map()
        total_cnt = sum([len(v) for v in tag_to_gpids.values()])
        i = 0
        for data_tag, data_dir in data_tag_to_dir.items():
            # 2. 备份当前的目录
            timestamp_postfix = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            cmd = 'mv {data_dir} {data_dir}_{postfix} && mkdir -p {data_dir}'.format(data_dir=data_dir, postfix=timestamp_postfix)
            check_call(cmd, self.logger.debug)

            for gpid in tag_to_gpids[data_tag]:
                i += 1
                local_tar = os.path.join(self.restore_from_backup_path_on_each_host, '%s.tar' % gpid)
                size_mb = int(os.path.getsize(local_tar) / (1024 * 1024))
                self.print_msg_to_screen('[%d/%d] decompressing %s... (approximately %sMB: %s)' % (
                    i, total_cnt, gpid, size_mb, data_tag))

                # 3. 解压
                cmd = 'cd %s && tar xf %s' % (data_dir, local_tar)
                timeout_sec = size_mb / 20 + 600  # 每秒至少写入20MB
                check_call(cmd, self.logger.debug, timeout_sec)

                # 4. 把每个checkpoint mv成rdb
                # /sensorsdata/rnddata00/skv_offline/replica/reps/1.6.pegasus/data
                root_dir = os.path.join(data_dir, 'replica/reps', '%s.pegasus' % gpid, 'data')
                checkpoint_dirs = os.listdir(root_dir)
                if len(checkpoint_dirs) != 1:
                    raise Exception('invalid path %s! subdirs %s!' % (root_dir, checkpoint_dirs))
                cmd = 'mv %s/%s %s/rdb' % (root_dir, checkpoint_dirs[0], root_dir)
                check_call(cmd, self.logger.debug)

                # 5. 把包里面的info文件挪到外面
                for filename in ['.app-info', '.init-info']:
                    cmd = 'mv %s/rdb/%s %s' % (root_dir, filename, os.path.dirname(root_dir))
                    check_call(cmd, self.logger.debug)
