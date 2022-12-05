#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

1. 接着按照映射关系打包，只打checkpoint+xx-info

cd /sensorsdata/rnddata00/skv_offline/
tar rf /sensorsdata/main/packages/data0.tar replica/reps/4.0.pegasus/data/checkpoint.123
cd /sensorsdata/rnddata01/skv_offline/
tar rf /sensorsdata/main/packages/data0.tar replica/reps/4.1.pegasus/data/checkpoint.127

3. 更新统计信息，包括产生的每个tar的md5和本机的ip
"""
import datetime
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, STATIC_REPLICA_SERVER_FILENAME

from hyperion_utils.shell_utils import check_call, call


class TarReplicaDataStep(BaseBackupStep):

    def _tar_data_dirs(self, old_tag_info, md5_file):
        """打包
old_tag_info: old_tag -> {'size_mb': size_mb, 'checkpoint_path_to_size_mb': {path: size_mb...}}
        """
        data_tag_to_dir = self.get_data_dir_map()

        # 1. 打tar包操作可能很长 按理说应该支持不重复打包
        # 因此需要先解析哪些已经打完了
        #     2ffe074569a7447a6a633887dca4580f  ./1.1.tar
        finished_gpids = []
        if os.path.isfile(md5_file):
            with open(md5_file) as f:
                for line in f:
                    if line:
                        # ./1.1.tar
                        tar_file = line.strip().split()[-1].strip()
                        finished_gpids.append(tar_file[2:-4])

        # 2. 打tar包
        # 按照备份的机器的tag遍历 但是目标包的名字是恢复机器的tag名字
        total_checkpoint_cnts = sum([len(v['gpid_to_checkpoint_info']) for v in old_tag_info.values()], 0)
        i = 0  # 计数：第几个checkpoint了 打印进度用
        for old_tag, data_dir in data_tag_to_dir.items():
            for gpid, checkpoint_info in old_tag_info[old_tag]['gpid_to_checkpoint_info'].items():
                i += 1

                # 2.1 如果已经计算过md5了 则跳过
                if gpid in finished_gpids:
                    self.logger.debug('skip %s because md5 done' % gpid)
                    continue

                # 2.2 打包checkpoint 带重试
                self.print_msg_to_screen('[%d/%d]compressing %s... (approximately %sMB: %s)' % (
                    i, total_checkpoint_cnts, old_tag, checkpoint_info['size_mb'], checkpoint_info['checkpoint_path']))
                timeout = checkpoint_info['size_mb'] / 20 + 600  # 粗略预估 每秒最少写入20MB
                # 这里需要多次重试 因为如果在打包过程中发生compaction 会造成文件发生变动 导致tar退出 此时需要重试
                # 但是整体超时需要控制 避免其他类型的失败影响最终结果
                end_time = datetime.datetime.now() + datetime.timedelta(seconds=timeout)
                while timeout > 0:
                    # 删除老的tar
                    cmd = 'rm -f %s/%s.tar' % (self.backup_path_on_each_host, gpid)
                    check_call(cmd, self.logger.debug)
                    # 打checkpoint
                    cmd = 'cd %s && tar -c --hard-dereference -f %s/%s.tar %s' % (data_dir, self.backup_path_on_each_host, gpid, checkpoint_info['checkpoint_path'])
                    ret = call(cmd, self.logger.debug, timeout=timeout)
                    if ret == 0:
                        break
                    timeout = (end_time - datetime.datetime.now()).total_seconds()
                    self.logger.info('execute %s failed, try again, timeout %d' % (cmd, timeout))
                else:
                    raise Exception('failed to execute %s and timeout!' % cmd)

                # 2.3 计算md5
                timeout_sec = checkpoint_info['size_mb'] / 40 + 600  # 粗略预估 每秒读40MB
                cmd = 'cd %s && md5sum ./%s.tar >> %s' % (self.backup_path_on_each_host, gpid, md5_file)
                check_call(cmd, self.logger.debug, timeout=timeout_sec)

    def do_update(self):
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return
        # 1. 读取各种配置
        static_replica_server_yml_file = os.path.join(self.backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)
        with open(static_replica_server_yml_file) as f:
            replica_server_statics = yaml.safe_load(f)

        size_mb_to_gpids = {}
        for gpid_to_checkpoint_info in replica_server_statics['old_tag_info'].values():
            for name, detail in gpid_to_checkpoint_info.items():
                if name == 'size_mb':
                    continue
                for gpid, mesg in detail.items():
                    size_mb_to_gpids[gpid] = mesg['size_mb']

        # 2. 打包+md5
        md5_file = os.path.join(self.backup_path_on_each_host, 'md5')
        self._tar_data_dirs(replica_server_statics['old_tag_info'], md5_file)

        # 3. 读取解析并记录 md5
        # e.g. tar_mesg_to_gpids {'1.1': {'md5': 'xxxxx', 'size_mb': 1024}, '1.2': {'md5': 'qqqqq', 'size_mb': 1024}}
        tar_mesg_to_gpids = {}
        if os.path.isfile(md5_file):
            with open(md5_file) as f:
                for line in f:
                    if line:
                        # ./1.1.tar
                        tar_file = line.strip().split()[-1].strip()
                        gpid = tar_file[2:-4]
                        md5 = line.strip().split()[0].strip()
                        tar_mesg_to_gpids[gpid] = {'md5': md5, 'size_mb': size_mb_to_gpids[gpid]}

        replica_server_statics['tar_mesg_to_gpids'] = tar_mesg_to_gpids

        # 4. 写回统计信息
        with open(static_replica_server_yml_file, 'w+') as f:
            yaml.dump(replica_server_statics, f, default_flow_style=False)
        self.logger.info('updated %s' % static_replica_server_yml_file)
