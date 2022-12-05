#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

1. 记录当前时间
2. 创建checkpoint
3. 不断检查checkpoint是否创建成功
4. 检查磁盘空间够不够
5. 写入统计文件
"""
import datetime
import os
import re
import sys
import time
import traceback
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, STATIC_REPLICA_SERVER_FILENAME, STATIC_CLUSTER_FILENAME
from skv_admin_api import SkvAdminApi
from recipes import get_skv_config_manager
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME

from hyperion_utils.shell_utils import check_output, run_cmd, check_call


class MakeSnapshotStep(BaseBackupStep):

    def get_checkpoint_path_after_time(self, gpid, after_time, data_dir_paths):
        """返回某个表(gpid)在某个时间(after_time)以后产生的checkpoint, None表示没有找到;
data_dir_paths: 所有的data路径列表"""
        for data_dir in data_dir_paths:
            path_pattern = os.path.join(data_dir, 'replica/reps/%s.pegasus/data/checkpoint.*' % gpid)
            cmd = 'ls -1d %s' % path_pattern
            ret = run_cmd(cmd, self.logger.debug)
            if ret['ret'] != 0:
                continue
            for checkpoint_path in ret['stdout'].splitlines():
                # checkpoint.tmp.1639376331980611.tmp 可能会出现这种临时目录
                basename = os.path.basename(checkpoint_path)
                if not re.match(r'checkpoint\.([0-9]+)', basename):
                    self.logger.debug('ignore file %s' % checkpoint_path)
                    continue
                try:
                    # >>> os.stat(os.path.join(path, 'checkpoint.1025296348'))
                    # os.stat_result(st_mode=16877, st_ino=537640765, st_dev=64832, st_nlink=2, st_uid=1000, st_gid=1000, st_size=4096, st_atime=1638942636, st_mtime=1638942636, st_ctime=1638942636)
                    mtime = datetime.datetime.fromtimestamp(os.stat(checkpoint_path).st_mtime)
                    if mtime >= after_time:
                        self.logger.debug('checkpoint created: %s mtime is %s >= %s!' % (checkpoint_path, mtime, after_time))
                        # 需要把这个checkpoint给rename了 避免后续被gc掉 注意rename之后文件名不能包含checkpoint 否则会直接被删了
                        dirname = os.path.dirname(checkpoint_path)
                        new_path = os.path.join(dirname, 'skv_backup_ckpt.%s' % mtime.strftime('%Y%m%d_%H%M%S'))
                        os.makedirs(new_path)

                        # 创建硬链接
                        for file in os.listdir(checkpoint_path):
                            os.link(os.path.join(checkpoint_path, file), os.path.join(new_path, file))

                        # 然后需要把这几个隐藏文件拷贝到包里面
                        for filename in ['.app-info', '.init-info']:
                            cmd = 'cp %s/%s %s' % (os.path.dirname(dirname), filename, new_path)
                            check_call(cmd, self.logger.debug)
                        return new_path
                    else:
                        self.logger.debug('ignore old checkpoint %s: mtime is %s < %s' % (checkpoint_path, mtime, after_time))
                except Exception:
                    self.logger.debug('ignore exception while trying to stat %s' % checkpoint_path)
                    self.logger.debug(traceback.format_exc())
        return None

    def trigger_all_replica_checkpoint_and_check(self, data_dir_paths):
        """触发checkpoint并且检查是否完成 返回一个dict gpid->checkpoint_path"""
        # 1. 记录当前时间
        now = datetime.datetime.now()
        api = SkvAdminApi(self.logger, self.module_name)

        # 3. 获取本机的 primary replica
        port = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_default_port()
        my_addr = '%s:%d' % (self.my_ip, port)
        yml_file = os.path.join(self.backup_path_on_each_host, STATIC_CLUSTER_FILENAME)
        if not os.path.isfile(yml_file):
            raise Exception('%s not existed! The program is not complete.')
        with open(yml_file) as f:
            local_primary_replica = yaml.safe_load(f)['primary_replica_to_server'][my_addr]
        total_replica_count = len(local_primary_replica)
        if total_replica_count == 0:
            raise Exception('cannot find replica in replica server!')

        # 3. 创建checkpoint
        api.trigger_checkpoint(my_addr, ','.join(local_primary_replica))

        # 4. 不断检查
        gpid_to_checkpoint_path = {}
        count_down = 600  # 重试600次 相当于10分钟
        while local_primary_replica:
            # 循环等待
            self.print_msg_to_screen('checking %d/%d replica checkpoint..' % (total_replica_count - len(local_primary_replica), total_replica_count))
            count_down -= 1
            if count_down == 0:
                # 超时
                raise Exception('failed to check replica, still %d/%d not done!: %s' % (
                    len(local_primary_replica), total_replica_count, local_primary_replica))
            elif count_down >= 30 and count_down % 10 == 0:
                # 这玩意不靠谱 等半分钟然后每10秒再触发一次 这个命令本身要跑好一会 就不sleep了
                api.trigger_checkpoint(my_addr, ','.join(local_primary_replica))
            else:
                # 否则等待
                time.sleep(1)

            for i, gpid in enumerate(local_primary_replica):
                path = self.get_checkpoint_path_after_time(gpid, now, data_dir_paths)
                if path:
                    gpid_to_checkpoint_path[gpid] = path
                    local_primary_replica.pop(i)
                    self.logger.info('%s checkpoint %s is created' % (gpid, path))
            self.logger.debug('still %d undone:\n%s' % (len(local_primary_replica), local_primary_replica))
        return gpid_to_checkpoint_path

    def do_update(self):
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return

        # 1. 触发checkpoint
        data_tag_to_dir = self.get_data_dir_map()
        gpid_to_checkpoint_path = self.trigger_all_replica_checkpoint_and_check(list(data_tag_to_dir.values()))

        # 2. 统计数据目录总大小 注意这个信息后面打包超时也有用 因此会记录到本地目录下
        # data0 -> 13452
        data_tag_info = {data_tag: {'size_mb': 0, 'gpid_to_checkpoint_info': {}} for data_tag in data_tag_to_dir}
        for gpid, checkpoint_path in gpid_to_checkpoint_path.items():
            # 找到对应的data_tag
            for data_tag, data_dir in data_tag_to_dir.items():
                if checkpoint_path.startswith(data_dir):
                    break
            else:
                raise Exception('cannot find data_tag for %s!' % checkpoint_path)
            # 累加占用大小
            cmd = 'du -smc %s' % checkpoint_path
            output = check_output(cmd, self.logger.debug, 1800)
            mb = int(output.splitlines()[-1].split()[0])
            data_tag_info[data_tag]['gpid_to_checkpoint_info'][gpid] = {
                'checkpoint_path': os.path.relpath(checkpoint_path, data_dir),  # 打包用的相对路径
                'size_mb': mb}
            data_tag_info[data_tag]['size_mb'] += mb
            self.logger.debug('%s[%sMB] add %s[%dMB]' % (data_tag, data_tag_info[data_tag]['size_mb'], checkpoint_path, mb))

        # 3. 检查目标目录的80%的剩余空间mb
        cmd = 'df -m %s' % self.backup_path_on_each_host
        output = check_output(cmd, self.logger.debug)
        fields = ' '.join(output.splitlines()[1:]).split()
        all_mb, used_mb = int(fields[1]), int(fields[2])
        ava_mb = all_mb - used_mb
        self.logger.info('%s %dMB/%dMB used, ava %dMB' % (self.backup_path_on_each_host, used_mb, all_mb, ava_mb))
        total_mb = sum([v['size_mb'] for v in data_tag_info.values()], 0)
        if total_mb > ava_mb * 0.8:
            raise Exception('not enough space for %s! replica data total %dMB, current used %dMB/%dMB!' % (
                self.backup_path_on_each_host, total_mb, used_mb, all_mb))

        # 4. 写入统计信息
        yml_file = os.path.join(self.backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)
        with open(yml_file, 'w+') as f:
            yaml.dump({'old_tag_info': data_tag_info}, f, default_flow_style=False)
        self.logger.info('wrote static data to %s' % yml_file)
