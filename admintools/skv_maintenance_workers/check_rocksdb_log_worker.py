#!/bin/env python
# -*- coding: UTF-8 -*-

"""
# 检查 rocksdb 的 LOG 日志
# 这里主要是检查 rocksdb 日志中是否有 write buffer 不足的日志信息
# 通过遍历检查每台机器上每个表的一个分片的 LOG 日志，当存在相关日志信息时表明当前机器分配给 skv 的内存资源不足，可能导致异常
"""
import os
import sys
import random
import datetime

import utils.sa_utils

from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import SKV_PRODUCT_NAME, SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager


SECTION_NAME = 'replication'
ROLE_NAME = 'replica_server'
PRINT_ROCKSDB_LOG_MAX_LINES = 5
MATCH_ROCKSDB_LOG_STR = 'Flushing column family with largest mem table size. Write buffer'


class CheckRocksdbLogWorker(BaseWorker):
    def is_rocksdb_log_normal(self, verbose):
        flag = True
        # 检查rocksdb日志(write buffer)
        # 主要是判断是否有"Flushing column family with largest mem table size. Write buffer is using 262705152 bytes out of a total of 67108864."类似字段
        # 1. 遍历所有机器
        # 2. 一个表只检查一个分片
        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        groups = skv_config_manager.get_config_groups()

        msg = str()
        for group in groups:
            hosts = skv_config_manager.get_config_group_hosts(group)
            data_dirs = skv_config_manager.get_final_config_value('replication', 'data_dirs', group)
            for host in hosts:
                # 每台机器上一个表只检查一个分片，表write buffer是公用的，通常一个表的一个分片存在 write buffer 不足，那么其他的分片也会存在 write buffer 不足
                # 每台机器上使用一个 black list 来记录以及检查的表
                check_black_list = []
                connector = SSHConnector.get_instance(host)
                for dir in data_dirs.split(','):
                    path = os.path.join(dir.split(':')[1], 'replica', 'reps')
                    cmd = 'ls -l %s' % path
                    pegasus_files = connector.check_output(cmd, self.logger.debug).splitlines()[1:]
                    # 这里打乱一下顺序，避免每次检查时都检查一个分片
                    random.shuffle(pegasus_files)
                    for pegasus_file in pegasus_files:
                        file = pegasus_file.split()[-1]
                        # 获取 app_id
                        table_id = file.split('.')[0]
                        if table_id in check_black_list:
                            continue
                        check_black_list.append(table_id)
                        # grep 'Flushing column family with largest mem table size. Write buffer' /sensorsdata/rnddata00/skv_offline/replica/reps/13.0.pegasus/data/rdb/LOG | tail -5
                        cmd = "grep '{}' {}/{}/data/rdb/LOG | tail -{}".format(MATCH_ROCKSDB_LOG_STR, path, file, PRINT_ROCKSDB_LOG_MAX_LINES)
                        ret = connector.check_output(cmd, self.logger.debug)
                        if ret:
                            flag = False
                            new_msg = '------{host} {path}/{file}------\nexec command:\n{exec_command}\noutput:\n{ret}\n\n'.format(
                                host=host,
                                path=path,
                                file=file,
                                exec_command=cmd,
                                ret=ret
                            )
                            msg = msg + new_msg
                            break
        if not flag and verbose:
            # 将每次检查的结果保存在一个目录中, /sensorsdata/main/runtime/skv/skv_offline/rocksdb_error_message/
            log_dir = os.path.join(
                utils.sa_utils.get_default_runtime_dir(SKV_PRODUCT_NAME),
                '{skv_offline}',
                'rocksdb_log_error_message'
            ).format(skv_offline=self.module)
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, 'rocksdb_{timestamp}.log'.format(timestamp=datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
            with open(log_file, 'w') as f:
                f.write(msg)
            self.logger.warn('rocksdb log with insufficient write buffer, please check log [%s] ' % log_file)

        return flag

    def is_state_abnormal(self):
        return not self.is_rocksdb_log_normal(verbose=False)

    def diagnose(self):
        self.is_rocksdb_log_normal(verbose=True)

    def repair(self):
        self.is_rocksdb_log_normal(verbose=True)
