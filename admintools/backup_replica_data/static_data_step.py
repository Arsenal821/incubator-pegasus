#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

创建测试表(sp_skv_backup_mark)，用于后续验证完整性，包含以下kv

测试kv(spanich->lychee)
此外在临时目录下记录了
replica server的addr列表
all_tables: json格式的ls -j结果

不需要条数 因为是做了checkpoint
"""
import datetime
import os
import time
import sys
import yaml

from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, STATIC_CLUSTER_FILENAME, MARK_TABLE_PREFIX
from skv_admin_api import SkvAdminApi


class StaticDataStep(BaseBackupStep):

    def do_update(self):
        # 1. 创建一个测试表
        api = SkvAdminApi(self.logger, self.module_name)
        replica_server_list = self.get_replica_server_addrs()
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        mark_table_name = '%s_%s' % (MARK_TABLE_PREFIX, timestamp)
        replica_server_count = len(replica_server_list)
        self.print_msg_to_screen('create & write %s..' % mark_table_name)
        # partition个数简单设置几个档位 最高64分片 尽可能保证每个机器上都有分片
        partition_num = 4
        while partition_num <= 64 and partition_num < replica_server_count * 3:
            partition_num <<= 1

        replica_num = replica_server_count if replica_server_count < 3 else 3
        api.create_table(mark_table_name, partition_num, replica_num)

        # 2. 产生partition_num * 10对kv 尽量保证每台机器上都有数据
        table_data = [['spanich_%d' % i, str(i), 'lychee_%d' % i] for i in range(partition_num * 10)]
        for hash_key, sort_key, value in table_data:
            api.set_kv(mark_table_name, hash_key, sort_key, value)
        time.sleep(1)   # 等待写入
        table_lines_num = api.count_table(mark_table_name)
        if table_lines_num != len(table_data):
            raise Exception('failed to write mark_table! write %d lines but only %d lines in table %s!' % (
                table_lines_num, len(table_data), mark_table_name))
        self.logger.info('wrote %d lines to %s: %s' % (table_lines_num, mark_table_name, table_data))

        # 3. 计算相关统计信息
        static_data = {
            'mark_table': mark_table_name,
            'mark_table_data': table_data,
        }
        # 3.1 新集群的 replica_server list
        if self.new_replica_server_list:
            static_data['replica_server_list'] = self.new_replica_server_list.split(',')
        else:
            static_data['replica_server_list'] = replica_server_list

        # 3.2 统计所有表名
        static_data['all_tables'] = api.get_all_avaliable_table_name()

        # 3.3 统计版本
        major_version = api.get_version().split()[0]
        static_data['version'] = major_version

        # 3.4 统计表分片数
        static_data['table_to_partition_count'] = api.get_all_table_to_partition_count()

        # 3.5 统计表副本数
        static_data['table_to_replica_count'] = api.get_all_table_to_replica_count()

        # 3.6 统计当前 primary replica 分布
        primary_replica_to_server = {server: [] for server in api.get_all_replica_server()}
        for table in api.get_all_avaliable_table_name():
            for gpid, server in api.get_table_primary_map(table).items():
                primary_replica_to_server[server].append(gpid)
        static_data['primary_replica_to_server'] = primary_replica_to_server

        # 4. 写入临时目录
        yml_file = os.path.join(self.get_stepworker_work_dir(), STATIC_CLUSTER_FILENAME)
        with open(yml_file, 'w+') as f:
            yaml.dump(static_data, f, default_flow_style=False)
        self.logger.info('wrote static data to %s' % yml_file)

        # 5. 拷贝文件
        for host in self.get_replica_server_hosts():
            connector = SSHConnector.get_instance(host)
            # 创建目录
            connector.check_call('mkdir -p %s' % self.backup_path_on_each_host, self.logger.debug)
            # 拷贝统计文件
            connector.copy_from_local(yml_file, os.path.join(self.backup_path_on_each_host, STATIC_CLUSTER_FILENAME), self.logger.debug)
