#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

删表
包括删除带时间戳/不带时间戳的表
通过构造函数指定
"""
import os
import sys
import time
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep


class DropTableStep(BasePartitionSplitStep):
    def __init__(self, from_table_type="old", use_time_prefix=False):
        self.use_time_prefix = use_time_prefix
        self.from_table_type = from_table_type

    def do_update(self):
        # 删旧表 use_time_prefix = False； 删新表 use_time_prefix = True
        table_name = self.table_name
        if self.use_time_prefix:
            table_name += '_%s' % self.time_str
        if table_name in self.api.get_all_avaliable_table_name():  # 重试检查
            # drop table会立刻返回 而实际上replica server并不一定真的关闭了所有实例 如果立刻recall会有问题
            # 因此需要人肉sleep一下 sleep时长与分片相关
            partition_count = self.api.get_table_partition_count(table_name)
            self.api.drop_table(table_name)
            self.print_msg_to_screen('wait %d seconds for replica server to drop db...' % partition_count)
            time.sleep(partition_count)

    def do_rollback(self):
        table_name = self.table_name
        if self.use_time_prefix:
            table_name += '_%s' % self.time_str
        if table_name not in self.api.get_all_avaliable_table_name():
            with open(self.static_yml_file) as f:
                static_data = yaml.safe_load(f)
            self.print_msg_to_screen('wait %d seconds for replica server to recall db...' % self.partition_count)
            self.api.recall_table(static_data['%s_app_id' % self.from_table_type], table_name)
            self.api.set_table_env(table_name, 'replica.deny_client_write', 'true')
