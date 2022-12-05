#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

恢复单表
包括恢复前的表是否带时间戳
恢复后的是否带时间戳
"""
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep


class RecallTableStep(BasePartitionSplitStep):
    def __init__(self, from_table_type, to_use_time_prefix):
        self.from_table_type = from_table_type
        self.to_use_time_prefix = to_use_time_prefix

    def do_update(self):
        with open(self.static_yml_file) as f:
            static_data = yaml.safe_load(f)
        app_id = static_data['%s_app_id' % self.from_table_type]
        to_table_name = self.table_name
        if self.to_use_time_prefix:
            to_table_name += '_mark_deleted_%s' % self.time_str
        if to_table_name not in self.api.get_all_avaliable_table_name():  # 重试检查
            self.print_msg_to_screen('wait %d seconds for replica server to recall db...' % self.partition_count)
            self.api.recall_table(app_id, to_table_name)
            self.api.set_table_env(to_table_name, 'replica.deny_client_write', 'true')

    def do_rollback(self):
        table_name = self.table_name
        if self.to_use_time_prefix:
            table_name += '_mark_deleted_%s' % self.time_str
        if table_name in self.api.get_all_avaliable_table_name():
            self.api.drop_table(table_name)
