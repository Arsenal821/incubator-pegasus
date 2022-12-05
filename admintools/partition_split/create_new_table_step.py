#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


# 建新表，名字带时间后缀
"""
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep


class CreateNewTableStep(BasePartitionSplitStep):
    def do_update(self):
        # 建表
        table_name = '%s_%s' % (self.table_name, self.time_str)
        replica_count = min(self.api.get_replica_server_num(), 3)
        if table_name not in self.api.get_all_avaliable_table_name():  # 重试检查
            self.api.create_table(table_name, self.partition_count, replica_count)

        # 记录app_id
        app_id = self.api.get_app_id_by_table(table_name)

        # 写入文件
        with open(self.static_yml_file) as f:
            static_data = yaml.safe_load(f)
        static_data['new_app_id'] = app_id
        with open(self.static_yml_file, 'w+') as f:
            yaml.dump(static_data, f)

    def do_rollback(self):
        table_name = '%s_%s' % (self.table_name, self.time_str)
        # 删除新表
        if table_name in self.api.get_all_avaliable_table_name():
            self.api.drop_table(table_name)
