#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


检查表是否有读写流
统计条数
"""
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep


class PreCheckStep(BasePartitionSplitStep):
    def do_update(self):
        # 检查是否有读写流，如果没有则设置老表不可写
        if self.api.check_table_has_ops(self.table_name):
            raise Exception('table %s still has write/read operations!' % self.table_name)
        self.api.set_table_env(self.table_name, 'replica.deny_client_write', 'true')

        # 统计条数
        self.print_msg_to_screen('counting table..')
        table_count_num = self.api.count_table(self.table_name)

        # 记录app_id
        app_id = self.api.get_app_id_by_table(self.table_name)

        # 写入文件
        with open(self.static_yml_file, 'w+') as f:
            yaml.dump({'table_count_num': table_count_num, 'old_app_id': app_id}, f)

    def do_rollback(self):
        # 设置老表可写
        self.api.set_table_env(self.table_name, 'replica.deny_client_write', 'false')
