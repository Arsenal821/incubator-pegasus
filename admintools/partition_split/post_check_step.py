#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


最后检查条数
"""
import os
import sys
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep


class PostCheckStep(BasePartitionSplitStep):
    def do_update(self):
        self.print_msg_to_screen('counting new table..')
        table_count_num = self.api.count_table(self.table_name)
        with open(self.static_yml_file) as f:
            static_data = yaml.safe_load(f)
        if table_count_num != static_data['table_count_num']:
            raise Exception('check failed! table count umatch! before %s->after %s!' % (static_data['table_count_num'], table_count_num))
        self.api.set_table_env(self.table_name, 'replica.deny_client_write', 'false')

    def do_rollback(self):
        self.api.set_table_env(self.table_name, 'replica.deny_client_write', 'true')
