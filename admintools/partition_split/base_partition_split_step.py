#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import socket
import os

from stepworker.base_step import BaseStep

from skv_common import get_context_details
from skv_admin_api import SkvAdminApi


class BasePartitionSplitStep(BaseStep):

    def do_init(self):
        details = get_context_details()

        # 当前主机
        self.my_host = socket.getfqdn()
        self.my_ip = socket.gethostbyname(self.my_host)

        # 模块名
        self.module_name = details['module_name']
        # 表名
        self.table_name = details['table']
        # 分片数
        self.partition_count = details['partition_count']
        # 时间戳
        self.time_str = details['time_str']
        # multi_set
        self.use_multi_set = details['multi_set']

        # 统计条数的信息 会记录到这里
        self.static_yml_file = os.path.join(self.get_stepworker_work_dir(), 'static.yml')

        self.api = SkvAdminApi(self.logger, self.module_name)

    def do_update(self):
        raise Exception('please implement this method!')

    def update(self):
        self.do_init()
        self.do_update()

    def backup(self):
        pass

    def check(self):
        return True

    def do_rollback(self):
        raise Exception('please implement this method!')

    def rollback(self):
        self.do_init()
        self.do_rollback()
