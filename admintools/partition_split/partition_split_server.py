#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

server
主要是动态计算超时时间

"""
import datetime
import os
import socket

from stepworker.server import BaseServer
from stepworker.steps_info_utils import StepsInfoGenerator

from skv_admin_api import SkvAdminApi
from skv_common import SKV_TOOLS_PARTITION_SPLIT_OPERATION, SKV_TOOLS_STEPWORKER_NAME
from recipes.platform_adapter import _get_client_conf


class PartitionSplitServer(BaseServer):
    def __init__(self, logger, module_name, table, partition_count, context_type, multi_set=False):
        self.step_class_path = os.path.join(os.environ['SKV_HOME'], 'admintools/partition_split')
        self.context_details = {
            'module_name': module_name,
            'table': table,
            'partition_count': partition_count,
            'time_str': datetime.datetime.now().strftime('%Y%m%d_%H%M%S'),  # 这个字符串用来标记旧表名
            'multi_set': multi_set,
            'operation': SKV_TOOLS_PARTITION_SPLIT_OPERATION,
        }
        super().__init__(
            hosts=[socket.getfqdn()],
            name=SKV_TOOLS_STEPWORKER_NAME,
            support_rollback=True,
            step_class_path=self.step_class_path,
            logger=logger,
            context_type=context_type,
            context_details=self.context_details)

    def estamite_old_new_scan_time(self):
        """预估扫描老表和新表的时间 返回tuple(scan_old_table_time, scan_new_table_time)"""
        table = self.context_details['table']
        module_name = self.context_details['module_name']
        partition_count = self.context_details['partition_count']
        api = SkvAdminApi(self.logger, module_name)
        if table not in api.get_all_avaliable_table_name():
            raise Exception('cannot find table %s in %s!' % (table, module_name))
        table_size = api.app_disk_used(table)
        self.logger.info('table %s size %.2fMB' % (table, table_size))

        old_partition_count = api.get_table_partition_count(table)
        partition_factor = _get_client_conf(module_name, 'partition_factor')
        # 假设scan的速率是1MB/s 预估时长包括:
        # 1. count_data旧表的时间 并发数取决于老的partition_count和实际盘的个数
        # 300s是基本操作的超时时间 比如网络开销文件读写等
        scan_old_table_time = 300 + int(table_size / (1 * min(old_partition_count, partition_factor)))
        self.logger.info('scan old table time is about %d seconds' % scan_old_table_time)
        # 2. copy_data+count_data新表的时间 并发数取决于新的partition_count和实际盘的个数
        # 300s是基本操作的超时时间 比如网络开销文件读写等
        scan_new_table_time = 300 + int(table_size / (1 * min(partition_count, partition_factor)))
        self.logger.info('scan new table time is about %d seconds' % scan_new_table_time)
        return scan_old_table_time, scan_new_table_time

    def gen_steps_yml(self):
        """重写gen_steps.yml方法 主要修改是增超时"""
        steps_yml = self.get_steps_yml()
        if os.path.isfile(steps_yml):
            self.logger.info("{steps_yml} has been existing".format(steps_yml=steps_yml))
            return

        # 从customized_steps.yml加载
        step_file = os.path.join(self.step_class_path, 'customized_steps.yml')
        steps_info_list = StepsInfoGenerator.get_steps_info_list_by_customized_steps(step_file)
        # 增加超时
        # 注意！这个用法非常hack 有一定风险后续被云平台改了之后失效
        # 因此需要保底超时策略
        scan_old_table_time, scan_new_table_time = self.estamite_old_new_scan_time()
        for s in steps_info_list:
            if s['name'] == 'pre_check':  # 这一步的超时取决于旧表的分片
                s['timeout'] = scan_old_table_time
            elif s['name'] in ['copy_data', 'post_check']:  # 这一步的超时取决于新表的分片
                s['timeout'] = scan_new_table_time

        # 生成最终的 steps.yml
        StepsInfoGenerator.gen_steps_info_by_step_info_list(steps_info_list, save=True, steps_yml_path=steps_yml)
