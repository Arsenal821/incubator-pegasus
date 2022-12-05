#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

此处检查都是最后一起抛异常 这是为了防止有已知的不match场景(比如写入没法停止)

1. 检查测试表
2. 检查每个表的条数
"""
import os
import sys
import traceback
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import STATIC_CLUSTER_FILENAME
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from skv_admin_api import SkvAdminApi


class PostCheckStep(BaseRestoreStep):
    def do_update(self):
        if not self.is_first_replica_server():
            self.print_msg_to_screen('skip because not first replica server')
            return
        # 1. 解析之前统计的集群信息
        static_data_yml = os.path.join(self.restore_from_backup_path_on_each_host, STATIC_CLUSTER_FILENAME)
        with open(static_data_yml) as f:
            static_data = yaml.safe_load(f)

        # 此处记录最终结果
        exception_msg_list = []
        api = SkvAdminApi(self.logger, self.module_name)

        # 1. 检查表个数是否一致
        my_tables = sorted(api.get_all_avaliable_table_name())
        expect_tables = sorted(static_data['all_tables'])
        if my_tables != expect_tables:
            self.logger.error('check table failed: table unmatch! current %d tables: %s\nexpect %d tables: %s' % (
                len(my_tables), my_tables, len(expect_tables), expect_tables))
            exception_msg_list.append('table unmatch')

        # 2. 检查标记表是否一致
        mark_table_name = static_data['mark_table']
        if mark_table_name not in my_tables:
            self.logger.info('skip check mark table %s data because not in current cluster' % mark_table_name)
        else:
            self.print_msg_to_screen('checking table %s...' % mark_table_name)
            for hash_key, sort_key, value in static_data['mark_table_data']:
                try:
                    my_value = api.get_kv(mark_table_name, hash_key, sort_key)
                    if my_value != value:
                        self.logger.error('check mark table %s data failed! hash_key[%s] sort_key[%s] = %s, expect[%s]' % (
                            mark_table_name, hash_key, sort_key, my_value, value))
                        exception_msg_list.append('check mark table %s data failed' % mark_table_name)
                        break
                except Exception:
                    self.logger.error('check mark table %s data failed with exception! hash_key[%s] sort_key[%s] expect[%s]' % (
                        mark_table_name, hash_key, sort_key, value))
                    self.logger.error(traceback.format_exc())
                    exception_msg_list.append('check mark table %s data failed' % mark_table_name)
                    break
            else:
                self.logger.info('check mark table %s data ok' % mark_table_name)

        # 3. 检查表分片数前后是否有变化
        table_to_partition_count = sorted(api.get_all_table_to_partition_count())
        expect_table_to_partition_count = sorted(static_data['table_to_partition_count'])
        if table_to_partition_count != expect_table_to_partition_count:
            self.logger.error('check table partition failed: unmatch! current %s\nexpect %s' % (
                str(table_to_partition_count), str(expect_table_to_partition_count)))
            exception_msg_list.append('table partition unmatch')

        # 4. 检查表副本数数前后是否有变化
        table_to_replica_count = sorted(api.get_all_table_to_replica_count())
        expect_table_to_replica_count = sorted(static_data['table_to_replica_count'])
        if table_to_replica_count != expect_table_to_replica_count:
            self.logger.error('check table replica failed: unmatch! current %s\nexpect %s' % (
                str(table_to_replica_count), str(expect_table_to_replica_count)))
            exception_msg_list.append('table replica unmatch')

        # 5. 攒了一堆错误一起爆出来
        if exception_msg_list:
            details = ';'.join(exception_msg_list)
            raise Exception('post check failed!! check log for details: %s' % details)
