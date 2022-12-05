#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

1. 检查集群是否启动
2. 检查是否有读写
3. 检查目录是否有东西
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep
from skv_admin_api import SkvAdminApi
from recipes import check_health
from skv_common import SKV_PRODUCT_NAME

from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_client.module_service import ModuleService


class PreCheckStep(BaseBackupStep):

    def do_update(self):
        # 检查是不是健康的
        if not check_health(self.logger, self.module_name):
            raise Exception('cluster not healthy!')
        # 检查服务起来没
        status = ModuleService().status(SKV_PRODUCT_NAME, self.module_name)
        if status != "ALIVE":
            raise Exception('please start %s before backup!' % self.module_name)
        # 检查是否有读写
        api = SkvAdminApi(self.logger, self.module_name)
        all_tables = api.get_all_avaliable_table_name()
        for table in all_tables:
            if api.check_table_has_ops(table):
                raise Exception('table %s still has write/read operations!' % table)
        # 检查目录是否为空
        for host in self.get_replica_server_hosts():
            connector = SSHConnector.get_instance(host)
            # 创建目录
            connector.check_call('mkdir -p %s' % self.backup_path_on_each_host, self.logger.debug)
            # 检查是否为空
            all_files = connector.check_output('ls -1 %s | wc -l' % self.backup_path_on_each_host, self.logger.debug)
            if all_files.strip() != '0':
                raise Exception('%s on %s is not empty!' % (self.backup_path_on_each_host, host))
