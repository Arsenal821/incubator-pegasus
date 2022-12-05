#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import json
import os
import socket
import sys

from stepworker.base_step import BaseStep
from stepworker.server import BaseServer

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import STATIC_REPLICA_SERVER_FILENAME
from skv_common import SKV_TOOLS_STEPWORKER_NAME, SKV_REPLICA_SERVER_ROLE_NAME, get_zk_root
from recipes import get_skv_config_manager


# 实际 gpid reps 数据存储的分配信息
GPIDS_DATA_ASSIGNATION_FILENAME = 'data_tag_assignation_by_gpid.yaml'


class BaseRestoreStep(BaseStep):

    def do_update(self):
        raise Exception('please implement this method!')

    def update(self):
        context = BaseServer.read_context(SKV_TOOLS_STEPWORKER_NAME)
        details = json.loads(context['details'])

        # 当前主机
        self.my_host = socket.getfqdn()
        self.my_ip = socket.gethostbyname(self.my_host)
        # 模块名
        self.module_name = details['module']
        # restore_from_backup_path_on_each_host <每台机器上备份到同一个目录>
        self.restore_from_backup_path_on_each_host = details['restore_from_backup_path_on_each_host']
        # 执行机
        self.execute_host = details['execute_host']

        self.do_update()

    def backup(self):
        pass

    def check(self):
        return True

    def get_data_dir_map(self):
        skv_config_manager = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        my_config_group = skv_config_manager.get_config_group_by_host(self.my_host)
        data_dirs = skv_config_manager.get_final_config_value('replication', 'data_dirs', my_config_group)
        ret = {}
        for x in data_dirs.split(','):
            k, v = x.split(':')
            ret[k] = v
        return ret

    def get_cluster_root(self):
        return get_zk_root(self.module_name)

    def get_replica_server_hosts(self):
        return get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_host_list()

    def is_replica_server(self):
        return self.my_host in self.get_replica_server_hosts()

    def is_first_replica_server(self):
        return self.my_host == sorted(self.get_replica_server_hosts())[0]

    def is_migrate(self):
        # 通过判断文件 static_replica_server.yml 是否存在确定是否为迁移
        if os.path.exists(os.path.join(self.restore_from_backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)):
            return False
        return True
