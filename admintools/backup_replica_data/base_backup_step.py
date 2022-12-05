#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import configparser
import json
import os
import socket
import sys

from stepworker.base_step import BaseStep
from stepworker.server import BaseServer

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_config_file_path, SKV_TOOLS_STEPWORKER_NAME, SKV_REPLICA_SERVER_ROLE_NAME
from skv_admin_api import SkvAdminApi
from recipes import get_skv_config_manager

# 统计集群的信息 包括所有表名 replica server列表等等
# 集群只有一个
STATIC_CLUSTER_FILENAME = 'static_cluster.yml'

# 每个replica_server的信息static_replica_server.yml，包括旧机器每个data tag-》大小，新集群每个tag-》大小，新机器ip
STATIC_REPLICA_SERVER_FILENAME = 'static_replica_server.yml'

# 备份前需要创建一个特殊标记的表
MARK_TABLE_PREFIX = 'skv_backup_replica_data_mark'

# 新集群的 replica 数据分布
NEW_CLUSTER_REPLICA_MAP_FILENAME = 'new_cluster_replica_map.yaml'

# replica 数据迁移的路径
REPLICA_MIGRATE_WAY_FILENAME = 'replica_migrate_way.yaml'


class BaseBackupStep(BaseStep):

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
        # backup_path_on_each_host 每台机器上备份到同一个目录>
        self.backup_path_on_each_host = details['backup_path_on_each_host']
        # 执行机
        self.execute_host = details['execute_host']
        # 迁移远端存储目录
        self.remote_migration_dir = details['remote_migration_dir']

        # 新的 replica_server list
        self.new_replica_server_list = details['new_replica_server_list']

        self.do_update()

    def backup(self):
        pass

    def check(self):
        return True

    def get_new_replica_server_ips(self):
        return [x.split(':')[0] for x in self.new_replica_server_list.split(',')]

    def get_data_dir_map(self):
        config_file = get_config_file_path(self.module_name, 'replica_server', self.logger.debug)
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file)
        data_dirs = config_parser.get('replication', 'data_dirs')
        ret = {}
        for x in data_dirs.split(','):
            k, v = x.split(':')
            ret[k] = v
        return ret

    def get_core_data_dir(self):
        config_file = get_config_file_path(self.module_name, 'replica_server', self.logger.debug)
        config_parser = configparser.ConfigParser()
        config_parser.read(config_file)
        return config_parser.get('core', 'data_dir')

    def get_cluster_root(self):
        # 通过api获取cluster root
        api = SkvAdminApi(self.logger, self.module_name)
        return api.get_cluster_root()

    def get_replica_server_hosts(self):
        return get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_host_list()

    def get_replica_server_addrs(self):
        skv_config_manager = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        port = skv_config_manager.get_default_port()
        return ['%s:%d' % (socket.gethostbyname(host), port) for host in skv_config_manager.get_host_list()]

    def is_replica_server(self):
        return self.my_host in self.get_replica_server_hosts()
