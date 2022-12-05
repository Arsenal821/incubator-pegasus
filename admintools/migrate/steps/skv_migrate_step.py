# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
"""

import json
import os
import shutil
import sys
import yaml


from abc import ABC
from stepworker.base_step import BaseStep

from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_context_details
from skv_admin_api import SkvAdminApi

SKV_SYSTEM_TABLE = ['temp', '__stat', '__detect']

TIMEOUT_MS = 200 * 1000


class SkvMigrateStep(BaseStep, ABC):
    def __init__(self):
        context_details = get_context_details()

        self.timeout_ms = TIMEOUT_MS
        self.module = context_details['module_name']
        with open(context_details['migrate_parameters_file']) as f:
            migrate_msg = yaml.load(f)

        self.skip_table_names =\
            migrate_msg['skip_table_names'].split(',') if migrate_msg['skip_table_names'] else []
        self.skip_table_names.extend(SKV_SYSTEM_TABLE)

        self.ssh_host = migrate_msg['ssh_host']
        self.ssh_port = migrate_msg['ssh_port']
        self.ssh_password = migrate_msg['ssh_password']
        self.max_batch_count = migrate_msg['max_batch_count']

        self.assign_table_names =\
            migrate_msg['assign_table_names'].split(',') if migrate_msg['assign_table_names'] else None

        self.src_meta_server_list, self.dest_meta_server_list = self.get_meta_server_list()
        self.manage_root_dir = self.get_stepworker_work_dir()
        self.copy_manage_file = os.path.join(self.manage_root_dir, 'copy_manage_file.yml')
        self.skv_tool_run_script = os.path.join(os.environ['SKV_HOME'], self.module, 'tools/run.sh')

        tools_dir = os.path.join(os.environ['SKV_HOME'], self.module, 'tools')
        self.shell_config_path = os.path.join(tools_dir, 'src', 'shell', 'config.ini')
        self.src_config_path = os.path.join(tools_dir, 'src', 'shell', 'config.ini.src')
        self.dest_config_path = os.path.join(tools_dir, 'src', 'shell', 'config.ini.dest')

        # 使用该 api 前，请确保 config.ini 处于对应的文件，可以通过下面两个接口切换 config.ini
        self.src_cluster_api = SkvAdminApi(logger=self.logger, meta_server_endpoint=self.src_meta_server_list)
        self.dest_cluster_api = SkvAdminApi(logger=self.logger, meta_server_endpoint=self.dest_meta_server_list)

    def change_config_file_to_source_cluster(self):
        """将 pegasus_shell config.ini 切换为 src_cluster"""
        shutil.copyfile(self.src_config_path, self.shell_config_path)

    def change_config_file_to_destination_cluster(self):
        """将 pegasus_shell config.ini 切换为 dest_cluster"""
        shutil.copyfile(self.dest_config_path, self.shell_config_path)

    def check_cluster_health(self):
        """检查两个集群是否健康"""
        self.change_config_file_to_source_cluster()
        if self.src_cluster_api.get_unhealthy_app_count() > 0:
            self.change_config_file_to_destination_cluster()
            raise Exception('skv cluster {meta_server_list} have unhealthy table! please check it.'.format(
                meta_server_list=self.src_meta_server_list))
        self.change_config_file_to_destination_cluster()
        if self.dest_cluster_api.get_unhealthy_app_count() > 0:
            raise Exception('skv cluster {meta_server_list} have unhealthy table! please check it.'.format(
                meta_server_list=self.dest_meta_server_list))

    def get_src_cluster_ssh_client(self):
        """获取数据来源集群的 ssh client"""
        src_cluster_ssh_client = SSHConnector.get_instance(
            hostname=self.ssh_host,
            user='sa_cluster',
            password=self.ssh_password,
            ssh_port=self.ssh_port,
        )
        return src_cluster_ssh_client

    def get_meta_server_list(self):
        """获取两个集群的 meta_server_list"""
        ssh_client = self.get_src_cluster_ssh_client()
        # 源端的 skv 可能在产品线 skv 下, 也可能在 sp 下, 这里做一下判断
        output = ssh_client.check_output('spadmin upgrader version 2>&1', self.logger.debug)
        product = 'sp'
        if 'skv current version' in output.strip():
            product = 'skv'
        get_meta_server_list_cmd = 'spadmin config get server -m %s -n meta_server_list -c -p %s' % (self.module, product)
        out = ssh_client.check_output(get_meta_server_list_cmd, self.logger.debug)
        # ssh 拿到的信息需要去格式化
        src_meta_cluster_server_list = ','.join(json.loads(out))
        dest_cluster_meta_server_list = SkvAdminApi(self.logger, self.module).meta_server_endpoint
        return src_meta_cluster_server_list, dest_cluster_meta_server_list
