#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


逐一把sp的meta server停了，然后逐一启动skv的meta server
注意停可以用ModuleService，但是启动不可以，需要用local start

另外replica启停比较重
按需启停 停止前的准备工作需要调用recipes里面的代码 启动后检查也是
主要是停止前需要迁移primary副本 停止后需要检查服务正常
"""
import os
import socket
import sys
import time
import json
import yaml

from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_client.module_service import ModuleService
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo
from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo
from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_client.deploy_topo import DeployTopo

SKV_ADMINTOOLS_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'admintools')
if SKV_ADMINTOOLS_ROOT_PATH not in sys.path:
    sys.path.append(SKV_ADMINTOOLS_ROOT_PATH)
from recipes import prepare_safely_stop_replica_server, check_after_start_replica_server
from recipes import wait_table_healthy
from skv_common import is_hubble_installed
from skv_admin_api import SkvAdminApi


class BaseReplaceReplicaServerStep(BaseInstallerStep):
    skv_module = None  # 需要修改为对应的模块
    port = None  # 需要改为对应的port

    def update(self):
        if self.skv_module not in DeployTopo().get_all_module_name_by_product_name('sp'):
            self.logger.info('skip replace replica server for %s because not installed' % self.skv_module)
            return
        os.environ['SKV_HOME'] = self.product_home

        # 经过上一步频繁切 '主meta-server' 后, 这里需等待确保集群完全healthy后再继续
        wait_table_healthy(self.skv_module, self.logger, self.print_msg_to_screen)

        self.sp_host_lists = InnerDeployTopo().get_host_list_by_role_name('sp', self.skv_module, 'replica_server')
        self.service = ModuleService()

        if len(self.sp_host_lists) >= 3:  # 集群版
            # self._cluster_upgrader()
            self._prepare_non_rolling_upgrader()
            self._non_rolling_upgrader()
            self._finalize_non_rolling_upgrader()

        else:  # 单机版,操作计划不允许两机集群
            self._standalone_cluster_upgrader()

    def check(self):
        return True

    def _standalone_cluster_upgrader(self):
        host = self.sp_host_lists[0]
        self.print_msg_to_screen('stopping sp %s replica_server %s' % (self.skv_module, host))
        self.service.stop('sp', self.skv_module, 'replica_server', host)
        self._backup_old_skv_code()
        self.print_msg_to_screen('starting skv %s replica_server %s' % (self.skv_module, host))
        self.service.start('skv', self.skv_module, 'replica_server', host)

    def _cluster_upgrader(self):
        # saas 滚动升级
        if is_hubble_installed():
            self._prepare_rolling_upgrader()
            self._rolling_upgrader()
            self._finalize_rolling_upgrader()
        # 非saas 全停全起
        else:
            self._prepare_non_rolling_upgrader()
            self._non_rolling_upgrader()
            self._finalize_non_rolling_upgrader()

    def _prepare_rolling_upgrader(self):
        # 只对 saas 环境提供滚动重启, 重启前开启 bulk_load 模式
        # 遍历所有表记录下其 usage scenario 模式进行备份,保存在一个唯一文件中,防止滚动重启表模式丢失
        self.table_usage_sceario_file = os.path.join(os.path.dirname(__file__), 'skv_upgrade_table_usage_sceario.txt')
        self.api = SkvAdminApi(self.logger, self.skv_module)
        self.table_list = self.api.get_all_avaliable_table_name()
        self.tables_usage_sceario_dict = None
        if not os.path.exists(self.table_usage_sceario_file):
            self.tables_usage_sceario_dict = {}
            # 备份表环境变量
            for table_name in self.table_list:
                envs = self.api.get_table_env(table_name)
                if 'app_envs' in envs.keys() and 'rocksdb.usage_scenario' in envs['app_envs']:
                    self.tables_usage_sceario_dict[table_name] = envs['app_envs']['rocksdb.usage_scenario']
            try:
                with open(self.table_usage_sceario_file, 'w+', encoding='utf-8') as f:
                    f.write(json.dumps(self.tables_usage_sceario_dict, indent=4))
            except Exception as e:
                self.logger.error('backup tables usage scenario failed')
                raise Exception(str(e))
        # 备份完后将所有表开启 bulk_load 模式
        for table_name in self.table_list:
            self.api.set_table_env(table_name, 'rocksdb.usage_scenario', 'bulk_load')

    def _rolling_upgrader(self):
        # 滚动重启
        for host in self.sp_host_lists:
            ssh_connector = SSHConnector.get_instance(host)
            addr = '%s:%d' % (socket.gethostbyname(host), self.port)

            # 1. 按需停止老的服务
            old_status = self.service.status('sp', self.skv_module, 'replica_server', host)
            if old_status == 'ALIVE':
                prepare_safely_stop_replica_server(self.skv_module, self.logger, addr, self.print_msg_to_screen, timeout=600)
                self.service.stop('sp', self.skv_module, 'replica_server', host)
            elif old_status != 'DEAD':
                raise Exception('unexpect status for sp %s replica_server %s: %s' % (self.skv_module, host, old_status))

            # 2. 备份当前 host 上的老代码
            self._backup_old_skv_code(list(host))

            # 3. 启动新服务并检查
            self.print_msg_to_screen('starting skv %s replica_server %s' % (self.skv_module, host))
            ssh_connector.check_call('spadmin local start -p skv -m %s -r replica_server' % self.skv_module, self.logger.debug)
            ssh_connector.close()

            # 4. 检查服务
            check_after_start_replica_server(self.skv_module, self.logger, addr, self.print_msg_to_screen, timeout=600)

    def _finalize_rolling_upgrader(self):
        # 恢复表 usage_scenario 模式
        if self.tables_usage_sceario_dict is None:
            try:
                with open(self.table_usage_sceario_file, 'r', encoding='utf-8') as f:
                    self.tables_usage_sceario_dict = yaml.safe_load(f.read())
            except Exception as e:
                self.logger.error('read backup tables file[%s] failed' % self.table_usage_sceario_file)
                raise Exception(str(e))
        for table_name in self.table_list:
            value = self.tables_usage_sceario_dict[table_name] if table_name in self.tables_usage_sceario_dict else 'normal'
            self.api.set_table_env(table_name, 'rocksdb.usage_scenario', value)

    def _prepare_non_rolling_upgrader(self):
        product = 'sp'
        self.print_msg_to_screen('status check before stop product[%s] %s replica server' % (product, self.skv_module))
        self._check_replica_server_status(product, 'ALIVE')

    def _non_rolling_upgrader(self):
        # 1. 全停 sp 下 skv
        host = self.sp_host_lists[0]
        ssh_connector = SSHConnector.get_instance(host)
        self.print_msg_to_screen('stopping all old %s replica_server, executing on host[%s]' % (self.skv_module, host))
        ssh_connector.check_call('spadmin stop -p sp -m %s -r replica_server' % self.skv_module, self.logger.debug)
        self.print_msg_to_screen('stop old %s replica_server done, new %s replica_server will start, please wait patiently~' % (self.skv_module, self.skv_module))
        ssh_connector.close()
        time.sleep(30)

        # 2. 对 sp 下的老代码进行备份，防止误启动
        self._backup_old_skv_code()

        # 3. 全起 skv 下的 skv
        ssh_connector = SSHConnector.get_instance(host)
        self.print_msg_to_screen('starting all new %s replica_server, executing on host[%s]' % (self.skv_module, host))
        # 使用 restart 是适配升级可重入
        ssh_connector.check_call('spadmin restart -p skv -m %s -r replica_server' % self.skv_module, self.logger.debug)
        ssh_connector.close()

    def _finalize_non_rolling_upgrader(self):
        product = 'skv'
        self.print_msg_to_screen('status check after start product[%s] %s replica_server' % (product, self.skv_module))
        # 等待服务health
        wait_table_healthy(self.skv_module, self.logger, self.print_msg_to_screen)

    def _check_replica_server_status(self, product, status):
        # 检查replica server服务状态
        for host in self.sp_host_lists:
            ssh_connector = SSHConnector.get_instance(host)
            old_status = self.service.status(product, self.skv_module, 'replica_server', host)
            if old_status != status:
                raise Exception('unexpect status for %s %s replica_server %s: %s' % (product, self.skv_module, host, old_status))
            ssh_connector.close()

    def _backup_old_skv_code(self, hosts=None):
        # 备份 sp 下老版本的 skv 代码
        if not hosts:
            hosts = self.sp_host_lists
        for host in hosts:
            ssh_connector = SSHConnector.get_instance(host)
            sp_runtime_dir = InnerDirectoryInfo.get_instance().get_home_dir_by_product(product_name='sp')
            ret = ssh_connector.call('cd %s && ls %s.bak' % (sp_runtime_dir, self.skv_module))
            if ret == 0:
                ret = ssh_connector.call('cd %s && ls %s' % (sp_runtime_dir, self.skv_module))
                if ret == 0:
                    ssh_connector.check_call('cd %s && rm -rf %s' % (sp_runtime_dir, self.skv_module))
            else:
                ssh_connector.check_call('cd %s && cp -r %s %s.bak' % (sp_runtime_dir, self.skv_module, self.skv_module))
                ssh_connector.check_call('cd %s && rm -rf %s' % (sp_runtime_dir, self.skv_module))
            ssh_connector.close()
