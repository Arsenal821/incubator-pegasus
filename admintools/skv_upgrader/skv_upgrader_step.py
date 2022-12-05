# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from abc import ABC
import os
import socket

from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo
from stepworker.base_step import BaseStep

from skv_admin_api import SkvAdminApi
from skv_common import (
    get_context_details,
    SKV_PRODUCT_NAME,
    SKV_META_SERVER_ROLE_NAME,
    SKV_REPLICA_SERVER_ROLE_NAME,
    get_installed_skv_modules,
)
from recipes import get_skv_config_manager


class SkvUpgraderStep(BaseStep, ABC):
    def __init__(self):
        self.context_details = get_context_details()
        self.skv_module_name = self.context_details['module_name']
        self.meta_server_host_list = get_skv_config_manager(self.skv_module_name, SKV_META_SERVER_ROLE_NAME, self.logger).get_host_list()
        self.skv_admin_api = SkvAdminApi(self.logger, self.skv_module_name)
        self.skv_meta_server_node_list = self.skv_admin_api.meta_server_endpoint.split(',')
        replica_server_host_list = get_skv_config_manager(self.skv_module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_host_list()
        self.skv_role_ip_list_map = {
            SKV_META_SERVER_ROLE_NAME: list(map(
                socket.gethostbyname, self.meta_server_host_list
            )),
            SKV_REPLICA_SERVER_ROLE_NAME: list(map(
                socket.gethostbyname, replica_server_host_list
            )),
        }

        self.skv_main_host = self.context_details['main_host']
        self.skv_remote_pack_path = self.context_details['skv_remote_pack_path']

        self.skv_product_dir = InnerDirectoryInfo.get_instance().get_home_dir_by_product(
            product_name=SKV_PRODUCT_NAME,
        )
        self.skv_module_dir = os.path.join(self.skv_product_dir, self.skv_module_name)

        runtime_dir = self.get_stepworker_work_dir()
        # 这个路径用来备份升级前的skv模块目录
        self.skv_old_dir = os.path.join(runtime_dir, 'old')
        self.skv_old_module_dir = os.path.join(self.skv_old_dir, self.skv_module_name)
        # 这个路径用来放置升级包解压的skv模块目录
        self.skv_new_dir = os.path.join(runtime_dir, 'new')
        self.skv_new_module_dir = os.path.join(self.skv_new_dir, self.skv_module_name)

        self.other_meta_server_nodes_map = {}
        for module in get_installed_skv_modules():
            self.other_meta_server_nodes_map[module] = SkvAdminApi(self.logger, module).meta_server_endpoint.split(',')
