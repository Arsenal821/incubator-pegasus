# -*- coding: UTF-8 -*-

"""
Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import socket
import sys

from construction_vehicle.module_assembler.external_module_assembler import ExternalModuleAssembler

SKV_ADMINTOOLS_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'admintools')
if SKV_ADMINTOOLS_ROOT_PATH not in sys.path:
    sys.path.append(SKV_ADMINTOOLS_ROOT_PATH)
from skv_common import SKV_OFFLINE_MODULE_NAME, SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME, \
    REPLICA_SERVER_DISK_TYPE, META_SERVER_DISK_TYPE


class SkvModuleAssembler(ExternalModuleAssembler):
    """
    组装skv module
    """

    def __init__(self, logger):
        super().__init__(logger)

        self._skv_server_list_map = {}
        self._skv_server_conf = {}
        self._skv_client_conf = {}
        self._skv_monitor_conf = {}

    @property
    def skv_server_list_map(self):
        """把role map里面的fqdn:port变成ip:port 注意skv都是用的ip"""
        if self._skv_server_list_map:
            return self._skv_server_list_map

        for (role_name, server_list) in self.roles_map[self.module_name].items():
            self._skv_server_list_map[role_name] = []

            for host_port in server_list:
                host, port = host_port.split(':')
                ip = socket.gethostbyname(host)
                ip_port = "{ip}:{port}".format(ip=ip, port=port)
                self._skv_server_list_map[role_name].append(ip_port)

        return self._skv_server_list_map

    @property
    def skv_server_conf(self):
        if self._skv_server_conf:
            return self._skv_server_conf

        # 此处hack一下 因为这段代码会用root执行。。
        if 'SKV_HOME' not in os.environ:
            os.environ['SKV_HOME'] = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from skv_config_manager import init_meta_server_role_config, init_replica_server_role_config,\
            gengerate_replica_server_host_specified_items, gengerate_meta_server_host_specified_items, \
            SKV_HOST_SPECIFIED_ITEMS

        if (hasattr(self.runtime_conf, 'external_conf') and SKV_OFFLINE_MODULE_NAME in self.runtime_conf.external_conf):
            # 混部的时候需要填写server conf中的meta_server_list+replica_server_list字段
            skv_offline_conf = self.runtime_conf.external_conf[SKV_OFFLINE_MODULE_NAME]
            meta_server_list = list(map(str.strip, skv_offline_conf['meta_server_list'].split(',')))
            replica_server_list = list(map(str.strip, skv_offline_conf['replica_server_list'].split(',')))
            self._skv_server_conf = {
                'meta_server_list': meta_server_list,
                'replica_server_list': replica_server_list,
                'meta_server': '',
                'replica_server': '',
                SKV_HOST_SPECIFIED_ITEMS: '',
            }
            return self._skv_server_conf
        else:
            meta_server_list = self.skv_server_list_map[SKV_META_SERVER_ROLE_NAME]
            replica_server_list = self.skv_server_list_map[SKV_REPLICA_SERVER_ROLE_NAME]

        self._skv_server_conf = {
            'meta_server_list': meta_server_list,
            'replica_server_list': replica_server_list,
            'meta_server': init_meta_server_role_config(self.module_name, replica_server_list),
            'replica_server': init_replica_server_role_config(self.module_name, replica_server_list),
        }

        # dict {hostname: meta_dir}
        host_to_meta_dir = {}
        for server in meta_server_list:
            hostname = socket.getfqdn(server.split(':')[0])
            host_to_meta_dir[hostname] = self.get_storage_data_dir(hostname=hostname, storage_type=META_SERVER_DISK_TYPE[self.module_name])[0]

        # dict {hostname: list(random_dir)}
        host_to_random_dir_list = {}
        # dict {hostname: int(mem_gb)}
        host_to_mem_gb = {}
        for server in replica_server_list:
            hostname = socket.getfqdn(server.split(':')[0])
            host_to_random_dir_list[hostname] = self.get_storage_data_dir(hostname=hostname, storage_type=REPLICA_SERVER_DISK_TYPE[self.module_name])
            host_to_mem_gb[hostname] = self.get_hardware_info_resource(hostname=hostname, resource_type='mem')

        self._skv_server_conf[SKV_HOST_SPECIFIED_ITEMS] = {
            SKV_META_SERVER_ROLE_NAME:
                gengerate_meta_server_host_specified_items(
                    host_to_meta_dir,
                    self.module_name,
                    self.logger
                ),
            SKV_REPLICA_SERVER_ROLE_NAME:
                gengerate_replica_server_host_specified_items(
                    host_to_random_dir_list,
                    host_to_mem_gb,
                    self.module_name,
                    self.logger
                )
        }

        return self._skv_server_conf

    @property
    def skv_client_conf(self):
        # 此处hack一下 因为这段代码会用root执行。。
        if 'SKV_HOME' not in os.environ:
            os.environ['SKV_HOME'] = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from skv_common import REPLICA_SERVER_DISK_TYPE
        if self._skv_client_conf:
            return self._skv_client_conf

        # 分片因子，值为所有机器磁盘数总和，用于计算分片数。注意混部时值设置为3。
        # 老版本单机分片为4，集群为8，数据量不同时各机器负载是不一样的，为了让各个机器负载更均衡，保持每个磁盘上至少2分片，引入分片因子来计算分片数，使得数据储存分布更均匀。
        partition_factor = 0
        if (
            hasattr(self.runtime_conf, 'external_conf') and
            SKV_OFFLINE_MODULE_NAME in self.runtime_conf.external_conf
        ):
            skv_offline_conf = self.runtime_conf.external_conf[SKV_OFFLINE_MODULE_NAME]
            meta_server_list = list(map(str.strip, skv_offline_conf['meta_server_list'].split(',')))
            table_prefix = "{customer_id}_".format(
                customer_id=self.runtime_conf.user_conf_model.customer_id)
            partition_factor = 3
        else:
            meta_server_list = self.skv_server_list_map[SKV_META_SERVER_ROLE_NAME]
            table_prefix = ''
            replica_server_list = self.skv_server_list_map[SKV_REPLICA_SERVER_ROLE_NAME]
            for server in replica_server_list:
                hostname = socket.getfqdn(server.split(':')[0])
                # 通过内存来计算分区因子
                partition_factor_by_mem = int(self.get_hardware_info_resource(hostname=hostname, resource_type='mem') / 28)
                # 通过磁盘数来计算分区因子
                partition_factor_by_disk_count = len(self.get_storage_data_dir(hostname=hostname, storage_type=REPLICA_SERVER_DISK_TYPE[self.module_name]))
                partition_factor += min(partition_factor_by_mem, partition_factor_by_disk_count)

        self._skv_client_conf = {
            'meta_server_list': meta_server_list,
            'table_prefix': table_prefix,
            'partition_factor': partition_factor,
            # 此参数用于客户端在访问 server 时, garden是使用老版本客户端(xiaomiold) 还是新版本客户端(xiaomi)
            # 当第一位版本号为1, 使用的是xiaomiold，当第一位版本 >= 2 时,使用的是xiaomi
            # 这里是skv独立产品组件,先写入老版本，防止升级一半时 skv 业务方使用了新客户端访问老版本server出问题，待安装完成改为 2.0.0
            'major_version': '1.12',
        }
        return self._skv_client_conf

    @property
    def skv_monitor_conf(self):
        if self._skv_monitor_conf:
            return self._skv_monitor_conf

        if (
            hasattr(self.runtime_conf, 'external_conf') and
            SKV_OFFLINE_MODULE_NAME in self.runtime_conf.external_conf
        ):
            detached = True
        else:
            detached = False

        self._skv_monitor_conf = {
            'detached': detached,
        }
        return self._skv_monitor_conf

    def custom_module_generate_skv_server_conf(self, config_name):
        return self.skv_server_conf[config_name]

    def custom_module_generate_skv_client_conf(self, config_name):
        return self.skv_client_conf[config_name]

    def custom_module_generate_skv_monitor_conf(self, config_name):
        return self.skv_monitor_conf[config_name]

    def custom_role_generate_start_cmd(self, role_name):
        return (
            "cd {skv_home}/construction_blueprint && python3 skv_server.py start "
            "-m {module_name} -r {role_name}"
        ).format(
            skv_home=self.get_product_home_dir(),
            module_name=self.module_name,
            role_name=role_name,
        )
