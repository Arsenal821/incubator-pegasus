#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

把sp的client/server/deploy conf都拷贝到skv下面
"""
import os
import sys
import time

from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_guidance.arsenal_connector import ArsenalConnector
from hyperion_client.hyperion_inner_client.inner_config_manager import InnerConfigManager
from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo
from hyperion_client.module_service import ModuleService
from hyperion_client.deploy_topo import DeployTopo
from hyperion_client.config_manager import ConfigManager

SKV_ADMINTOOLS_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'admintools')
if SKV_ADMINTOOLS_ROOT_PATH not in sys.path:
    sys.path.append(SKV_ADMINTOOLS_ROOT_PATH)
from skv_common import fix_shell_config, is_hubble_installed, SKV_MODULE_NAME_LIST, SKV_REPLICA_SERVER_ROLE_NAME, \
    SKV_META_SERVER_ROLE_NAME


class BaseCopyZkConfStep(BaseInstallerStep):
    skv_module = None  # 需要修改为对应的模块

    def _fix_legacy_server_config(self, server_conf):
        """部分server conf因为历史原因 老版本没有机会修复 此处统一修复"""
        # 临时设置SKV_HOME 后面用到了
        SKV_CONSTRUCTION_BLUEPRINT_ROOT_PATH = os.path.join(os.environ['SKV_HOME'], 'construction_blueprint')
        if SKV_CONSTRUCTION_BLUEPRINT_ROOT_PATH not in sys.path:
            sys.path.append(SKV_CONSTRUCTION_BLUEPRINT_ROOT_PATH)
        from skv_config_manager import init_replica_server_group_config, init_replica_server_role_config
        # 1. 自动打开promethus
        PEGASUS_SERVER_SECTION = 'pegasus.server'
        per_counter_kv = {
            "perf_counter_enable_prometheus": "true",
            "perf_counter_sink": "prometheus"
        }
        for role in [SKV_REPLICA_SERVER_ROLE_NAME, SKV_META_SERVER_ROLE_NAME]:
            if PEGASUS_SERVER_SECTION not in server_conf[role]:
                server_conf[role][PEGASUS_SERVER_SECTION] = per_counter_kv
            else:
                server_conf[role][PEGASUS_SERVER_SECTION].update(per_counter_kv)

        # 2. 设置rocksdb_filter_type为common
        server_conf[SKV_REPLICA_SERVER_ROLE_NAME][PEGASUS_SERVER_SECTION]["rocksdb_filter_type"] = "common"

        # 3. 计算初始化的block cache和global write buffer，如果发现当前值小于这个值，则设置为这个值
        # 3.1 首先给每个配置组选取第一个主机 用于后面检查机器的内存
        group_to_onehost = {}
        for host, group in server_conf['host_group'][SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'].items():
            if group not in group_to_onehost:
                group_to_onehost[group] = host
        # 3.2 然后遍历检查配置组的配置
        for group, group_config in server_conf['host_group'][SKV_REPLICA_SERVER_ROLE_NAME]['group_config'].items():
            # group_config中有这个group, 但group_to_onehost中不一定有这个group,
            # 比如group下所有的host都被alter到别的group下了
            if group not in group_to_onehost:
                continue

            # 按照当前初始化规则计算一遍应该的初始值
            default_replica_group_config = init_replica_server_group_config(
                self.skv_module,
                ['/fake_path'],  # 我们不关心随机盘相关的配置，因此设置为一个假值
                InnerNodeInfo().get_machine_mem_gb(group_to_onehost[group]))
            kv = group_config[PEGASUS_SERVER_SECTION]
            for k in 'rocksdb_block_cache_capacity', 'rocksdb_max_open_files', 'rocksdb_total_size_across_write_buffer':
                # 配置必须是string的，但是实际含义是int的，需要转
                kv[k] = str(max(int(kv[k]), int(default_replica_group_config[PEGASUS_SERVER_SECTION][k])))

        # 3. 修改saas环境的部分值
        if is_hubble_installed():
            # 这些key要和配置对齐 value都是none只是占位
            saas_config_section_and_key = {
                'replication': {
                    "checkpoint_interval_seconds": None,
                    "gc_interval_ms": None,
                    "checkpoint_max_interval_hours": None,
                },
                'pegasus.server': {
                    'rocksdb_slow_query_threshold_ns': None,
                    'rocksdb_abnormal_get_size_threshold': None,
                    'rocksdb_abnormal_multi_get_size_threshold': None,
                    'prepare_timeout_ms_for_potential_secondaries': None,
                    'prepare_timeout_ms_for_secondaries': None,
                    'rocksdb_abnormal_multi_get_iterate_count_threshold': None,
                    'rocksdb_target_file_size_base': None,
                    'rocksdb_max_bytes_for_level_base': None,
                },
            }
            # 我们不关心和replica server相关的配置 因此设置为一个假值
            default_saas_role_config = init_replica_server_role_config(self.skv_module, ['fake1'])
            for section, keys in saas_config_section_and_key.items():
                current_kv = server_conf[SKV_REPLICA_SERVER_ROLE_NAME][section]
                default_kv = default_saas_role_config[section]
                for k in keys:
                    if k not in default_kv:
                        continue
                    if k not in current_kv:
                        # 如果配置不存在 则无脑覆盖
                        current_kv[k] = default_kv[k]
                    else:  # 都存在 选取大的
                        current_kv[k] = str(max(int(default_kv[k]), int(default_kv[k])))

    def _fix_legacy_client_config(self, client_conf):
        """部分client conf因为历史原因 老版本没有机会修复 此处统一修复"""
        # 1. client conf默认增加table_prefix 这个是为了给兼容混布skv的场景 一般没有设置的就表示不是混布 此处全部设置为空字符串
        # 这样后面建表的时候也可以少一些判断
        if 'table_prefix' not in client_conf:
            client_conf['table_prefix'] = ''
        # 2. partition_factor 已经在半年前发布了 按理说应该都存在了 如果不存在完全不符合预期 抛出异常
        if 'partition_factor' not in client_conf:
            raise Exception('cannot find partition_factor in client_conf!')

    def update(self):
        if self.skv_module not in DeployTopo().get_all_module_name_by_product_name('sp'):
            self.logger.info('skip copy zk conf for %s because not installed' % self.skv_module)
            return
        self.logger.info('start copy zk %s' % self.skv_module)
        # 后面需要用到这个skv_home 因此先设置一下
        os.environ['SKV_HOME'] = self.product_home

        # 1. 拷贝server_conf
        server_conf = InnerConfigManager().get_server_conf('sp', self.skv_module)
        # 修复server conf
        self._fix_legacy_server_config(server_conf)
        InnerConfigManager().set_server_conf('skv', self.skv_module, server_conf)
        self.logger.info('copy sp %s server conf to skv: %s' % (self.skv_module, server_conf))

        # 2. 拷贝client conf
        client_conf = InnerConfigManager().get_client_conf('sp', self.skv_module)
        client_conf['major_version'] = '2.0'
        # 修复client conf
        self._fix_legacy_client_config(client_conf)
        InnerConfigManager().set_client_conf('skv', self.skv_module, client_conf)
        self.logger.info('copy sp %s client conf to skv: %s' % (self.skv_module, client_conf))

        # 3. disable skv产品组件
        service = ModuleService()
        service.disable('skv')
        time.sleep(5)  # 以防万一 等待5s

        # 4. 拷贝deploy info
        # 高危操作 和李宁+姚聪讨论后确定inf内部使用ArsenalConnector迁移
        store_connector = ArsenalConnector.get_store().get_instance()
        for subpath in ['', '/meta_server', '/replica_server']:
            deploy_topo_sp_path = store_connector.join_full_path('deploy_topo', 'sp', self.skv_module + subpath)
            deploy_topo_skv_path = store_connector.join_full_path('deploy_topo', 'skv', self.skv_module + subpath)
            deploy_topo = store_connector.get_json_value_by_path(deploy_topo_sp_path)
            # 修改启动的命令
            if subpath != '':
                role = subpath[1:]
                deploy_topo['role_monitor_conf']['start_cmd'] = 'cd ${HOME} && python3 ${SKV_HOME}/construction_blueprint/skv_server.py start -m %s -r %s' % (self.skv_module, role)
            store_connector.set_json_value_by_path(deploy_topo_skv_path, deploy_topo)
            self.logger.info('copy deploy topo from %s to %s:\n%s' % (deploy_topo_sp_path, deploy_topo_skv_path, deploy_topo))

        # 5. 生成shell ini
        meta_server_nodes_map = {
            module: ConfigManager().get_server_conf_by_key('sp', module, 'meta_server_list')
            for module in SKV_MODULE_NAME_LIST if module in DeployTopo().get_all_module_name_by_product_name('sp')
        }
        hosts = DeployTopo().get_all_host_list()
        fix_shell_config([self.skv_module], meta_server_nodes_map, hosts, self.logger)

    def check(self):
        return True
