#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

检查 skv block_cache/write buffer 等内存配置是否合理
"""
import math
import os
import sys
import socket

from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo
from hyperion_client.deploy_info import DeployInfo
from hyperion_guidance.ssh_connector import SSHConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_admin_api import SkvAdminApi
from skv_common import SKV_PRODUCT_NAME, is_hubble_installed, SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'construction_blueprint'))
from skv_config_manager import init_replica_server_group_config

WRITE_BUFFER_KEY = 'rocksdb_total_size_across_write_buffer'
BLOCK_CACHE_KEY = 'rocksdb_block_cache_capacity'
RDB_WRITE_BUFFER_KEY = 'rocksdb_write_buffer_size'
SECTION_NAME = 'pegasus.server'
DEFAULT_RDB_WRITE_BUFFER = '20971520'

# saas 标准化 skv memory 配置
SAAS_STANDARD_MEM_CONF = {
    'skv_online': {
        WRITE_BUFFER_KEY: 8 * 1024 * 1024 * 1024,
        BLOCK_CACHE_KEY: 16 * 1024 * 1024 * 1024
    },
    'skv_offline': {
        WRITE_BUFFER_KEY: 16 * 1024 * 1024 * 1024,
        BLOCK_CACHE_KEY: 32 * 1024 * 1024 * 1024
    }
}


class CheckMemoryConfigWorker(BaseWorker):

    def _memory_rounding(self, memory_config):
        '''内存取整 4G以下取500MB的整数 4G以上必须是2G的倍数 担心写个不是整数的会触发bug'''
        HALF_GB = 0.5 * 1024 * 1024 * 1024
        TWO_GB = 2 * 1024 * 1024 * 1024
        FOUR_GB = 4 * 1024 * 1024 * 1024
        if memory_config < HALF_GB:
            return memory_config
        if memory_config < FOUR_GB:
            return int(math.ceil(memory_config / HALF_GB) * HALF_GB)
        return int(math.ceil(memory_config / TWO_GB) * TWO_GB)

    def _print_group_host_memory(self, group, host):
        connector = SSHConnector.get_instance(host)
        output = connector.check_output('free -g', self.logger.debug).strip()
        connector.close()
        self.logger.info('executing [free -g] on %s[%s]:\n%s' % (host, group, output))

    def _config_calculate(self, verbose=False):
        '''计算block_cache和write_buffer应该是多少，如果verbose=True(diagons/repair阶段), 则输出计算过程'''
        api = SkvAdminApi(self.logger, self.module)

        # 计算每个host对应的分片个数
        host_to_partition_num = {}
        for server_endpoint, node_info in api._get_nodes_details()['details'].items():
            host = socket.getfqdn(server_endpoint.split(':')[0])
            host_to_partition_num[host] = int(node_info['replica_count'])
        self.logger.debug('host_to_partition_num: %s' % host_to_partition_num)

        bad_config = False
        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        for group in skv_config_manager.get_config_groups():

            write_buffer_size = int(skv_config_manager.get_final_config_value(SECTION_NAME, WRITE_BUFFER_KEY, group))
            block_cache_size = int(skv_config_manager.get_final_config_value(SECTION_NAME, BLOCK_CACHE_KEY, group))

            # 检查saas环境内存相关配置
            # rocksdb_block_cache_capacity: skv_offline 32G skv_online 16G
            # rocksdb_total_size_across_write_buffer: skv_offline 16G skv_online 8G
            if is_hubble_installed():
                if verbose:
                    self.logger.info('In SaaS environment, memory conf will be checked according to SaaS standardization')
                if not self._check_saas_env_group_memory_config_worker(group, block_cache_size, write_buffer_size, verbose):
                    bad_config = True
            else:
                rdb_write_buffer_size = int(skv_config_manager.get_final_config_value(
                    SECTION_NAME, RDB_WRITE_BUFFER_KEY, group, DEFAULT_RDB_WRITE_BUFFER))
                hosts = skv_config_manager.get_config_group_hosts(group)
                max_partition = max([host_to_partition_num[h] for h in hosts])
                if verbose:
                    self.logger.info('checking group %s: %d hosts, max partition %d' % (group, len(hosts), max_partition))
                if not self._check_non_saas_env_group_memory_config_worker(hosts, group, block_cache_size, write_buffer_size, rdb_write_buffer_size, max_partition, verbose):
                    bad_config = True
        return bad_config

    def is_state_abnormal(self):
        return self._config_calculate(verbose=False)

    def diagnose(self):
        self._config_calculate(verbose=True)

    def repair(self):
        self._config_calculate(verbose=True)

    def _check_saas_env_group_memory_config_worker(self, group, block_cache_size, write_buffer_size, verbose=False):
        """
saas 环境随时都会有新租户接入，存在新表的创建和同时写入分片数不确定等因素，所以 saas 环境对 block cache 和 write buffer 参数设置一个预估值，避免后续参数频繁变更。
saas 环境 block cache, write buffer 主要是参考rocksdb_write_buffer(skv_offline: 80M,skv_online:20M)和约束总分片数(skv_offline:500,skv_online:1000M)，计算的一个标准预估值。
saas 标准化检查，参数 section_config 为当前 配置组 的 pegasus.server 值; 这里主要是检查实际的值和标准值是否一致。
        """
        cmd_list = []
        section_config = {BLOCK_CACHE_KEY: block_cache_size, WRITE_BUFFER_KEY: write_buffer_size}
        for k in [BLOCK_CACHE_KEY, WRITE_BUFFER_KEY]:
            if int(section_config[k]) != SAAS_STANDARD_MEM_CONF[self.module][k]:
                if verbose:
                    self.logger.warn('%s for %s expected val is %s, current val is %s' % (
                        k, group, SAAS_STANDARD_MEM_CONF[self.module][k], section_config[k]))
                cmd = 'skvadmin config set -m %s -r %s -g %s -s %s -n %s -v %d' % (
                    self.module, SKV_REPLICA_SERVER_ROLE_NAME, group, SECTION_NAME, k, SAAS_STANDARD_MEM_CONF[self.module][k])
                cmd_list.append(cmd)
        if cmd_list and verbose:
            self.logger.warn('please execute the follwing command to change config:\n%s' % ('\n'.join(cmd_list)))
            self._print_restart_replica(group)
        return len(cmd_list) == 0

    def _check_non_saas_env_group_memory_config_worker(self, hosts, group, block_cache_size, write_buffer_size, rdb_write_buffer_size, max_partition, verbose=False):
        """
非saas环境，通过总分片数和rocksdb配置 rocksdb_write_buffer_size 来计算rocksdb_total_size_across_write_buffer
具体检查为 rocksdb_total_size_across_write_buffer > 总分片数 * rdb_write_buffer_size * 0.4
block cahce 的值需要参考机器内存给出的一个默认值和 rocksdb_total_size_across_write_buffer * 1.5 来取最大值
        """
        if verbose:
            self.logger.info('checking config for config group[%s] hosts%s' % (group, hosts))

        # 计算配置组内最小的机器内存
        min_memory_gb = min([InnerNodeInfo().get_machine_mem_gb(host) for host in hosts])
        # 按照最小内存初始化配置 这里第二个参数传了假的数据盘路径 因为我们只关心内存计算的值
        default_replica_group_config = init_replica_server_group_config(self.module, ['/fake_path'], min_memory_gb)
        d = {k: int(default_replica_group_config[SECTION_NAME][k]) for k in [BLOCK_CACHE_KEY, WRITE_BUFFER_KEY]}
        if verbose:
            self.logger.info('checking min memory %dGB -> config %s' % (min_memory_gb, d))

        # 检查write_buffer > 每个单机的分片数 * rocksdb_write_buffer_size * 0.4
        # 计算配置组内最多的分片数
        self.logger.debug('group %s rocksdb_write_buffer_size %d, max_partition %d' % (group, rdb_write_buffer_size, max_partition))
        d[WRITE_BUFFER_KEY] = max(d[WRITE_BUFFER_KEY], max_partition * rdb_write_buffer_size)
        if verbose:
            self.logger.info('checking write_buffer_size %s, max_partition %d -> config %s %s' % (
                rdb_write_buffer_size, max_partition, WRITE_BUFFER_KEY, d[WRITE_BUFFER_KEY]))

        # write buffer 数值取整
        d[WRITE_BUFFER_KEY] = self._memory_rounding(d[WRITE_BUFFER_KEY])
        if verbose:
            self.logger.info('rounding write buffer -> %s' % d[WRITE_BUFFER_KEY])

        # 检查rocksdb_write_buffer 小于 block_cache 2/3
        d[BLOCK_CACHE_KEY] = int(max(d[BLOCK_CACHE_KEY], d[WRITE_BUFFER_KEY] * 1.5))
        if verbose:
            self.logger.info('checking block_cache by rocksdb_write_buffer -> config %s' % d)

        # block cache取整
        d[BLOCK_CACHE_KEY] = self._memory_rounding(d[BLOCK_CACHE_KEY])
        if verbose:
            self.logger.info('rounding block cache -> %s' % d[BLOCK_CACHE_KEY])

        # 对比当前的配置检查 允许10%的误差
        cmd_list = []
        section_config = {BLOCK_CACHE_KEY: block_cache_size, WRITE_BUFFER_KEY: write_buffer_size}
        for k in [BLOCK_CACHE_KEY, WRITE_BUFFER_KEY]:
            if int(section_config[k]) <= d[k] * 0.9:
                self.logger.warn('%s for %s expected val is %s, current val is %s' % (
                    k, group, d[k], section_config[k]))
                cmd = 'skvadmin config set -m %s -r %s -g %s -s %s -n %s -v %s' % (
                    self.module, SKV_REPLICA_SERVER_ROLE_NAME, group, SECTION_NAME, k, d[k])
                cmd_list.append(cmd)

        if cmd_list and verbose:
            self.logger.warn('please execute the follwing command to change config:\n%s' % ('\n'.join(cmd_list)))
            self._print_group_host_memory(group, hosts[0])
            self._print_restart_replica(group)
        return len(cmd_list) == 0

    # 打印修改配置信息以及重启skv信息
    def _print_restart_replica(self, group):
        if DeployInfo().get_simplified_cluster():
            restart_cmd = 'spadmin restart -m %s -p %s' % (self.module, SKV_PRODUCT_NAME)
            self.logger.warn(
                'after change config, please execute [%s] to RESTART replica server.' % restart_cmd)
        else:
            restart_cmd = 'skvadmin restart all_replica_server -m %s -g %s' % (self.module, group)
            self.logger.warn(
                'after change config, please execute [%s] to ROLLING RESTART all replica server one by one' % restart_cmd)
