#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

skv对外建表接口 实际对外调用的时候是被sp引用的(SKV_HOME/commlibs/inflibs/skv_conf_tools.py)
此处兼容了mothership和非mothership场景

最终配置依然写入zk/etcd上的client conf 这个是和李宁确认过的
"""
from enum import Enum
import os
import logging
import sys

from utils.shell_wrapper import check_output
from hyperion_guidance.arsenal_connector import ArsenalConnector

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import exists_module, SKV_REPLICA_SERVER_ROLE_NAME, is_skv_in_mothership
from recipes.platform_adapter import _get_client_conf, get_skv_config_manager


class ProjectTableNameFormat(Enum):
    PROJECT_TABLE_FORMAT_DEFAULT = "PROJECT_TABLE_FORMAT_DEFAULT"
    PROJECT_TABLE_FORMAT_ONLY_NUMBER_SUFFIX = "PROJECT_TABLE_FORMAT_ONLY_NUMBER_SUFFIX"
    PROJECT_TABLE_FORMAT_STRING_SUFFIX = "PROJECT_TABLE_FORMAT_STRING_SUFFIX"


class SkvTableManager:
    def __init__(self, logger=None):
        self.product = 'skv'
        self.product_home = os.environ['SKV_HOME']
        if logger:
            self.logger = logger
        else:
            self.logger = logging
        self.store_connector = ArsenalConnector().get_store().get_instance()

    def set_skv_table_conf(self, product, skv_module, business, table_by_project, table_name=None, partition_num=None, project_table_format=None, exist_ok=False):
        '''新增 skv client conf'''
        module = os.path.join(skv_module, business)
        skv_business_path = self.store_connector.join_full_path(product, 'client', module)

        # 查询client conf, 如果client conf 已存在，直接返回
        if exist_ok and self.store_connector.check_config_by_path(skv_business_path):
            self.logger.info("exist_ok=={exist_ok} and skv_business_path:{skv_business_path} exists, no need set it again.".format(exist_ok=exist_ok, skv_business_path=skv_business_path))
            return

        # 防御性检查是否部署该 skv_module
        if not exists_module(skv_module):
            raise Exception('module %s does not exists!' % skv_module)

        # pegasus原生只支持下划线 尽量不要用减号
        product_table_prefix_list = [product + '_', product.upper() + '_', product.replace('-', '_') + '_', product.upper().replace('-', '_') + '_']
        for product_table_prefix in product_table_prefix_list:
            if business.find(product_table_prefix) == 0:
                break
        else:
            raise Exception('business name must start with one of %s' % product_table_prefix_list)

        # 在该产品线在 zk 上的节点增加 skv 信息
        skv_module_path = self.store_connector.join_full_path(product, 'client', skv_module)
        if not self.store_connector.check_config_by_path(skv_module_path):
            self.store_connector.set_json_value_by_path(skv_module_path, {})

        # 检查表名合规性
        if table_name is None:
            table_name = business
        elif '-' in table_name:
            raise Exception('%s, table name can\'t support \'-\'' % table_name)

        # 用于混部等环境建表
        table_prefix = _get_client_conf(skv_module, 'table_prefix')
        table_name = table_prefix + table_name

        conf = {
            'table_name': table_name,
            'table_by_project': table_by_project,
        }

        if isinstance(partition_num, int):
            conf["partition_num"] = partition_num

        if isinstance(project_table_format, ProjectTableNameFormat):
            conf["project_table_format"] = project_table_format.value
        if isinstance(project_table_format, str):
            conf["project_table_format"] = project_table_format

        # 向 zk 写入 conf
        self.store_connector.set_json_value_by_path(skv_business_path, conf)

        self.logger.info(
            """create skv client conf.
            [business=%s, skv_module=%s, product=%s, table_by_project=%r, table_name=%s]""" %
            (business, skv_module, product, table_by_project, table_name))

    def create_table(self, product, skv_module, business, table_name=None, exist_ok=False):
        """在 skv 上创建普通表并增加 skv client conf"""
        if not isinstance(table_name, str) and table_name is not None:
            raise Exception('Invalid parameter : table_name-%s, type-%s!' % (table_name, type(table_name)))

        meta_server_list = _get_client_conf(skv_module, 'meta_server_list')
        if is_skv_in_mothership(skv_module):
            skv_tool_postfix = ' | %s/%s/%s/tools/run.sh shell --cluster %s' % (
                self.product_home, skv_module, SKV_REPLICA_SERVER_ROLE_NAME, ','.join(meta_server_list))
        else:
            skv_tool_postfix = ' | %s/%s/tools/run.sh shell --cluster %s' % (
                self.product_home, skv_module, ','.join(meta_server_list))

        real_table_name = business
        if isinstance(table_name, str):
            real_table_name = table_name
        if '-' in real_table_name:
            raise Exception('%s, table name can\'t support \'-\'' % table_name)

        default_replica_num = self._get_default_replica_num(skv_module)
        table_partition_num = 8 if 3 == default_replica_num else 4

        # 新建表
        self._execute_create_skv_table(skv_tool_postfix, real_table_name, table_partition_num, default_replica_num, exist_ok=exist_ok)

        # 新增 skv client conf
        self.set_skv_table_conf(product, skv_module, business, table_by_project=False, table_name=table_name, exist_ok=exist_ok)

    def create_big_table(self, product, skv_module, business, table_name=None, min_replica_per_disk=2, exist_ok=False, min_partition_num=None):
        """在 skv 上创建大表（性能随机器硬件资源提升而提升）并增加 skv client conf"""
        if not isinstance(table_name, str) and table_name is not None:
            raise Exception('Invalid parameter : table_name-%s, type-%s!' % (table_name, type(table_name)))

        meta_server_list = _get_client_conf(skv_module, 'meta_server_list')
        if is_skv_in_mothership(skv_module):
            skv_tool_postfix = ' | %s/%s/%s/tools/run.sh shell --cluster %s' % (
                self.product_home, skv_module, SKV_REPLICA_SERVER_ROLE_NAME, ','.join(meta_server_list))
        else:
            skv_tool_postfix = ' | %s/%s/tools/run.sh shell --cluster %s' % (
                self.product_home, skv_module, ','.join(meta_server_list))
        # partition factor按理说已经在sp 2.0都支持了 如果真的不支持报错了 可以跑一下spadmin skv health会自动补全
        partition_factor = _get_client_conf(skv_module, 'partition_factor')

        partition_factor = partition_factor * min_replica_per_disk
        if partition_factor <= 8:
            final_partition_num = 8
        elif partition_factor <= 16:
            final_partition_num = 16
        elif partition_factor <= 32:
            final_partition_num = 32
        elif partition_factor <= 64:
            final_partition_num = 64
        elif partition_factor <= 172:
            final_partition_num = 128
        else:
            final_partition_num = 256

        if min_partition_num is not None:
            def is_power_of_2(x):
                return x and (x & (x - 1) == 0)
            if not is_power_of_2(min_partition_num):
                raise ValueError("min_partition_num({m}) should be a power of 2".format(m=min_partition_num))
            if min_partition_num > 256:
                raise ValueError("min_partition_num({m}) should be <= 256".format(m=min_partition_num))
            if min_partition_num > final_partition_num * 4:
                raise ValueError("min_partition_num({m}) is too larger than estimated_partition_num({e})".format(
                    m=min_partition_num, e=final_partition_num))
            if min_partition_num > final_partition_num:
                final_partition_num = min_partition_num

        real_table_name = business
        if isinstance(table_name, str):
            real_table_name = table_name
        if '-' in real_table_name:
            raise Exception('%s, table name can\'t support \'-\'' % table_name)

        default_replica_num = self._get_default_replica_num(skv_module)

        # 新建表
        self._execute_create_skv_table(skv_tool_postfix, real_table_name, final_partition_num, default_replica_num, exist_ok=exist_ok)

        # 新增 skv client conf
        self.set_skv_table_conf(product, skv_module, business, table_by_project=False, table_name=table_name, exist_ok=exist_ok)

    def _execute_create_skv_table(self, skv_tool_postfix, table_name, partition_num, replica_num, exist_ok=False):
        """调用 pegasus_shell 在 skv 新建表"""
        if exist_ok and self._check_skv_table_exists(skv_tool_postfix, table_name):
            self.logger.info("exist_ok=={exist_ok} and table:{table_name} exists, no need create it again.".format(exist_ok=exist_ok, table_name=table_name))
            return

        cmd = 'echo -e "create {table_name} -p {partition_num} -r {replica_num}"{skv_tool_postfix}'.format(
            table_name=table_name,
            partition_num=partition_num,
            replica_num=replica_num,
            skv_tool_postfix=skv_tool_postfix,
        )
        output = check_output(cmd, self.logger.debug)
        if 'failed' in output:
            raise Exception('create table failed, please check skv cluster!!!')
        self.logger.info('create table %s partition_num=%d, replica_num=%d' % (table_name, partition_num, replica_num))

    def _check_skv_table_exists(self, skv_tool_postfix, table_name):
        cmd = 'echo -e "app {table_name}"{skv_tool_postfix}'.format(table_name=table_name,
                                                                    skv_tool_postfix=skv_tool_postfix)
        output = check_output(cmd, self.logger.debug)
        return "ERR_OBJECT_NOT_FOUND" not in output

    def _get_default_replica_num(self, module_name):
        """获取集群中副本数 主要和replica server个数相关"""
        config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        replica_server_num = len(config_manager.get_host_list())
        return replica_server_num if replica_server_num < 3 else 3
