#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

对配置管理工具的抽象

因 mothership 2.0 接入 部分逻辑在skv内部会写两份
一份是基于 sp 2.0 的 此时配置在zk server conf中 启停靠captain 是此处这些方法
另外一份是sp 2.1的 此时配置在mothership中 启停靠mothership 是由汤超重写的部分方法 在shim下

"""


class BaseSkvConfigManager:
    """
封装各类配置解析工作 兼容sp 2.0/sp 2.1各类逻辑
    """
    def __init__(self, module, role, logger, verbose=True):
        self.module = module
        self.role = role
        self.logger = logger
        self.verbose = verbose

    def get_default_port(self):
        """获取默认端口"""
        raise Exception('please implement this method!')

    def get_config_groups(self, server_conf=None):
        """获取所有配置组名称 返回一个list of string"""
        raise Exception('please implement this method!')

    def get_config_group_hosts(self, group_name, server_conf=None):
        """获取某个配置组对应的主机列表"""
        raise Exception('please implement this method!')

    def get_config_group_by_host(self, host, server_conf=None):
        """获取某个主机所在的配置组"""
        raise Exception('please implement this method!')

    def get_config_value(self, section, name, group_name=None, default_value=None):
        """根据配置的section和name 返回对应的value 如果group_name为None则返回默认配置 否则返回对应配置组的
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        raise Exception('please implement this method!')

    def set_config_value(self, section, name, value, group_name=None, skip_confirm=True):
        """修改section和name 对应的value
如果group_name为None则设置默认配置组 否则设置对应配置组
        """
        raise Exception('please implement this method!')

    def get_config_section_to_kv(self, group_name=None):
        """获取所有配置组的配置
返回一个双层dict 第一层key是所有的section，第二层key是对应section的配置名称 对应value是 配置的值
        """
        raise Exception('please implement this method!')

    def get_final_config_value(self, section, name, group_name, default_value=None):
        """部分工具会主动获取skv某个配置 此处说的是 最终 生成的配置
会依次检查默认配置 和 配置组配置
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        raise Exception('please implement this method!')

    def check_final_config_value(self, section, name, expected_value, group_name=None):
        """部分工具会主动检查skv某个配置是否正确配置 此处说的是 最终 生成的配置
group_name传入的是配置组名称 如果是空 则检查*所有*的配置组
会依次检查默认配置 和 配置组配置
返回true/false"""
        raise Exception('please implement this method!')

    def get_host_list(self):
        """返回所有replica server的列表"""
        raise Exception('please implement this method!')

    def _get_config_kv(self, section, group_name=None, default_value=None):
        """返回某个section的配置 一个dict 是配置组的kv 主要适配skvadmin config set"""
        raise Exception('please implement this method!')

    def _set_config_section_to_kv(self, section_to_kv, group_name=None, skip_confirm=True):
        """整体修改配置组的配置 主要适配skvadmin config set"""
        raise Exception('please implement this method!')

    def _set_config_kv(self, kv, section, group_name=None, skip_confirm=True):
        """整体修改某个section的值 主要适配skvadmin config set"""
        raise Exception('please implement this method!')

    def _delete_config_section(self, section, group_name=None, skip_confirm=True):
        """整体删除某个section 主要适配skvadmin config set"""
        raise Exception('please implement this method!')

    def _delete_config(self, section, name, group_name=None, ignore_if_section_not_exist=False, skip_confirm=True):
        """整体删除某个配置 主要适配skvadmin config set"""
        raise Exception('please implement this method!')

    def _alter_config_group(self, group, host, skip_confirm=True):
        """配置组修改 把host对应的配置组改成group 主要适配skvadmin config"""
        raise Exception('please implement this method!')

    def _add_config_group(self, config_copy_group=None, hosts=None, skip_confirm=True):
        """增加配置组
config_copy_group表示新的配置组的配置从哪个配置组拷贝过来
hosts 表示要把哪些当前的主机加到这里
        主要适配skvadmin config"""
        raise Exception('please implement this method!')

    def _delete_config_group(self, group, transfer_host_group=None, skip_confirm=True):
        """删除配置组 transfer_host_group 表示把老的主机转移到这个配置组里面"""
        raise Exception('please implement this method!')

    def _get_maintenace_config(self, worker_name, config_name, default_value):
        """skv maintenance job包含了部分可配置的检测规则
worker_name是检测任务的名字 比如skv_check_p99_worker
config_name是配置名 比如latency_bound
default_value 是如果没有配置 默认值咋写 比如100
        """
        raise Exception('please implement this method!')

    def _confirm_yes(self):
        """交互式输入确认"""
        self.logger.info('please enter [yes] to confirm')
        resp = input()
        if resp != 'yes':
            self.logger.error('invalid response: %s' % resp)
            raise Exception('failed to set skv config!')

    def _calc_partition_factor(self):
        """计算分区因子 加减盘或者节点时需要调用"""
        from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo
        partition_factor = 0
        mem = 0
        for group in self.get_config_groups():
            data_dir_num = len(self.get_final_config_value('replication', 'data_dirs', group).split(','))
            for host in self.get_config_group_hosts(group):
                mem = InnerNodeInfo().get_machine_mem_gb(host)
                partition_factor += min(int(mem / 28), data_dir_num)
        return partition_factor
