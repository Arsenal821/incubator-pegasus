#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

mothership中配置管理(即原来的server_conf)是二级key value，section和name用'|'给join在了一起作为配置的key
同时mothership中需要指定配置的namespace，对应的就是我们生成的配置名称,replica_server.ini或者meta_server.ini
注意其中部分接口是只能通过命令行调用 另外一部分接口是不允许调用 希望抛出异常指导运维用命令行

mothership的client conf(包括meta server访问信息, 分片因子)存储在guidance里面

"""
import os
import sys
import traceback
import yaml


from hyperion_utils.shell_utils import check_call

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from construction_vehicle.constants import MCV_BLUEPRINT_NAME
from base_skv_config_manager import BaseSkvConfigManager
# 一般已经在sys.path里面了
mothership_client_dir = os.path.join(os.environ['MOTHERSHIP_HOME'], 'mothership_client')
if mothership_client_dir not in sys.path:
    sys.path.append(mothership_client_dir)
from mothership_client import MothershipClient


class MothershipSkvConfigManager(BaseSkvConfigManager):
    def get_default_port(self):
        """获取默认端口"""
        modules_yml = os.path.join(os.environ['SKV_HOME'], MCV_BLUEPRINT_NAME, 'mothership_modules.yaml')
        with open(modules_yml) as f:
            modules_conf = yaml.safe_load(f.read())
            return modules_conf['spec']['modules'][self.module]['roles'][self.role]['ports']['server_port']['port']

    def get_config_groups(self, server_conf=None):
        """获取所有配置组名称 返回一个list of string"""
        client = MothershipClient(self.logger)
        return client.get_all_role_config_groups_by_role_name(self.module, self.role)

    def get_config_group_hosts(self, group_name, server_conf=None):
        """获取某个配置组对应的主机列表"""
        client = MothershipClient(self.logger)
        return client.get_host_list_by_role_config_group_name(self.module, self.role, group_name)

    def get_config_group_by_host(self, host, server_conf=None):
        """获取某个主机所在的配置组"""
        client = MothershipClient()
        return client.get_role_config_group_by_host(self.module, self.role, host)

    def get_config_value(self, section, name, group_name=None, default_value=None):
        """根据配置的section和name 返回对应的value 如果group_name为None则返回默认配置 否则返回对应配置组的
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        client = MothershipClient(self.logger)
        config_name = '|'.join([section, name])
        conf_file = '%s.ini' % self.role
        if not group_name:
            return client.get_module_config(self.module, conf_file, config_name)
        else:
            return client.get_role_config_group_config(self.module, self.role, group_name, conf_file, config_name)

    def set_config_value(self, section, name, value, group_name=None, skip_confirm=True):
        """修改section和name 对应的value
如果group_name为None则设置默认配置组 否则设置对应配置组
这部分接口已经被云平台收回 改成使用mothershipadmin工具了
        """
        config_name = '|'.join([section, name])
        conf_file = '%s.ini' % self.role
        if not group_name:
            cmd = 'mothershipadmin module config set --module "%s" --namespace "%s" --key "%s" --value "%s"' % (
                self.module, conf_file, config_name, value)
        else:
            cmd = 'mothershipadmin role_config_group config set --role_config_group "%s" --module "%s" --role "%s" --namespace "%s" --key "%s" --value "%s"' % (
                group_name, self.module, self.role, conf_file, config_name, value)
        check_call(cmd, self.logger.debug)

    def get_config_section_to_kv(self, group_name=None):
        """获取某个配置组的全部配置, 没有指定配置组则获取默认配置组的配置
返回一个双层dict 第一层key是所有的section，第二层key是对应section的配置名称 对应value是 配置的值
        """
        client = MothershipClient(self.logger)
        if group_name:
            configs = client.get_role_config_group_configurations(self.module, self.role, group_name)
        else:
            configs = client.get_module_configurations(self.module)
        section_to_kv = {}
        for config_name, config_info in configs['%s.ini' % self.role].items():
            section, name = config_name.split('|')
            if section not in section_to_kv:
                section_to_kv[section] = {}
            section_to_kv[section][name] = config_info
        return section_to_kv

    def get_final_config_value(self, section, name, group_name, default_value=None):
        """部分工具会主动获取skv某个配置 此处说的是最终生成的配置
会依次检查默认配置 和 配置组配置, 优先级: 配置组 > 默认配置组 > 默认值
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        client = MothershipClient(self.logger)
        config_name = '|'.join([section, name])
        conf_file = '%s.ini' % self.role
        # 1. 获取配置组配置
        configs = client.get_role_config_group_configurations(self.module, self.role, group_name).get(conf_file)
        if config_name in configs:
            return configs[config_name]
        # 2. 获取默认配置组配置
        configs = client.get_module_configurations(self.module).get(conf_file)
        if config_name in configs:
            return configs[config_name]
        if default_value is None:
            raise Exception('cannot find neither module config nor group config!')
        return default_value

    def check_final_config_value(self, section, name, expected_value, group_name=None):
        """部分工具会主动检查skv某个配置是否正确配置 此处说的是 最终 生成的配置
group_name传入的是配置组名称 如果是空 则检查*所有*的配置组
会依次检查默认配置 和 配置组配置
返回true/false"""
        group_list = [group_name] if group_name else self.get_config_groups()
        config_name = '|'.join([section, name])
        conf_file = '%s.ini' % self.role
        client = MothershipClient(self.logger)

        # 1. 获取默认配置
        default_value = None
        configs = client.get_module_configurations(self.module).get(conf_file)
        if config_name in configs:
            default_value = configs[config_name]

        # 2. 依次检查配置组的配置, 当前配置组不存在时检查默认配置
        match = True
        for group in group_list:
            configs = client.get_role_config_group_configurations(self.module, self.role, group).get(conf_file)
            if config_name in configs:
                current_value = configs[config_name]
                if current_value != expected_value:
                    self.logger.warning('get config section[%s] name[%s] group[%s] value[%s] != expected[%s]' % (
                        section, name, group, current_value, expected_value))
                    match = False
            elif default_value is None:
                self.logger.warning('config section[%s] name[%s] not exist group [%s] and has no default value' % (
                    section, name, group))
                match = False
            else:
                if default_value != expected_value:
                    self.logger.warning('config section[%s] name[%s] not exist group [%s], get default value[%s] != expected[%s]' % (
                        section, name, group, current_value, expected_value))
                    match = False
        return match

    def get_host_list(self):
        """返回所有replica server的列表"""
        client = MothershipClient(self.logger)
        return client.get_host_list_by_role_name(self.module, self.role)

    def _get_config_kv(self, section, group_name=None, default_value=None):
        """返回某个section的配置 一个dict 是配置组的kv 主要适配skvadmin config set"""
        client = MothershipClient(self.logger)
        if group_name:
            configs = client.get_role_config_group_configurations(self.module, self.role, group_name)
        else:
            configs = client.get_module_configurations(self.module)
        kv = {}
        for config_name, config_info in configs['%s.ini' % self.role].items():
            s, name = config_name.split('|')
            if section == s:
                kv[name] = config_info
        return kv

    def __raise_exception_hint_mothershipadmin(self, group_name, subcmd=None):
        """很多接口云平台是不希望提供的 比如删除一个配置 他们希望这个工具链在他们手上
这种情况下需要抛出一个异常 提示运维如何修改
这里需要区分是否有group_name 如果有则是配置组的配置 如果没有则是模块配置
subcmd是更下一级的配置
        """
        cmd = 'mothershipadmin'
        if group_name:
            cmd += ' role_config_group'  # 修改配置组
        else:
            cmd += ' module'  # 修改配置组
        cmd += ' config'
        if subcmd:
            cmd += ' %s' % subcmd
            raise Exception('please use [%s]!' % cmd)

    def _set_config_section_to_kv(self, section_to_kv, group_name=None, skip_confirm=True):
        """整体修改配置组的配置 主要适配skvadmin config set"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to set config: module[%s] role[%s] group[%s] value:\n%s' % (
            self.module, self.role, group_name, section_to_kv))
        self.__raise_exception_hint_mothershipadmin(group_name, 'update')

    def _set_config_kv(self, kv, section, group_name=None, skip_confirm=True):
        """整体修改某个section的值 主要适配skvadmin config set"""
        # 由于mothership的配置是2级kv，没有section的概念 因此修改整个section需要涉及删除配置
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to set config: module[%s] role[%s] group[%s] section[%s] value:\n%s' % (
            self.module, self.role, group_name, section, kv))
        self.__raise_exception_hint_mothershipadmin(group_name, 'update')

    def _delete_config_section(self, section, group_name=None, skip_confirm=True):
        """整体删除某个section 主要适配skvadmin config set"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to delete config: module[%s] role[%s] group[%s] section[%s]' % (
            self.module, self.role, group_name, section))
        self.__raise_exception_hint_mothershipadmin(group_name, 'delete')

    def _delete_config(self, section, name, group_name=None, ignore_if_section_not_exist=False, skip_confirm=True):
        """整体删除某个配置 主要适配skvadmin config set"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to delete config: module[%s] role[%s] group[%s] section[%s] name[%s]' % (
            self.module, self.role, group_name, section, name))
        self.__raise_exception_hint_mothershipadmin(group_name, 'delete')

    def _alter_config_group(self, group, host, skip_confirm=True):
        """配置组修改 把host对应的配置组改成group 主要适配skvadmin config"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to alter config group[%s] host[%s]' % (group, host))
        raise Exception('please use [mothershipadmin role_config_group]!')

    def _add_config_group(self, config_copy_group=None, hosts=None, skip_confirm=True):
        """增加配置组
config_copy_group表示新的配置组的配置从哪个配置组拷贝过来
hosts 表示要把哪些当前的主机加到这里
        主要适配skvadmin config"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to add config group hosts[%s] config_copy_group[%s]' % (hosts, config_copy_group))
        raise Exception('please use [mothershipadmin role_config_group add]!')

    def _delete_config_group(self, group, transfer_host_group=None, skip_confirm=True):
        """删除配置组 transfer_host_group 表示把老的主机转移到这个配置组里面"""
        # 与姜睿沟通 mothership不支持接口 必须走mothershipadmin
        self.logger.error('failed to add config group group[%s] transfer_host_group[%s]' % (group, transfer_host_group))
        raise Exception('please use [mothershipadmin role_config_group delete]!')

    def _get_maintenace_config(self, worker_name, config_name, default_value):
        """skv maintenance job包含了部分可配置的检测规则
worker_name是检测任务的名字 比如skv_check_p99_worker
config_name是配置名 比如latency_bound
default_value 是如果没有配置 默认值咋写 比如100
与云平台沟通ing
        """
        # 后续云平台会提供这样的机制，在平台组件升级(比如skv产品线版本升级)的时候可以通过更新某个yml来增加guidance的key和默认value。
        # 此处可以认为这个配置一定存在 但是保险期间 还是加了try catch返回
        try:
            client = MothershipClient()
            # 配置是worker|config
            value = client.get_module_guidance_config(self.module, '|'.join([worker_name, config_name]))
            return default_value if value is None else value
        except Exception:
            self.logger.debug('failed to get guidance from mothership, use default value %s' % (default_value))
            self.logger.debug(traceback.format_exc())
            return default_value


# 添加接口测试
if __name__ == '__main__':
    import logging
    logger = logging.getLogger()
    r1 = 'meta_server'
    r2 = 'replica_server'
    mc1 = MothershipSkvConfigManager('skv_offline', r1, logger)
    mc2 = MothershipSkvConfigManager('skv_offline', r2, logger)
    '''get'''
    assert mc1.get_default_port() == 8170
    assert mc2.get_default_port() == 8171
    replica_group_list = mc2.get_config_groups()
    value = mc2.get_final_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', replica_group_list[0])
    print('rocksdb_total_size_across_write_buffer = %s' % value)
    value = mc2.get_final_config_value('pegasus.server', 'rocksdb_write_buffer_size', replica_group_list[0])
    print('rocksdb_write_buffer_size = %s' % value)
    meta_group_list = mc1.get_config_groups()
    replica_group_list = mc2.get_config_groups()
    print('meta_group_list = %s' % meta_group_list)
    print('replica_group_list = %s' % replica_group_list)
    meta_group_hosts = mc1.get_config_group_hosts(meta_group_list[0])
    replica_group_hosts = mc2.get_config_group_hosts(replica_group_list[0])
    print('%s meta_group_hosts = %s' % (meta_group_list[0], meta_group_hosts))
    print('%s replica_group_list = %s' % (replica_group_list[0], replica_group_hosts))
    meta_host_group = mc1.get_config_group_by_host(meta_group_hosts[0])
    replica_host_group = mc2.get_config_group_by_host(replica_group_hosts[0])
    assert meta_host_group == meta_group_list[0]
    assert replica_host_group == replica_group_list[0]

    '''group/default'''
    # replication|data_dirs:
    value = mc2.get_config_value('replication', 'data_dirs', replica_group_list[0])
    print(value)  # 获取指定配置组配置
    value = mc1.get_config_value('replication', 'data_dirs', meta_group_list[0])
    assert value is None  # return None, data_dirs 是replica server的配置
    value = mc2.get_config_value('replication', 'data_dirs')
    assert value is None  # return None, 默认配置不存在此配置

    # pegasus.server|rocksdb_write_buffer_size (只存在默认配置中的,可以添加在配置组中)
    value = mc2.get_config_value('pegasus.server', 'rocksdb_write_buffer_size')
    print("default value: rocksdb_write_buffer_size = %s" % value)  # 获取默认配置
    # 更改默认配置, 如果指定了配置组则跟改配置组的
    mc2.set_config_value('pegasus.server', 'rocksdb_write_buffer_size', str(int(value) + 1))
    value = mc2.get_config_value('pegasus.server', 'rocksdb_write_buffer_size')
    print("update default value: rocksdb_write_buffer_size = %s" % value)  # 获取默认配置

    value1 = mc2.get_config_value('pegasus.server', 'rocksdb_write_buffer_size', replica_group_list[0])
    print('group %s rocksdb_write_buffer_size = %s' % (replica_group_list[0], value1))  # 获取配置组配置
    # 更改配置组配置
    mc2.set_config_value('pegasus.server', 'rocksdb_write_buffer_size', 22020220, replica_group_list[0])
    value1 = mc2.get_config_value('pegasus.server', 'rocksdb_write_buffer_size', replica_group_list[0])  # 获取配置组配置
    print('update group %s rocksdb_write_buffer_size = %s' % (replica_group_list[0], value1))  # 获取配置组配置
    # check_final_config_value 会检查, 配置组的优先级高于默认的
    assert mc2.check_final_config_value('pegasus.server', 'rocksdb_write_buffer_size', value1)

    # pegasus.server|rocksdb_block_cache_capacity (获取配置组中的配置, 配置组中不存在则会返回None)
    value = mc2.get_config_value('pegasus.server', 'checkpoint_reserve_min_count', replica_group_list[0])
    assert value is None  # None
    # 获取默认配置
    value = mc2.get_config_value('pegasus.server', 'checkpoint_reserve_min_count')  # 2
    assert mc2.check_final_config_value('pegasus.server', 'checkpoint_reserve_min_count', value)

    # pegasus.server|rocksdb_total_size_across_write_buffer
    value = mc2.get_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer')
    assert value is None
    mc2.set_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', 33020220)
    value = mc2.get_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer')
    assert value is None  # None 改不了默认值
    value = mc2.get_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', replica_group_list[0])
    mc2.set_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', str(int(value) + 1), replica_group_list[0])
    value1 = mc2.get_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', replica_group_list[0])
    assert value != value1
    assert mc2.check_final_config_value('pegasus.server', 'rocksdb_total_size_across_write_buffer', value1)

    '''get total role conf'''
    meta_default_conf = mc1.get_config_section_to_kv()
    print('meta server default config: %s' % meta_default_conf)
    meta_conf = mc1.get_config_section_to_kv(meta_group_list[0])
    print('meta server group %s config: %s' % (meta_group_list[0], meta_conf))
    replica_default_conf = mc2.get_config_section_to_kv()
    print('replica server default config: %s' % replica_default_conf)
    replica_conf = mc2.get_config_section_to_kv(replica_group_list[0])
    print('replica server group %s config: %s' % (replica_group_list[0], replica_conf))

    print('replica server list: %s' % mc2.get_host_list())

    ''''get one section'''
    section_conf = mc1._get_config_kv('meta_server')  # defalut
    assert section_conf == meta_default_conf['meta_server']
    section_conf = mc1._get_config_kv('core', meta_group_list[0])
    assert section_conf == meta_conf['core']   # group
    section_conf = mc2._get_config_kv('pegasus.server')
    assert section_conf == replica_default_conf['pegasus.server']
    section_conf = mc2._get_config_kv('pegasus.server', replica_group_list[0])
    assert section_conf == replica_conf['pegasus.server']
