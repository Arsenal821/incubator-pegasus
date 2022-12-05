#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

sp 2.0的读写抽象 从zk读写
"""
import json
import os
import sys
import yaml

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from base_skv_config_manager import BaseSkvConfigManager

from hyperion_client.hyperion_inner_client.inner_config_manager import InnerConfigManager
from hyperion_client.config_manager import ConfigManager
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo

from construction_vehicle.constants import MCV_BLUEPRINT_NAME
from skv_common import SKV_PRODUCT_NAME

HOST_GROUP_KEY = 'host_group'
GROUP_CONFIG_KEY = 'group_config'
HOST_TO_GROUP_KEY = 'hosts_to_groups'
MAX_GROUP_ID_KEY = 'max_group_id'
MAINTENANCE_JOB_CONF_KEY = 'skv_maintenance_workers'


class ZkSkvConfigManager(BaseSkvConfigManager):
    """zk server conf样例
{
    "host_group": {                              # 配置组级别的
        "meta_server": {                         # meta server的配置信息
            "group_config": {                    # 配置组级别的配置
                "group_0": {                     # 配置组名称
                    "core": {                    # section -> {name: value}
                        "data_dir": "/sensorsdata/metadata/skv_offline"
                    } # core
                } # group_0
            }, # group_config
            "hosts_to_groups": {                 # 配置组的对应关系 主机:配置组
                "hybrid01.octopus.deployid.octopus-1641468244756-debugbox": "group_0",
                "hybrid02.octopus.deployid.octopus-1641468244756-debugbox": "group_0",
                "hybrid03.octopus.deployid.octopus-1641468244756-debugbox": "group_0"
            },  # hosts_to_groups
            "max_group_id": 1                     # 这个主要是为了创建配置组时记录用的 最大配置组id
        }, # meta_server
        "replica_server": {
            ...
        },
    }, # host_group
    "meta_server": {                              # 从这里是默认配置组信息 下面是meta server的
        "apps.meta": {                            # section -> {name: value}
            "ports": "8170"
        },
        ...
    },
    "replica_server": {                           # 从这里是默认配置组信息 下面是 replica server的
        ....
    },
    ... # 其他配置我们不关心
}"""
    def get_default_port(self):
        """获取默认端口"""
        modules_yml = os.path.join(os.environ['SKV_HOME'], MCV_BLUEPRINT_NAME, 'modules.yml')
        with open(modules_yml) as f:
            modules_conf = yaml.safe_load(f.read())
            return modules_conf['modules'][self.module]['roles'][self.role]['role_desc']['port']

    def get_config_groups(self, server_conf=None):
        """获取所有配置组名称 返回一个list of string"""
        if server_conf is None:
            server_conf = self.__get_server_conf()
        return list(server_conf[HOST_GROUP_KEY][self.role][GROUP_CONFIG_KEY].keys())

    def get_config_group_hosts(self, group_name, server_conf=None):
        """获取某个配置组对应的主机列表"""
        if server_conf is None:
            server_conf = self.__get_server_conf()
        host_to_group = server_conf[HOST_GROUP_KEY][self.role][HOST_TO_GROUP_KEY]
        return [k for k, v in host_to_group.items() if v == group_name]

    def get_config_group_by_host(self, host, server_conf=None):
        """获取某个主机所在的配置组"""
        if server_conf is None:
            server_conf = self.__get_server_conf()
        host_to_group = server_conf[HOST_GROUP_KEY][self.role][HOST_TO_GROUP_KEY]
        if host not in host_to_group:
            raise Exception('cannot find host %s in %s!' % (host, host_to_group))
        return host_to_group[host]

    def get_config_value(self, section, name, group_name=None, default_value=None):
        """根据配置的section和name 返回对应的value 如果group_name为None则返回默认配置 否则返回对应配置组的
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        server_conf = self.__get_server_conf()
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        conf_desc = self.__format_conf_desc(group_name, section, name)
        if section not in section_to_kv:  # section不存在 返回默认值或者抛出异常
            if default_value is not None:
                self.logger.warn('try to get %s but section not exists; return default value[%s]' % conf_desc, default_value)
                return default_value
            else:
                raise Exception('cannot find section %s! candidates %s' % (section, list(section_to_kv.keys())))
        kv = section_to_kv[section]
        if name not in kv:  # name不存在 返回默认值或者抛出异常
            if default_value is not None:
                self.logger.warn('try to get %s but name not exists; return default value[%s]' % conf_desc, default_value)
                return default_value
            else:
                raise Exception('cannot find name %s! candidates %s' % (name, list(kv.keys())))
        if self.verbose:
            self.logger.info('get config %s %s' % (conf_desc, kv[name]))
        return kv[name]

    def set_config_value(self, section, name, value, group_name=None, skip_confirm=True):
        """修改section和name 对应的value
如果group_name为None则设置默认配置组 否则设置对应配置组
        """
        server_conf = self.__get_server_conf()
        conf_desc = self.__format_conf_desc(group_name, section, name)
        self.logger.debug('set %s->%s' % (conf_desc, value))
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        if section not in section_to_kv:  # section不存在 创建或者抛出异常
            self.logger.warn('section %s not exists, create it' % section)
            section_to_kv[section] = {}
        kv = section_to_kv[section]
        if self.verbose:
            if name in kv:
                self.logger.info('old config for %s: %s' % (conf_desc, kv[name]))
            self.logger.info('new config for %s: %s' % (conf_desc, value))
        if not skip_confirm:
            self._confirm_yes()
        kv[name] = value
        self.__set_server_conf(server_conf)

    def get_config_section_to_kv(self, group_name=None):
        """获取所有配置组的配置
返回一个双层dict 第一层key是所有的section，第二层key是对应section的配置名称 对应value是 配置的值
        """
        server_conf = self.__get_server_conf()
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        conf_desc = self.__format_conf_desc(group_name, None, None)
        if self.verbose:
            self.logger.info('get config %s\n%s' % (conf_desc, self.__dict_format(section_to_kv)))
        return section_to_kv

    def get_final_config_value(self, section, name, group_name, default_value=None):
        """部分工具会主动获取skv某个配置 此处说的是 最终 生成的配置
会依次检查默认配置 和 配置组配置
如果配置不存在 则返回default_value 如果default value为None 则抛异常"""
        server_conf = self.__get_server_conf()
        final_value = default_value
        # 1. 检查默认配置
        section_to_kv = self.__get_role_group_config(server_conf, None)
        default_conf_desc = self.__format_conf_desc(None, section, name)
        if section not in section_to_kv:  # section不存在 打印错误
            self.logger.debug('try to check %s but section not exists' % default_conf_desc)
        else:
            kv = section_to_kv[section]
            if name not in kv:  # name不存在 打印错误
                self.logger.debug('try to get %s but name not exists' % default_conf_desc)
            else:
                final_value = kv[name]

        # 2. 检查配置组的配置
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        conf_desc = self.__format_conf_desc(group_name, section, name)
        if section not in section_to_kv:
            self.logger.debug('try to check %s but section not exists' % conf_desc)
        else:
            kv = section_to_kv[section]
            if name not in kv:  # name不存在 打印错误
                self.logger.debug('try to get %s but name not exists' % conf_desc)
                if final_value is None:
                    raise Exception('cannot find neither %s nor %s!' % (default_conf_desc, conf_desc))
            else:
                final_value = kv[name]
        return final_value

    def check_final_config_value(self, section, name, expected_value, group_name=None):
        """部分工具会主动检查skv某个配置是否正确配置 此处说的是 最终 生成的配置
group_name传入的是配置组名称 如果是空 则检查*所有*的配置组
会依次检查默认配置 和 配置组配置
返回true/false"""
        server_conf = self.__get_server_conf()
        group_list = [group_name] if group_name else self.get_config_groups()

        # 1. 检查默认配置
        default_value = None
        section_to_kv = self.__get_role_group_config(server_conf, None)
        default_conf_desc = self.__format_conf_desc(None, section, name)
        if section not in section_to_kv:  # section不存在 打印错误
            self.logger.debug('try to check %s but section not exists' % default_conf_desc)
        else:
            kv = section_to_kv[section]
            if name not in kv:  # name不存在 打印错误
                self.logger.debug('try to get %s but name not exists' % default_conf_desc)
            else:
                default_value = kv[name]

        # 2. 检查配置组的配置
        match = True
        for group in group_list:
            current_value, current_conf_desc = default_value, default_conf_desc
            section_to_kv = self.__get_role_group_config(server_conf, group)
            conf_desc = self.__format_conf_desc(group, section, name)
            if section not in section_to_kv:
                self.logger.debug('try to check %s but section not exists' % conf_desc)
            else:
                kv = section_to_kv[section]
                if name not in kv:  # name不存在 打印错误
                    self.logger.debug('try to get %s but name not exists' % conf_desc)
                else:
                    current_value, current_conf_desc = kv[name], conf_desc
            # 比较结果
            if current_value != expected_value:
                self.logger.warn('get config %s value[%s] != expected[%s]' % (current_conf_desc, current_value, expected_value))
                match = False
        return match

    def get_host_list(self):
        """返回所有这个角色的主机(FQDN)列表"""
        return InnerDeployTopo().get_host_list_by_role_name(SKV_PRODUCT_NAME, self.module, self.role)

    def _get_config_kv(self, section, group_name=None, default_value=None):
        """返回某个section的配置 一个dict 是配置组的kv 主要适配skvadmin config set"""
        server_conf = self.__get_server_conf()
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        conf_desc = self.__format_conf_desc(group_name, section, None)
        if section not in section_to_kv:  # section不存在 返回默认值或者抛出异常
            if default_value is not None:
                self.logger.warn('try to get %s but section not exists; return default value[%s]' % conf_desc, default_value)
                return default_value
            else:
                raise Exception('cannot find section %s! candidates %s' % (section, list(section_to_kv.keys())))
        kv = section_to_kv[section]
        if self.verbose:
            self.logger.info('get config %s\n%s' % (conf_desc, self.__dict_format(kv)))
        return kv

    def _set_config_section_to_kv(self, section_to_kv, group_name=None, skip_confirm=True):
        """整体修改配置组的配置 主要适配skvadmin config set"""
        server_conf = self.__get_server_conf()
        conf_desc = self.__format_conf_desc(group_name, None, None)
        old_section_to_kv = self.__get_role_group_config(server_conf, group_name)
        self.__check_dict(section_to_kv, section_to_kv, [], 2)  # 检查是不是2层kv
        if self.verbose:
            self.logger.info('old config for %s:\n%s' % (conf_desc, self.__dict_format(old_section_to_kv)))
            self.logger.info('new config for %s:\n%s' % (conf_desc, self.__dict_format(section_to_kv)))
        if not skip_confirm:
            self._confirm_yes()
        if group_name is None:
            server_conf[self.role] = section_to_kv
        else:
            server_conf[HOST_GROUP_KEY][self.role][GROUP_CONFIG_KEY][group_name] = section_to_kv
        self.__set_server_conf(server_conf)

    def _set_config_kv(self, kv, section, group_name=None, skip_confirm=True):
        """整体修改某个section的值 主要适配skvadmin config set"""
        server_conf = self.__get_server_conf()
        conf_desc = self.__format_conf_desc(group_name, section, None)
        self.__check_dict(kv, kv, [], 1)  # 检查是不是1层kv
        self.logger.debug('set %s->%s' % (conf_desc, kv))
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        if self.verbose:
            if section in section_to_kv:
                self.logger.info('old config for %s:\n%s' % (conf_desc, self.__dict_format(section_to_kv[section])))
            self.logger.info('new config for %s:\n%s' % (conf_desc, self.__dict_format(kv)))
        if not skip_confirm:
            self._confirm_yes()
        section_to_kv[section] = kv
        self.__set_server_conf(server_conf)

    def _delete_config_section(self, section, group_name=None, skip_confirm=True):
        """整体删除某个section 主要适配skvadmin config set"""
        server_conf = self.__get_server_conf()
        conf_desc = self.__format_conf_desc(group_name, section, None)
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        if section in section_to_kv:
            if self.verbose:
                self.logger.info('old config for %s:\n%s' % (conf_desc, self.__dict_format(section_to_kv[section])))
        else:
            raise Exception('cannot find %s! candidates %s' % (section, list(section_to_kv.keys())))
        if self.verbose:
            self.logger.info('delete %s' % conf_desc)
        if not skip_confirm:
            self._confirm_yes()
        section_to_kv.pop(section)
        self.__set_server_conf(server_conf)

    def _delete_config(self, section, name, group_name=None, ignore_if_section_not_exist=False, skip_confirm=True):
        """整体删除某个配置 主要适配skvadmin config set"""
        server_conf = self.__get_server_conf()
        conf_desc = self.__format_conf_desc(group_name, section, name)
        self.logger.debug('delete %s' % conf_desc)
        section_to_kv = self.__get_role_group_config(server_conf, group_name)
        if section not in section_to_kv:  # section不存在 忽略或者抛出异常
            if ignore_if_section_not_exist:
                self.logger.warn('section %s not exists, do nothing and return' % section)
                return
            else:
                raise Exception('cannot find section %s! candidates %s' % (section, list(section_to_kv.keys())))
        kv = section_to_kv[section]
        if name in kv:
            if self.verbose:
                self.logger.info('old config for %s: %s' % (conf_desc, kv[name]))
        else:
            raise Exception('cannot find %s! candidates %s' % (name, list(kv.keys())))
        if self.verbose:
            self.logger.info('delete %s' % conf_desc)
        if not skip_confirm:
            self._confirm_yes()
        kv.pop(name)
        self.__set_server_conf(server_conf)

    def _alter_config_group(self, group, host, skip_confirm=True):
        """配置组修改 把host对应的配置组改成group 主要适配skvadmin config"""
        server_conf = self.__get_server_conf()
        groups = self.get_config_groups(server_conf)
        if group not in groups:
            raise Exception('cannot find group %s! candidates %s' % (group, groups))
        host_to_group = server_conf[HOST_GROUP_KEY][self.role][HOST_TO_GROUP_KEY]
        if host not in host_to_group:
            raise Exception('invalid host %s! candidates %s' % (host, list(host_to_group.keys())))
        old_group = host_to_group.get(host, None)
        if self.verbose:
            self.logger.info('alter host[%s] %s %s from group[%s] to group[%s]' % (host, self.module, self.role, old_group, group))
        if not skip_confirm:
            self._confirm_yes()
        host_to_group[host] = group
        self.__set_server_conf(server_conf)

    def _add_config_group(self, config_copy_group=None, hosts=None, skip_confirm=True):
        """增加配置组
config_copy_group表示新的配置组的配置从哪个配置组拷贝过来
hosts 表示要把哪些当前的主机加到这里
        主要适配skvadmin config"""
        server_conf = self.__get_server_conf()
        groups = self.get_config_groups(server_conf)
        hosts = hosts if hosts else []
        if config_copy_group:
            if config_copy_group not in groups:
                raise Exception('cannot find group %s! candidates %s' % (
                    config_copy_group, list(groups)))
            group_config = self.get_config_section_to_kv(config_copy_group)
        else:
            group_config = {}
        host_to_group = server_conf[HOST_GROUP_KEY][self.role][HOST_TO_GROUP_KEY]
        for host in hosts:
            if host not in host_to_group:
                raise Exception('invalid host %s! candidates %s' % (host, list(host_to_group.keys())))
        server_conf[HOST_GROUP_KEY][self.role][MAX_GROUP_ID_KEY] += 1
        group = 'group_%d' % server_conf[HOST_GROUP_KEY][self.role][MAX_GROUP_ID_KEY]
        if self.verbose:
            self.logger.info('add new group[%s] to %s %s, init %d hosts %s, config %s' % (
                group, self.module, self.role, len(hosts), hosts, group_config))
        if not skip_confirm:
            self._confirm_yes()
        server_conf[HOST_GROUP_KEY][self.role][GROUP_CONFIG_KEY][group] = group_config
        for host in hosts:
            host_to_group[host] = group
        self.__set_server_conf(server_conf)

    def _delete_config_group(self, group, transfer_host_group=None, skip_confirm=True):
        """删除配置组 transfer_host_group 表示把老的主机转移到这个配置组里面"""
        server_conf = self.__get_server_conf()
        groups = self.get_config_groups(server_conf)
        if group not in groups:
            raise Exception('cannot find group %s! candidates %s' % (group, groups))
        hosts = self.get_config_group_hosts(group)
        if hosts:
            if not transfer_host_group:
                raise Exception('failed to delete %s %s group %s: still have %d hosts %s' % (
                    self.module, self.role, group, len(hosts), hosts))
            else:
                if transfer_host_group not in groups:
                    raise Exception('cannot find group %s! candidates %s' % (transfer_host_group, groups))
                self.logger.warn('delete %s %s group %s, move %d hosts to group %s: %s' % (
                    self.module, self.role, group, len(hosts), transfer_host_group, hosts))
                host_to_group = server_conf[HOST_GROUP_KEY][self.role][HOST_TO_GROUP_KEY]
                for host in hosts:
                    host_to_group[host] = transfer_host_group
        else:
            self.logger.warn('delete %s %s empty group %s' % (self.module, self.role, group))
        server_conf[HOST_GROUP_KEY][self.role][GROUP_CONFIG_KEY].pop(group)
        if not skip_confirm:
            self._confirm_yes()
        self.__set_server_conf(server_conf)

    def _get_maintenace_config(self, worker_name, config_name, default_value):
        """skv maintenance job包含了部分可配置的检测规则
worker_name是检测任务的名字 比如skv_check_p99_worker
config_name是配置名 比如latency_bound
default_value 是如果没有配置 默认值咋写 比如100
        """
        server_conf = self.__get_server_conf()
        if MAINTENANCE_JOB_CONF_KEY in server_conf \
                and worker_name in server_conf[MAINTENANCE_JOB_CONF_KEY] \
                and config_name in server_conf[MAINTENANCE_JOB_CONF_KEY][worker_name]:
            return server_conf[MAINTENANCE_JOB_CONF_KEY][worker_name][config_name]
        return default_value

    """
    内部接口 和sp 2.0相关 请勿外部调用
    """
    def __get_server_conf(self):
        return ConfigManager().get_server_conf(SKV_PRODUCT_NAME, self.module)

    def __set_server_conf(self, server_conf):
        return InnerConfigManager().set_server_conf(SKV_PRODUCT_NAME, self.module, server_conf)

    def __dict_format(self, d):
        return json.dumps(d, indent=4, sort_keys=True)

    def __get_role_group_config(self, server_conf, group_name):
        """获取单个角色组的配置 group_name为None即为默认配置 就是角色配置"""
        if group_name:
            group_configs = server_conf[HOST_GROUP_KEY][self.role][GROUP_CONFIG_KEY]
            if group_name not in group_configs:
                raise Exception('cannot find group %s!: candidates%s' % (group_name, list(group_configs.keys())))
            return group_configs[group_name]
        else:
            return server_conf[self.role]

    def __format_conf_desc(self, group_name, section, name):
        """打日志需要描述配置的具体信息 这里format成一个string返回"""
        s = 'module[%s] role[%s]' % (self.module, self.role)
        if group_name:
            s += ' group_name[%s]' % group_name
        if section:
            s += ' section[%s]' % section
            if name:
                s += ' name[%s]' % name
        return s

    def __check_dict(self, new_value, d, upper_keys, num_level):
        """检查是否是一个合法的dict 包括
1. d的嵌套是否为num_level这个层级
2. value是不是str

主要是修改配置的时候对配置的结构检查"""
        if num_level == 0:
            if type(d) != str:
                raise Exception('invalid value: %s, value%s should be a string instead of %s' % (
                    new_value, ''.join(['[%s]' % k for k in upper_keys]), d))
            return
        if type(d) != dict:
            raise Exception('invalid value: %s, value%s should be a dict instead of %s' % (
                new_value, ''.join(['[%s]' % k for k in upper_keys]), d))
        for k, v in d.items():
            if type(k) != str:
                raise Exception('invalid key: %s, should be a string!' % k)
            upper_keys.append(k)
            self.__check_dict(new_value, v, upper_keys, num_level - 1)
            upper_keys.pop(-1)
