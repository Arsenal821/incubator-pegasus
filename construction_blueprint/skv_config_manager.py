#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

skv配置管理

skv配置文件在重启+安装的时候会有生成配置的逻辑 因此抽象在此处

此外旧版本的skv server conf实际并没有存储定制化配置
在扩缩容或者配置文件丢失的情况下，无法复原配置
因此在skv 4.2期项目中，我们把这个工作在sp 1.18里面实现了
后续在skv重启/sp大版本升级/skv升级的时候会调用这个方法
"""
import configparser
import datetime
import os
import sys
import socket

from hyperion_client.config_manager import ConfigManager
from construction_vehicle.constants import MCV_BLUEPRINT_NAME
from hyperion_client.deploy_topo import DeployTopo
from hyperion_element.global_properties import GlobalProperties
from hyperion_client.directory_info import DirectoryInfo
from hyperion_utils.shell_utils import check_call
from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo


SKV_ADMINTOOLS_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'admintools')
if SKV_ADMINTOOLS_ROOT_PATH not in sys.path:
    sys.path.append(SKV_ADMINTOOLS_ROOT_PATH)
from skv_common import get_config_file_path, get_zk_root, is_hubble_installed, \
    SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME, SKV_PRODUCT_NAME, \
    SKV_PROMETHEUS_PORT, SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME
from recipes import get_skv_config_manager

SKV_HOST_SPECIFIED_ITEMS = 'host_group'
"""

因mothership 2.0接入 部分逻辑在skv内部会写两份
一份是基于sp 2.0的 此时配置在zk server conf中 启停靠captain 是此处这些方法
另外一份是sp 2.1的 此时配置在mothership中 启停靠mothership 是由汤超重写的部分方法 在shim下
下面这些代码等到skv 2.1中可以去掉了

"""


def init_replica_server_role_config(module_name, replica_server_list):
    """初始化replica server配置 仅安装需要"""
    replica_server_num = len(replica_server_list)
    mutation_2pc_min_replica_count = 2 if replica_server_num > 2 else 1
    port = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, None).get_default_port()
    ret = {
        "apps.replica": {
            "ports": str(port),
        },
        'replication': {
            'mutation_2pc_min_replica_count': str(mutation_2pc_min_replica_count),
        },
        'pegasus.server': {
            'perf_counter_enable_prometheus': 'true',
            'perf_counter_sink': 'prometheus',
            'rocksdb_filter_type': 'common',
        },
    }
    # saas环境的特殊配置
    # 注意 目前其实这段代码正常安装的时候是不会走到的 因为正常安装sp是在hubble之前安装的 所以这个选择永远是false
    # 但是日后如果有明确方法能够在sp安装的时候知道是否是saas版 那么可以修改is_hubble_installed的实现 这段代码就有用了
    # 后续可以考虑把这部分代码放到health检查中去
    if is_hubble_installed():
        if module_name == SKV_OFFLINE_MODULE_NAME:
            ret['pegasus.server'].update({
                'rocksdb_write_buffer_size': '83886080',
                'rocksdb_target_file_size_base': '33554432',
                'rocksdb_max_bytes_for_level_base': '335544320',
            })
        else:
            ret['pegasus.server'].update({
                'rocksdb_write_buffer_size': '20971520',
                'rocksdb_target_file_size_base': '8388608',
                'rocksdb_max_bytes_for_level_base': '8388608',
            })
        ret['pegasus.server'].update({
            'rocksdb_slow_query_threshold_ns': '200000000',
            'rocksdb_abnormal_get_size_threshold': '1048576',
            'rocksdb_abnormal_multi_get_size_threshold': '10485760',
            'prepare_timeout_ms_for_potential_secondaries': '20000',
            'prepare_timeout_ms_for_secondaries': '12000',
            'rocksdb_abnormal_multi_get_iterate_count_threshold': '200',
        })
        ret['replication'].update({
            "checkpoint_interval_seconds": "900",
            "gc_interval_ms": "1800000",
            "checkpoint_max_interval_hours": "24",
        })
    return ret


def init_meta_server_role_config(module_name, replica_server_list):
    """初始化meta server配置 仅安装需要"""
    replica_server_num = len(replica_server_list)
    min_live_node_count_for_unfreeze = 2 if replica_server_num > 2 else 1
    server_load_balancer_type = 'greedy_load_balancer'
    max_replica_count = replica_server_num if replica_server_num < 3 else 3
    mutation_2pc_min_replica_count = 2 if replica_server_num > 2 else 1
    port = get_skv_config_manager(module_name, SKV_META_SERVER_ROLE_NAME, None).get_default_port()
    return {
        "apps.meta": {
            "ports": str(port),
        },
        'meta_server': {
            'min_live_node_count_for_unfreeze': str(min_live_node_count_for_unfreeze),
            'server_load_balancer_type': server_load_balancer_type
        },
        'replication.app': {
            'max_replica_count': str(max_replica_count)
        },
        'meta_server.apps.__detect': {
            'max_replica_count': str(max_replica_count),
            'app_name': '__detect',
            'app_type': 'pegasus',
            'partition_count': '8',
            'stateful': 'true',
            'package_id': ''
        },
        'meta_server.apps.__stat': {
            'max_replica_count': str(max_replica_count),
            'app_name': '__stat',
            'app_type': 'pegasus',
            'partition_count': '8',
            'stateful': 'true',
            'package_id': ''
        },
        'replication': {
            'mutation_2pc_min_replica_count': str(mutation_2pc_min_replica_count),
        },
        'pegasus.server': {
            'perf_counter_enable_prometheus': 'true',
            'perf_counter_sink': 'prometheus',
        },
    }


def init_meta_server_group_config(module_name, meta_dir):
    """初始化meta server配置组的配置 安装和扩容都需要"""
    return {
        "core": {
            "data_dir": os.path.join(meta_dir, module_name),
        }
    }


def init_replica_server_group_config(module_name, random_dirs, mem_gb, is_saas=False):
    """初始化replica server配置组的配置 安装和扩容都需要"""
    # 默认data dirs是所有随机盘
    data_dirs = ','.join(['data%d:%s' % (i, os.path.join(d, module_name)) for i, d in enumerate(random_dirs)])

    one_gb = 1024 * 1024 * 1024
    multiple = int(mem_gb / 28)
    # skv_offline use about 1/30 of total memory; skv_online use 1/15 of total memory
    if is_hubble_installed():
        # 如果是标准saas机器 可以拿一个磁盘出来存放slog, 其他磁盘存数据
        if is_saas:
            data_dirs = ','.join(['data%d:%s' % (i, os.path.join(d, module_name)) for i, d in enumerate(random_dirs[1:])])

        if module_name == SKV_OFFLINE_MODULE_NAME:
            rocksdb_block_cache_capacity = 34359738368
            rocksdb_max_open_files = min(multiple * 3 * 128, 512)
        else:
            rocksdb_block_cache_capacity = 17179869184
            rocksdb_max_open_files = min(multiple * 6 * 128, 512)
    # 当机器内存较大时(>28G), 根据 multiple 动态计算内存参数
    elif multiple >= 1:
        if module_name == SKV_ONLINE_MODULE_NAME:
            multiple *= 2
        rocksdb_block_cache_capacity = multiple * one_gb
        rocksdb_max_open_files = min(multiple * 128, 512)
    # 当机器内存资源小于 28G 时, 使用默认配置
    else:
        if module_name == SKV_ONLINE_MODULE_NAME:
            rocksdb_block_cache_capacity = 2 << 30
            rocksdb_max_open_files = 256
        else:
            rocksdb_block_cache_capacity = 600 << 20
            rocksdb_max_open_files = 128

    return {
        "core": {
            "data_dir": os.path.join(random_dirs[0], module_name),
        },
        "replication": {
            "slog_dir": os.path.join(random_dirs[0], module_name),
            "data_dirs": data_dirs,
        },
        'pegasus.server': {
            'rocksdb_block_cache_capacity': str(rocksdb_block_cache_capacity),
            'rocksdb_total_size_across_write_buffer': str(int(rocksdb_block_cache_capacity * 0.5)),
            'rocksdb_max_open_files': str(rocksdb_max_open_files),
        }
    }


def generate_new_config_parser(module_name, role_name, my_host, server_conf):
    """根据server_conf生成config_parser对象并返回"""
    config_template_file_name = "%s.ini.template" % role_name
    config_template_file = os.path.join(os.environ['SKV_HOME'],
                                        MCV_BLUEPRINT_NAME,
                                        "config_template",
                                        config_template_file_name)
    config_parser = configparser.ConfigParser()
    if not os.path.exists(config_template_file):
        raise Exception('cannot find %s!' % config_template_file)
    config_parser.read(config_template_file)

    # 动态计算的部分
    fixed_dict_data = get_fixed_config_params(module_name, role_name, my_host, server_conf)
    for (section, kv) in fixed_dict_data[role_name].items():
        if not kv:
            continue
        if not config_parser.has_section(section):
            config_parser.add_section(section)
        for (name, value) in kv.items():
            config_parser.set(section, name, value)

    # server conf中的角色相关的自定义配置
    for (section, kv) in server_conf[role_name].items():
        if not kv:
            continue
        if not config_parser.has_section(section):
            config_parser.add_section(section)
        for (name, value) in kv.items():
            config_parser.set(section, name, value)

    # server conf中主机相关的配置
    host_specified = get_host_specfic_config(my_host, role_name, server_conf)
    for (section, kv) in host_specified.items():
        if not kv:
            continue
        if not config_parser.has_section(section):
            config_parser.add_section(section)
        for (name, value) in kv.items():
            config_parser.set(section, name, value)

    return config_parser


def generate_std_config(module_name, role_name, server_conf, check_level, logger):
    """根据zk server conf+模板生成配置 最后会检查生成配置和旧配置的匹配程度
check_level值包括：
* None: 不检查旧配置，直接生成新配置
* loose: 如果旧配置中哪个配置不存在则打日志 如果旧配置中哪个配置值不一致也打日志
* strict：如果旧配置中哪个配置不存在则报错，如果旧配置中哪个配置值不一致也报错
    """
    config_parser = generate_new_config_parser(module_name, role_name, socket.getfqdn(), server_conf)
    # 检查配置 写入配置文件
    std_config_file = os.path.join(os.environ['SKV_HOME'], module_name, "conf", '%s.ini' % role_name)
    old_config_file = get_config_file_path(module_name, role_name)
    if os.path.isfile(old_config_file):
        if check_level:
            diff_current_config(old_config_file, role_name, config_parser, check_level, logger)
        # 备份一下旧的配置
        runtime_dir = os.path.join(DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME), '%s_conf_bk' % module_name)
        if not os.path.isdir(runtime_dir):
            check_call('mkdir -p %s' % runtime_dir)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        cmd = 'cp %s %s/%s.%s' % (old_config_file, runtime_dir, os.path.basename(old_config_file), timestamp)
        check_call(cmd)

    with open(std_config_file, "w") as f:
        config_parser.write(f)

    return std_config_file


def diff_current_config(file_name, role_name, new_config_parser, check_level, logger):
    """对比当前的配置文件是否是一致的 主要检查下面两种情况
1. 当前配置文件中的部分key在新配置中没有
2. 当前配置文件中的部分value和新配置不一致

check_level取值范围:
* loose: 如果发生上述情况则报错误
* strict：如果发生上述情况则抛异常
    """
    current_config_parser = configparser.ConfigParser()
    current_config_parser.read(file_name)

    for section in current_config_parser.sections():
        # 当前配置文件里面的section丢失
        if not new_config_parser.has_section(section):
            logger.warn('CONFIG DIFF! new config missing section[%s]: %s' % (
                section, dict(current_config_parser.items(section))))
            continue
        for key, value in current_config_parser.items(section):
            # 当前配置文件里面某个配置丢失
            if not new_config_parser.has_option(section, key):
                logger.warn('CONFIG DIFF! new config missing key[%s] in section[%s]' % (key, section))
            # 当前配置文件里面某个值不对
            elif new_config_parser.get(section, key) != value:
                logger.warn('CONFIG DIFF! section[%s] key[%s] value[%s] in current config, but [%s] in new config!' % (
                    section, key, value, new_config_parser.get(section, key)))
        logger.warn('CONFIG DIFF[%s]! current check_level is loose, will print this log and continue' % file_name)


def get_host_specfic_config(host, role_name, zk_server_conf):
    """主机相关的配置由于存储层级比较深 还带了一层索引 所以单独封装一个方法"""
    role_specfic_server_conf = zk_server_conf[SKV_HOST_SPECIFIED_ITEMS][role_name]
    # 获取配置组的名称
    group_name = role_specfic_server_conf['hosts_to_groups'][host]
    # 反查配置组的配置
    return role_specfic_server_conf['group_config'][group_name]


def get_fixed_config_params(module_name, role_name, host, zk_server_conf):
    """
    skv config items has 3:
    1. items which define by env
    2. items which defined by skv template
    3. items which will save in zk, that can changed
    return 1.
    """
    cluster_root = get_zk_root(module_name)
    sp_log_dir = InnerDirectoryInfo.get_instance().get_log_dir_by_product(SKV_PRODUCT_NAME)
    log_dir = os.path.join(sp_log_dir, module_name, role_name)

    if role_name == SKV_META_SERVER_ROLE_NAME:
        return_dict = {
            role_name: {
                "core": {
                    "log_dir": log_dir,
                },
                "tools.simple_logger": {
                    "max_log_file_bytes": str(500 * 1024 * 1024),
                    "max_number_of_log_files_on_disk": "20",
                },
                "network": {
                    "explicit_host_address": socket.gethostbyname(host),
                },
                "replication": {
                    "cluster_name": module_name,
                    "cold_backup_root": module_name
                },
                "meta_server": {
                    "server_list": ",".join(zk_server_conf['meta_server_list']),
                    "cluster_root": cluster_root,
                    "distributed_lock_service_parameters": os.path.join(cluster_root, "lock")
                },
                "pegasus.server": {
                    "perf_counter_cluster_name": module_name,
                    "prometheus_port": str(SKV_PROMETHEUS_PORT[module_name][role_name]),
                },
                "zookeeper": {
                    "wrapper_log_dir": log_dir,
                    "helper_log_dir": log_dir,
                    "hosts_list": ",".join(GlobalProperties.get_instance().zookeeper.connect)
                }
            }
        }
    elif role_name == SKV_REPLICA_SERVER_ROLE_NAME:
        return_dict = {
            role_name: {
                "core": {
                    "log_dir": log_dir,
                },
                "tools.simple_logger": {
                    "max_log_file_bytes": str(500 * 1024 * 1024),
                    "max_number_of_log_files_on_disk": "20",
                },
                "network": {
                    "explicit_host_address": socket.gethostbyname(host),
                },
                "meta_server": {
                    "server_list": ",".join(zk_server_conf['meta_server_list']),
                },
                "replication": {
                    "cluster_name": module_name,
                    "data_dirs_black_list_file": os.path.join(os.environ['SKV_HOME'], module_name, "conf/.skv_data_dirs_black_list"),
                    "cold_backup_root": module_name,
                    "gc_disk_error_replica_interval_seconds": str(60 * 60 * 24 * 3),
                },
                "pegasus.server": {
                    "perf_counter_cluster_name": module_name,
                    "prometheus_port": str(SKV_PROMETHEUS_PORT[module_name][role_name]),
                    "rocksdb_max_log_file_size": str(8 * 1024 * 1024),
                    "rocksdb_log_file_time_to_roll": str(24 * 60 * 60),
                    "rocksdb_keep_log_file_num": "32",
                }
            }
        }
    else:
        return {}

    # 更新meta server list
    return_dict[role_name]['pegasus.clusters'] = {module_name: ",".join(zk_server_conf['meta_server_list'])}
    # 如果部署了skv_online 需要包含所有集群的配置
    if 'skv_online' in DeployTopo().get_all_module_name_by_product_name(SKV_PRODUCT_NAME):
        the_other_module_name = 'skv_online' if module_name == 'skv_offline' else 'skv_offline'
        meta_server_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, the_other_module_name, 'meta_server_list')
        return_dict[role_name]['pegasus.clusters'][the_other_module_name] = ','.join(meta_server_list)

    return return_dict


def gengerate_meta_server_host_specified_items(host_to_meta_dir, module_name, logger):
    """生成 meta_server 的配置，meta_dir 必须为同一块盘"""
    logger.debug("%s meta_server core data_dir %s" % (module_name, str(host_to_meta_dir)))
    if len(set(host_to_meta_dir.values())) != 1:
        raise Exception('%s meta_server list meta_dir is different!%s' % (module_name, str(host_to_meta_dir)))
    else:
        return {
            'hosts_to_groups': {h: 'group_0' for h in host_to_meta_dir.keys()},
            'max_group_id': 0,
            'group_config': {
                'group_0': init_meta_server_group_config(module_name, list(set(host_to_meta_dir.values()))[0])
            }
        }


def gengerate_replica_server_host_specified_items(host_to_random_dir_list, host_to_mem_gb, module_name, logger):
    """生成 replica_server 的配置，整体逻辑为向下取值
data_dirs 为 hosts 中 random_dirs 的交集,
mem_gb 为 hosts 中的最小值"""
    data_dirs = set(list(host_to_random_dir_list.values())[0])

    for host, random_dir_list in host_to_random_dir_list.items():
        logger.debug("%s: %s replica_server random_dirs %s, mem_gb %d" % (
            host, module_name, str(random_dir_list), host_to_mem_gb[host]))
        data_dirs = data_dirs & set(random_dir_list)

    return {
        'hosts_to_groups': {h: 'group_0' for h in host_to_mem_gb.keys()},
        'max_group_id': 0,
        'group_config': {
            'group_0': init_replica_server_group_config(module_name, list(data_dirs), min(list(host_to_mem_gb.values())))
        }
    }
