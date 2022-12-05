# -*- coding: UTF-8 -*-

import configparser
import enum
import json
import os
import socket
import sys

from hyperion_client.deploy_topo import DeployTopo
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo
from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo
from hyperion_client.hyperion_inner_client.inner_resource_alloc import InnerResourceAlloc
from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_element.exceptions import IaasElementException

from stepworker.server import BaseServer

from utils.sa_utils import SAMysql

SKV_PRODUCT_NAME = 'skv'
HUBBLE_PRODUCT_NAME = 'hubble'

# skv的一些相关配置
SKV_HOST_SPECIFIED_ITEMS = 'host_group'  # todo 去掉
SKV_OFFLINE_MODULE_NAME = 'skv_offline'
SKV_ONLINE_MODULE_NAME = 'skv_online'
SKV_MODULE_NAME_LIST = [SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME, ]
SKV_META_SERVER_ROLE_NAME = 'meta_server'
SKV_REPLICA_SERVER_ROLE_NAME = 'replica_server'
TEMPLATE_YAML = 'template.yml.j2'
STEPS_YAML = 'steps.yaml'
SKV_ROLE_NAME_LIST = [SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME]
SKV_PROMETHEUS_PORT = {
    SKV_OFFLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: 8370,
        SKV_REPLICA_SERVER_ROLE_NAME: 8371
    },
    SKV_ONLINE_MODULE_NAME: {
        SKV_META_SERVER_ROLE_NAME: 8360,
        SKV_REPLICA_SERVER_ROLE_NAME: 8361,
    },
}

# 所有skv的工具都用这个存储上下文
SKV_TOOLS_STEPWORKER_NAME = 'skv_tools'

SKV_TOOLS_ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
SKV_TOOLS_UPGRADER_PATH = os.path.join(SKV_TOOLS_ROOT_PATH, 'skv_upgrader')
SKV_TOOLS_UPGRADER_STEPS_PATH = os.path.join(SKV_TOOLS_UPGRADER_PATH, 'steps')
SKV_TOOLS_MIGRATE_PATH = os.path.join(SKV_TOOLS_ROOT_PATH, 'migrate')
SKV_TOOLS_MIGRATE_STEPS_PATH = os.path.join(SKV_TOOLS_MIGRATE_PATH, 'steps')
SKV_TOOLS_MANUAL_COMPACTION_STEPS_PATH = os.path.join(SKV_TOOLS_ROOT_PATH, 'manual_compaction')

SKV_TOOLS_UPGRADE_OPERATION = 'upgrader'
SKV_TOOLS_MIGRATE_OPERATION = 'migrate'
SKV_TOOLS_PARTITION_SPLIT_OPERATION = 'partition_split'
SKV_TOOLS_MAINTENANCE_OPERATION = 'maintenance'
SKV_TOOLS_BACUP_REPLICA_OPERATION = 'backup_replica'
SKV_TOOLS_RESTORE_FROM_BACKUP_REPLICA_OPERATION = 'restore_from_backup_replica'
SKV_TOOLS_MANUAL_COMPACTION_OPERATION = 'manual_compaction'

APP_NAME_TO_ROLE_NAME = {'replica_server': 'replica', 'meta_server': 'meta'}

# 磁盘配置信息
REPLICA_SERVER_DISK_TYPE = {}
REPLICA_SERVER_DISK_TYPE[SKV_ONLINE_MODULE_NAME] = "online_random"
REPLICA_SERVER_DISK_TYPE[SKV_OFFLINE_MODULE_NAME] = "random"
META_SERVER_DISK_TYPE = {}
META_SERVER_DISK_TYPE[SKV_ONLINE_MODULE_NAME] = "online_random"
META_SERVER_DISK_TYPE[SKV_OFFLINE_MODULE_NAME] = "meta"


def is_skv_in_mothership(module_name):
    """
    判断module_name对应的模块是不是被mothership管理
    """
    # 联调环境有些问题 可能实际上应该用InnerDeployTopo.get_instance().get_product_name_list() 来判断是不是mothership产品组件安装了
    # 后面有正式联调环境再改
    if 'MOTHERSHIP_HOME' not in os.environ:
        return False
    mothership_client_dir = os.path.join(os.environ['MOTHERSHIP_HOME'], 'mothership_client')
    if mothership_client_dir not in sys.path:
        sys.path.append(mothership_client_dir)
    from mothership_client import MothershipClient
    return module_name in MothershipClient().get_all_modules()


def get_context_details():
    context = BaseServer.read_context(SKV_TOOLS_STEPWORKER_NAME)
    return json.loads(context['details']) if context else None


def get_operation_history(logger, max_count=None):
    sql = ("select `id`, server_host, status, tmp_work_path, start_time, end_time, details "
           "from sp_stepworker_context where name = '{name}' order by `id` desc").format(
        name=SKV_TOOLS_STEPWORKER_NAME)
    if max_count:
        sql += ' limit %d' % max_count
    result_row_list = SAMysql.query(sql, logger.debug)
    for row in result_row_list:
        row['details'] = json.loads(row['details'])
        row['operation'] = row['details']['operation']
    return result_row_list


# 有时候发现历史版本需要打印一个错误提示 默认往stderr里面打
def stderr_print_func(x):
    print(x, file=sys.stderr)


def get_zk_root(module_name):
    """
    返回对应模块的背包路径
    """
    if is_skv_in_mothership(module_name):
        from mothership_client import MothershipClient
        client = MothershipClient()
        client.get_tool_params(module_name)['runtime_params']['backpack_path']
    else:
        ira = InnerResourceAlloc.get_instance()
        product_backpack_path = ira.get_backpack_path_by_product_name("sp")  # 历史原因 还是sp
        return os.path.join(product_backpack_path, module_name)


def get_config_file_path(module_name, role_name, print_func=stderr_print_func):
    module_home_dir = os.path.join(os.environ['SKV_HOME'], module_name)
    conf_file_name = "{role_name}.ini".format(role_name=role_name)
    config_file = os.path.join(module_home_dir, "conf", conf_file_name)
    if os.path.exists(config_file):
        return config_file
    conf_file_name = "{module_name}-{role_name}-v2".format(module_name=module_name, role_name=role_name)

    conf_file_name += ".ini"
    config_dir = os.path.join(module_home_dir, "conf")
    current_config_file = os.path.join(config_dir, conf_file_name)
    if os.path.exists(current_config_file):
        return current_config_file
    current_config_file = os.path.join(config_dir, "{}.ini".format(module_name))
    return current_config_file


def fix_shell_config(module_name_list, meta_server_nodes_map, dest_hosts=None, logger=None):
    """默认生成本地的配置 也可以指定dest_hosts 拷贝到其他节点
修复pegasus-shell的配置 重新生成配置 抽取出来公用这个逻辑
实际上是先在本地生成 然后ssh客户端拷贝到每个机器

修改的主要包括：
1. 日志目录，自动填充
2. 目标集群的module_name->meta server地址映射

参数说明：
module_name_list: 是需要修复哪些模块，如果只传skv offline，那么只会修改$SKV_HOME/skv_offline/tools/src/shell/config.ini
meta_server_nodes_map：是一个map，key是模块名，value是meta_server_list，表示集群内所有的配置
dest_hosts: 修复哪些机器上的配置，None表示只生成本地的，否则会scp拷贝到目标机器
logger: 会打印info日志
"""
    for module_name in module_name_list:
        skv_home = InnerDirectoryInfo.get_instance().get_home_dir_by_product(
            product_name=SKV_PRODUCT_NAME)
        old_module_dir = os.path.join(skv_home, module_name)
        tools_dir = os.path.join(old_module_dir, 'tools')
        shell_config_path = os.path.join(tools_dir, 'src', 'shell', 'config.ini')
        if not os.path.isfile(shell_config_path):
            continue

        # 读取配置
        config_parser = configparser.ConfigParser()
        # we should not convert all keys to lowercase,
        # since we have "@CLUSTER_NAME@ = @CLUSTER_ADDRESS@"
        config_parser.optionxform = lambda option: option
        config_parser.read(shell_config_path)

        # 补充集群名->meta server地址映射
        for name, node_list in meta_server_nodes_map.items():
            config_parser.set("pegasus.clusters", name, ','.join(node_list))
            if logger:
                logger.info('added %s: %s to %s' % (name, node_list, shell_config_path))
        # 补充log目录
        skv_log_dir = InnerDirectoryInfo.get_instance().get_log_dir_by_product(SKV_PRODUCT_NAME)
        config_parser.set('core', 'log_dir', os.path.join(skv_log_dir, module_name, 'shell'))

        # 写会本地配置
        with open(shell_config_path, "w") as f:
            config_parser.write(f)

        if dest_hosts:
            # 拷贝到目标机器
            for host in dest_hosts:
                if host == socket.getfqdn():
                    continue
                connector = SSHConnector.get_instance(host)
                if logger:
                    connector.copy_from_local(shell_config_path, shell_config_path, logger.debug)
                    logger.info('copied %s to %s' % (shell_config_path, host))
                else:  # 输出到stderr
                    connector.copy_from_local(shell_config_path, shell_config_path)
                connector.close()


def exists_module(module_name):
    return module_name in get_installed_skv_modules()


def check_exists_module(module_name):
    if exists_module(module_name):
        return

    raise Exception("{module_name} is not found!".format(module_name=module_name))


def get_all_skv_modules():
    return SKV_MODULE_NAME_LIST


def get_cluster_version(module_name):
    module_home_dir = os.path.join(os.environ['SKV_HOME'], module_name)
    if not os.path.isfile(os.path.join(module_home_dir, 'META_SERVER_VERSION')):
        root_dir = os.path.join(os.environ['SKV_HOME'], SKV_OFFLINE_MODULE_NAME)
        bin_file_path = os.path.join(root_dir, 'bin', 'pegasus_server')
        if not os.path.isfile(bin_file_path):
            raise Exception("Cannot recognize the meta version file (META_SERVER_VERSION) of skv")
        module_home_dir = root_dir

    meta_version_file_path = os.path.join(module_home_dir, 'META_SERVER_VERSION')

    with open(meta_version_file_path) as f:
        meta_version = f.read()
        if len(meta_version) <= 0:
            raise Exception("Invalid skv meta_server version file: empty line")

    return ''.join(meta_version.split()[2:-1])


def get_program_start_log_path_by_role(module_name, role_name):
    """
获取对应角色的启动日志文件路径
role_name: meta_server/replica_server
hostname: fqdn
    """
    return os.path.join(
        get_server_log_dir_by_role(module_name, role_name),
        "{app_name}.output.ERROR".format(app_name=APP_NAME_TO_ROLE_NAME[role_name])
    )


def get_server_log_dir_by_role(module_name, role_name):
    """
获取对应角色名 server 日志目录的路径（而非文件路径）
    """
    if is_skv_in_mothership(module_name):
        from mothership_client import MothershipClient
        client = MothershipClient()
        log_dir = client.get_tool_params(module_name)['runtime_params']['log_dir']
        return os.path.join(log_dir, role_name)
    else:
        skv_log_dir = InnerDirectoryInfo.get_instance().get_log_dir_by_product(SKV_PRODUCT_NAME)
        return os.path.join(skv_log_dir, module_name, role_name)


def is_context_consistent(logger, old_context, new_context):
    """检查新的和老的上下文中相关参数一致"""
    result = True
    for key in new_context.keys():
        if key not in old_context.keys():
            logger.error('The key:%s does not exist in the context' % key)
            result = False
        elif old_context[key] != new_context[key]:
            logger.error('{key}:old_value={old_value}, new_value={new_value}'.format(key=key, old_value=old_context[key], new_value=new_context[key]))
            result = False
    return result


def assert_context_consistent(logger, old_context, new_context):
    """检查新的和老的上下文中相关参数一致 不一致抛异常"""
    current_operation = old_context.get('operation', 'unknown')
    if current_operation != new_context['operation']:
        raise Exception('other operation %s exist, execute [skvadmin history] for details. please perform %s later!' % (
            current_operation, new_context['operation']))
    if not is_context_consistent(logger, old_context, new_context):
        raise Exception('inconsistent context! check')


# 检查是否安装了Hubble。安装了，则认为是saas
def is_hubble_installed():
    try:
        product_list = InnerDeployTopo.get_instance().get_product_name_list()
        if HUBBLE_PRODUCT_NAME in product_list:
            return True
        return False
    except IaasElementException:
        return False


class SKVClusterType(enum.Enum):
    """skv目前支持的集群类型：单机，两节点，大于三节点。注意两节点是历史产物，正常新装不会存在"""
    ONE_NODE = "ONE_NODE"
    TWO_NODE = "TWO_NODE"
    GE_THREE_NODE = "GE_THREE_NODE"


def get_skv_cluster_type(module_name):
    """返回当前的类型"""
    if is_skv_in_mothership(module_name):
        from mothership_client import MothershipClient
        client = MothershipClient()
        # mothership是通过replica server的主机个数判断
        host_num = client.get_host_list_by_role_name(module_name, SKV_REPLICA_SERVER_ROLE_NAME)
    else:
        # 如果是sp 2.0 可以通过hyperion的接口计算模块的主机列表推算
        host_num = len(DeployTopo().get_host_list_by_module_name(SKV_PRODUCT_NAME, module_name))
    if host_num == 1:
        return SKVClusterType.ONE_NODE
    elif host_num == 2:
        return SKVClusterType.TWO_NODE
    else:
        return SKVClusterType.GE_THREE_NODE


def get_installed_skv_modules():
    """
对外接口 返回所有skv模块列表
注意模块可能在mothership也可能不在

一种特殊的情况是：skv_offline是一路升级上来的，不在mothership里面，但是升级到sp 2.1之后通过加online节点增加了skv_online，则skv_online在mothership里面
    """
    ret = []
    for module_name in SKV_MODULE_NAME_LIST:
        if is_skv_in_mothership(module_name):
            ret.append(module_name)
        elif SKV_PRODUCT_NAME in DeployTopo().get_product_name_list() and module_name in DeployTopo().get_all_module_name_by_product_name(SKV_PRODUCT_NAME):
            ret.append(module_name)
    return ret
