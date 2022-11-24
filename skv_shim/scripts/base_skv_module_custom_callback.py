#!/bin/env python
# -*- coding: UTF-8 -*-

"""
skv_offline/skv_online 扩缩容 & 加减盘 回调
"""

import os
import sys
import socket
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'skv_shim/scripts/skv_utils'))
from shell_wrapper import check_output

skv_tools_path = os.path.join(os.environ['SKV_HOME'], 'admintools')
if skv_tools_path not in sys.path:
    sys.path.append(skv_tools_path)
from skv_common import get_skv_cluster_type, SKVClusterType, SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME
from recipes import check_balance, balance_and_wait, check_health, inactive_replica, restart_all_meta_server, wait_replica_server, wait_table_healthy, get_skv_config_manager
from skv_admin_api import SkvAdminApi
from recipes.ops.move_primary_runner import move_primary


SKV_DISK_DIR_TYPE = {
    'skv_offline': 'random_dir',
    'skv_online': 'online_random_dir'
}


def get_module_name(params):
    return params['module_params']['module_name']


def get_role_host_list(params, role):
    if role in (SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME):
        return params['cluster_node_info'][role].get('nodes')
    raise Exception('get hosts error, role[%s], role must be %s or %s!' % (role, SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME))


def get_role_port(params, role):
    if SKV_META_SERVER_ROLE_NAME == role:
        return params['cluster_port_info']['meta_server_ports']['server_port'].get('port')
    if SKV_REPLICA_SERVER_ROLE_NAME == role:
        return params['cluster_port_info']['replica_server_ports']['server_port'].get('port')
    raise Exception('get port error, role[%s], role must be %s or %s!' % (role, SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME))


def get_addr(params, role, host=None):
    """返回host:port形式的endpoint"""
    port = get_role_port(params, role)
    if host:
        return socket.gethostbyname(host) + ':' + str(port)
    else:
        return socket.gethostbyname(socket.getfqdn()) + ':' + str(port)


def get_role_addr_list(params, role, host_list):
    addr_list = []
    for host in host_list:
        addr_list.append(get_addr(params, role, host))
    return addr_list


def init_context(context, fun_desc=None):
    MyContext = type('MyContext', (object,), {})
    context_obj = MyContext()
    context_obj.params = context.get_params()  # 获取 param.json 的内容
    context_obj.module_name = get_module_name(context_obj.params)  # 获取模块名 skv_online or skv_offline
    context_obj.logger = context.get_logger()  # 获取 logger 实例,日志在执行机器上的 /sensorsdata/main/logs/mothership/shim_callback.log
    context_obj.scale_role = context.get_scale_role()  # 获取扩缩容的角色
    context_obj.exec_role = context.get_scale_exec_role()  # 获取当前执行 shim 的角色
    context_obj.scale_hosts = context.get_scale_hosts()  # 获取扩缩容的机器列表
    context_obj.scale_addr_list = get_role_addr_list(context_obj.params, context_obj.scale_role, context_obj.scale_hosts)  # 获取老集群 meta server 的 ip:host 列表
    context_obj.skv_cluster_type = get_skv_cluster_type(context_obj.module_name)  # 获取老集群类型
    context_obj.meta_server_host_list = get_role_host_list(context_obj.params, SKV_META_SERVER_ROLE_NAME)  # 获取老集群 meta server 的 所在节点的域名列表
    context_obj.meta_server_addr_list = get_role_addr_list(context_obj.params, SKV_META_SERVER_ROLE_NAME, context_obj.meta_server_host_list)  # 获取老集群 meta server 的 ip:host 列表
    context_obj.replica_server_host_list = get_role_host_list(context_obj.params, SKV_REPLICA_SERVER_ROLE_NAME)  # 获取老集群 replica server 的 所在节点的域名列表
    context_obj.replica_server_addr_list = get_role_addr_list(context_obj.params, SKV_REPLICA_SERVER_ROLE_NAME, context_obj.replica_server_host_list)   # 获取老集群 replica server 的 ip:host 列表
    context_obj.api = SkvAdminApi(context_obj.logger, context_obj.module_name)
    context_obj.logger.info('%s : %s' % (context_obj.module_name, fun_desc))
    return context_obj


def pre_scale_up_check(context):
    this = init_context(context, 'pre_scale_up_check')
    # 判断是否为标准环境，目前通过获取 replica_server 的个数来判断
    # 非标准环境不让扩缩容
    if SKVClusterType.GE_THREE_NODE != this.skv_cluster_type:
        raise Exception('%s unsppport scale up on skv %s cluster environment!' % (this.module_name, this.skv_cluster_type))
    # 这里防御性检查一下老 meta server 的个数
    # 这里不确定sp2->sp2.1怎么过渡升的，如果能升上来，那么存在两节点集群，这里就先抛异常吧
    meta_server_num = len(this.meta_server_host_list)
    if 1 != meta_server_num and meta_server_num != 3:
        raise Exception('role[%s] num is %d, usually 1 or 3, please check!' % (SKV_META_SERVER_ROLE_NAME, meta_server_num))
    if SKV_META_SERVER_ROLE_NAME == this.scale_role:
        this.logger.info("%s unsppport scale up" % SKV_META_SERVER_ROLE_NAME)
    if SKV_REPLICA_SERVER_ROLE_NAME == this.scale_role:
        # 这里防御性检查一下老 replica server 的个数, 实际上应该检查表的副本数
        replica_server_num = len(this.replica_server_host_list)
        if replica_server_num < 3:
            raise Exception('role[%s] num is %d, but at least 3, please check!' % (SKV_REPLICA_SERVER_ROLE_NAME, replica_server_num))
        # 新增机器资源检查
        # 1. 新增机器内存检查，新增机器与每台机器之间内存差不得大于新增机器内存的 10%
        # 2. 检查内存资源，新增 replica_server 机器的内存不得低于现有集群最低的 10%
        # 3. 检查磁盘资源，新增 replica_server 机器的 random 盘数量不得低于现有集群最低的，每块盘容量不得低于现有最低的 10%
        # wanghao todo
        pass


def pre_scale_up_start(context):
    init_context(context, 'pre_scale_up_start')
    # todo 配置更新, 问了一下貌似是云平台修改配置
    # 更新replica server list
    # saas环境判断检查
    # 更新配置组，写入zk
    # 更新skv client配置，主要是partition_factor，写入zk
    # 停 replica server
    # 创建skv目录， 会调用 install 接口？
    # todo


def post_scale_up_start(context):
    this = init_context(context, 'post_scale_up_start')
    # 检查新加节点状态是否为 live
    host_list = [get_addr(this.params, SKV_REPLICA_SERVER_ROLE_NAME, h) for h in this.scale_hosts]
    wait_replica_server(this.module_name, this.logger, host_list, 'ALIVE')
    # 配置检查
    # 1. 渲染skv shell config, todo
    # 2. 启动balance
    if this.skv_cluster_type == SKVClusterType.GE_THREE_NODE:
        balance_and_wait(this.module_name, this.logger)


def post_scale_up_check(context):
    this = init_context(context, 'post_scale_up_check')
    # 1. 渲染skv shell config
    # 2. 检查集群是否健康
    # if not check_health(logger, module_name):
    #     raise Exception('cluster not healthy!')
    # 3. 启动balance
    check_balance(this.module_name, this.logger)


def pre_scale_down_check(context):
    this = init_context(context, 'pre_scale_down_check')
    if SKVClusterType.GE_THREE_NODE != this.skv_cluster_type:
        raise Exception('%s unsppport scale down on skv %s cluster environment!' % (this.module_name, this.skv_cluster_type))
    if SKV_META_SERVER_ROLE_NAME == this.scale_role:
        raise Exception("%s unsppport scale up" % SKV_META_SERVER_ROLE_NAME)
    if SKV_REPLICA_SERVER_ROLE_NAME == this.scale_role:
        # 待下线节点上如果存在meta_server, 需要迁移 meta_server, meta 迁移暂时不支持
        meta_server_list_set = set(this.meta_server_host_list)
        res = meta_server_list_set.intersection(set(this.scale_hosts))
        if len(res) != 0:
            raise Exception('please transfer meta server before decommission on %s', res)
        # 配置标准化检查，sp2.0以上都已经是配置标准化的，这里不需要了
        # 缩容后集群replcia server个数不少于3个
        replica_server_num = len(this.replica_server_host_list)
        if replica_server_num < 3:
            raise Exception('role[%s] num is %d, but at least 3, please check' % (SKV_REPLICA_SERVER_ROLE_NAME, replica_server_num))
        # 检查待下节点上的表副本数是否允许缩容（避免导致表缺分片，或者直接把数据给下掉了，比如单分片表）
        if replica_server_num - len(this.scale_hosts) < 3:
            raise Exception('role[%s] num is %d, so only scale down %d %s at most, please check'
                            % (SKV_REPLICA_SERVER_ROLE_NAME, replica_server_num, (replica_server_num - 3), SKV_REPLICA_SERVER_ROLE_NAME))
        # 检查副本个数是否允许
        min_replica_count, max_replica_count = this.api.get_cluster_replica_count_range()
        if len(this.scale_hosts) >= min_replica_count:
            raise Exception('cannot decommission %d hosts because there are tables that only contains %d replica!' % (
                len(this.scale_hosts), min_replica_count))
        if replica_server_num - len(this.scale_hosts) < max_replica_count:
            raise Exception('cannot decommission %d hosts because there are tables that contains %d replica!' % (
                len(this.scale_hosts), max_replica_count))
        # 缩容前所有表必须为healthy的
        unhealthy_app_count = this.api.get_unhealthy_app_count()
        if unhealthy_app_count != 0:
            raise Exception('cannot decommission because there are %d unhealthy app!' % unhealthy_app_count)
        # 缩容期间不能有其他的分片迁移操作
        # 检查是否在进行 balance, 二者都会进行分片迁移，如果正在进行 balance 可能会导致 decommission 失效
        if this.api.META_LEVEL_LIVELY == this.api.get_meta_level() and not check_balance(this.module_name, this.logger):
            raise Exception('cannot decommission because the skv cluster is unbalanced!')


def decommission_callback(context):
    this = init_context(context, 'decommission_callback')
    if SKV_META_SERVER_ROLE_NAME == this.scale_role:
        raise Exception('unsupport horizontal scale down on %s!' % SKV_META_SERVER_ROLE_NAME)
    # 设置黑名单
    black_list = []
    for host in this.scale_hosts:
        black_list.append(get_addr(this.params, SKV_REPLICA_SERVER_ROLE_NAME, host))
    this.api.set_replica_server_black_list(','.join(black_list))
    # 将assign_delay_ms设为10，这样做的目的是让节点下线后，立即在其他节点上补充备份
    this.api.set_lb_assign_delay_ms(10)
    this.api.set_meta_level(this.api.META_LEVEL_STEADY)
    this.api.set_add_secondary_max_count_for_one_node('DEFAULT')
    for addr in black_list:
        # 把待下线节点上的primary副本全部挪走
        move_primary(this.module_name, this.logger, addr)
        # 挪走之后需要sleep 5s 等待所有客户端更新路由
        time.sleep(5)
        # 将待下线机器上的secondary replica转移走，使用downgrade_node是用来标记secondary为Inactive，从而触发数据转移
        inactive_replica(this.module_name, this.logger, addr)


def decommission_complete_inspection(context):
    this = init_context(context, 'decommission_complete_inspection')
    if SKV_META_SERVER_ROLE_NAME == this.scale_role:
        raise Exception('unsupport horizontal scale down on %s!' % SKV_META_SERVER_ROLE_NAME)
    decommission_addr = set(this.scale_addr_list)
    retry_times = 3
    while (len(decommission_addr) > 0 and retry_times > 0):
        for addr in decommission_addr.copy():
            count = this.api.get_primary_count_on_server(addr)
            if count == 0:
                this.logger.info('check decommission done for %s' % addr)
                decommission_addr.remove(addr)
        retry_times -= 1
    if (len(decommission_addr) != 0):
        raise Exception('check decommission undone for %s: still has %d primary replica' % (addr, count))


def post_scale_down_stop(context):
    this = init_context(context, 'post_scale_down_stop')
    wait_table_healthy(this.module_name, this.logger)
    # 检查已下掉的节点状态是否为 unlive
    host_list = [get_addr(this.params, SKV_REPLICA_SERVER_ROLE_NAME, h) for h in this.scale_hosts]
    wait_replica_server(this.module_name, this.logger, host_list, 'UNALIVE')
    # 重启meta server
    restart_all_meta_server(this.module_name, this.logger)


def post_scale_down_check(context):
    this = init_context(context, 'post_scale_down_check')
    # 2. 检查集群是否健康
    if not check_health(this.logger, this.module_name):
        raise Exception('cluster not healthy!')
    # 3. 启动balance
    if this.skv_cluster_type == SKVClusterType.GE_THREE_NODE:
        balance_and_wait(this.module_name, this.logger)


def get_data_dir_set_by_host(host, module_name, logger):
    skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, logger)
    group = skv_config_manager.get_config_group_by_host(host)
    data_dirs = skv_config_manager.get_config_value('replication', 'data_dirs', group).split(',')
    old_dir_set = {dir.split(':')[1] for dir in data_dirs}
    return old_dir_set


def get_slog_dir_by_host(host, module_name, logger):
    skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, logger)
    group = skv_config_manager.get_config_group_by_host(host)
    return skv_config_manager.get_config_value('replication', 'slog_dir', group)


def check_disk_type(module_name, dir_type):
    if module_name in SKV_DISK_DIR_TYPE.keys():
        assert dir_type == SKV_DISK_DIR_TYPE[module_name], '%s is usually place on %s type disk not %s' % (module_name, SKV_DISK_DIR_TYPE[module_name], dir_type)
    else:
        raise Exception("undefined module type: %s" % module_name)


def init_scale_disk_context(context, fun_desc=None):
    MyContext = type('MyContext', (object,), {})
    context_obj = MyContext()
    context_obj.params = context.get_params()  # 获取 param.json 的内容
    context_obj.module_name = get_module_name(context_obj.params)  # 获取模块名 skv_online or skv_offline
    context_obj.logger = context.get_logger()  # 获取 logger 实例
    context_obj.scale_host = context_obj.params['node_params'].get('hostname')
    context_obj.dir_type = context.get_change_dir_type()
    context_obj.change_dirs = context.get_change_dirs()
    context_obj.replica_server_ip_addr = get_addr(context_obj.params, SKV_REPLICA_SERVER_ROLE_NAME, context_obj.scale_host)
    context_obj.api = SkvAdminApi(context_obj.logger, context_obj.module_name)
    context_obj.logger.info('%s : %s' % (context_obj.module_name, fun_desc))
    return context_obj


def pre_add_dirs_check(context):
    """
    PRE_ADD_DIRS_CHECK 步骤回调函数
    加减盘接入文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=306393307
    context 说明文档：https://doc.sensorsdata.cn/pages/viewpage.action?pageId=272759515

    :param context: 上下文实例，可用于获取 params.json 等，支持的方法见 context 说明文档
    :return:
    """
    this = init_scale_disk_context(context, 'pre_add_dirs_check')
    # 1. 检查 skv 是否健康
    if not check_health(this.logger, this.module_name):
        raise Exception('skv cluster not healthy!')
    # 2. 不支持单机
    if get_skv_cluster_type(this.module_name) == SKVClusterType.ONE_NODE:
        raise Exception("simplified skv cluster not support add disk")
    # 3. 检查磁盘类型
    check_disk_type(this.module_name, this.dir_type)
    # 4. 获取已有的 data_dirs, 并检查是否冲突
    old_dir_set = get_data_dir_set_by_host(this.scale_host, this.module_name, this.logger)
    new_dir_set = {os.path.join(dir, this.module_name) for dir in this.change_dirs}
    if not old_dir_set.isdisjoint(new_dir_set):
        raise Exception('data_dir conflict: old data_dirs {} VS new data_dir {}.'.format(old_dir_set, new_dir_set))
    # 4. 检查新盘skv模块是否为空
    for new_dir in new_dir_set:
        if os.path.isdir(new_dir):
            raise Exception('path {} is exist, please check!'.format(new_dir))


def pre_add_dirs(context):
    # 由云平台来控制停掉当前加节点的replica_server
    this = init_scale_disk_context(context, 'pre_add_dirs')
    # 会重启，所以这里会先执行 move_primary
    this.api.set_meta_level(this.api.META_LEVEL_STEADY)
    # 1. 禁掉meta server的add secondary操作
    this.api.set_add_secondary_max_count_for_one_node(0)
    # 2. 将replica server上的primary replica迁走
    move_primary(this.module_name, this.logger, this.replica_server_ip_addr)
    # 3. meta server的add secondary操作设置为
    this.api.set_add_secondary_max_count_for_one_node('DEFAULT')


def add_dirs(context):
    # 在 pre_add_dirs 和 add_dirs 两步间captain 会停掉当前加减节点上的replica, 并完成了配置的更新
    # 所以这一步检查配置是否正确以及状态是否为 UNALIVE
    this = init_scale_disk_context(context, 'add_dirs')
    wait_replica_server(this.module_name, this.logger, [this.replica_server_ip_addr], 'UNALIVE')
    new_dir_set = {os.path.join(dir, this.module_name) for dir in this.change_dirs}
    after_add_data_dir_set = get_data_dir_set_by_host(this.scale_host, this.module_name, this.logger)
    if not new_dir_set.issubset(after_add_data_dir_set):
        raise Exception("new disks {} not in data_dirs {}, please check!".format(new_dir_set, after_add_data_dir_set))


def post_add_dirs(context):
    # 在这一步前会将 skv 拉起来, 这一步检查状态是否为 ALIVE
    this = init_scale_disk_context(context, 'post_add_dirs')
    wait_replica_server(this.module_name, this.logger, [this.replica_server_ip_addr], 'ALIVE')


def post_add_dirs_check(context):
    # 这一步将会检查集群是否健康，以及 balance 操作
    this = init_scale_disk_context(context, 'post_add_dirs_check')
    # 1. 检查集群健康
    if not check_health(this.logger, this.module_name):
        raise Exception('skv cluster not healthy!')
    # 2. 获取集群类型,并执行 balance
    skv_cluster_type = get_skv_cluster_type(this.module_name)
    if SKVClusterType.GE_THREE_NODE == skv_cluster_type:
        balance_and_wait(this.module_name, this.logger)


def pre_remove_dirs_check(context):
    this = init_scale_disk_context(context, 'pre_remove_dirs_check')
    # 1. 检查服务是否正常. -> scale_node
    if not check_health(this.logger, this.module_name):
        raise Exception('skv cluster not healthy!')
    # 2. 不支持单机
    if get_skv_cluster_type(this.module_name) == SKVClusterType.ONE_NODE:
        raise Exception("simplified skv cluster not support add disk")
    # 3. 检查是否存在单副本表
    this.api.check_all_avaliable_table_replica_count(3)
    # 4. 获取已有的data_dirs, 并检查是否存在
    old_data_dir_set = get_data_dir_set_by_host(this.scale_host, this.module_name, this.logger)
    remove_dir_set = {os.path.join(dir, this.module_name) for dir in this.change_dirs}
    if not remove_dir_set.issubset(old_data_dir_set):
        raise Exception("remove disks {} not in data_dirs {}, please check!".format(remove_dir_set, old_data_dir_set))
    # 5. 不能是slog
    slog_dir = get_slog_dir_by_host(this.scale_host, this.module_name, this.logger)
    if slog_dir in remove_dir_set:
        raise Exception("slog {} can not remove, please check!".format(slog_dir))
    # 6. 检查 reps下是否为空
    for dir in remove_dir_set:
        ret = check_output('ls %s' % os.path.join(dir, 'replica', 'reps'), this.logger.info)
        if len(ret) != 0:
            raise Exception('%s delete dir %s not empty!!!' % (this.scale_host, dir))
    # 7. 检查是否 remove 掉所有盘
    if not len(old_data_dir_set.difference(remove_dir_set)):
        raise Exception('remove dirs {} contains all old data_dirs {}!!!'.format(remove_dir_set, old_data_dir_set))


def pre_remove_dirs(context):
    # 由云平台来控制停掉当前减节点的replica_server
    this = init_scale_disk_context(context, 'pre_remove_dirs')
    # 会重启，所以这里会先执行 move_primary
    this.api.set_meta_level(this.api.META_LEVEL_STEADY)
    # 1. 禁掉meta server的add secondary操作
    this.api.set_add_secondary_max_count_for_one_node(0)
    # 2. 将replica server上的primary replica迁走
    move_primary(this.module_name, this.logger, this.replica_server_ip_addr)
    # 3. meta server的add secondary操作设置为
    this.api.set_add_secondary_max_count_for_one_node('DEFAULT')


def remove_dirs(context):
    # 在 pre_remove_dirs 和 remove_dirs 两步间 captain 会停掉当前减节点上的replica,并完成了配置的更新
    # 所以这一步检查配置是否正确以及状态是否为 UNALIVE
    this = init_scale_disk_context(context, 'remove_dirs')
    wait_replica_server(this.module_name, this.logger, [this.replica_server_ip_addr], 'UNALIVE')
    remove_dir_set = {os.path.join(dir, this.module_name) for dir in this.change_dirs}
    after_remove_data_dir_set = get_data_dir_set_by_host(this.scale_host, this.module_name, this.logger)
    if remove_dir_set.issubset(after_remove_data_dir_set):
        raise Exception("remove disks {} still exists in new data_dirs {}, please check!".format(remove_dir_set, after_remove_data_dir_set))


def post_remove_dirs(context):
    # 在这一步前会将 skv 拉起来, 这一步检查状态是否为 ALIVE
    this = init_scale_disk_context(context, 'post_remove_dirs')
    wait_replica_server(this.module_name, this.logger, [this.replica_server_ip_addr], 'ALIVE')


def post_remove_dirs_check(context):
    # 这一步将会检查集群是否健康，以及 balance 操作
    this = init_scale_disk_context(context, 'post_remove_dirs_check')
    # 1. 检查集群健康
    if not check_health(this.logger, this.module_name):
        raise Exception('skv cluster not healthy!')
    # 2. 获取集群类型,并执行 balance
    skv_cluster_type = get_skv_cluster_type(this.module_name)
    if SKVClusterType.GE_THREE_NODE == skv_cluster_type:
        balance_and_wait(this.module_name, this.logger)
