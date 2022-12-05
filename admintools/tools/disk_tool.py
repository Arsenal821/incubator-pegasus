#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv磁盘操作

skv磁盘操作的代码在 skv_tool.py 中过于冗长，所以拆分集合在该文件中
"""
import argparse
import datetime
import os
import random
import socket
import sys
import time

from hyperion_client.directory_info import DirectoryInfo
from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo
from hyperion_helper.hardware_info_helper import HardwareInfoHelper
from hyperion_utils import shell_utils
from hyperion_guidance.ssh_connector import SSHConnector

from recipes import safely_restart_replica_server, check_health, get_skv_config_manager, balance_and_wait, nonstandard_balance
from recipes.platform_adapter import _update_client_conf
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME, SKV_PRODUCT_NAME, SKV_MODULE_NAME_LIST, get_skv_cluster_type, \
    SKVClusterType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool
sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_admin_api import SkvAdminApi


class DiskTool(BaseTool):
    example_doc = '''
# automatically add random disk for group_1
skvadmin disk add -m skv_offline -g group_1
# local disk balance, will generate a script to run
skvadmin disk balance -m skv_offline --dir_tag_capacity_ratio data0:data1=2:1
# remove data1 for group_1, will generate a script to run
skvadmin disk delete -m skv_offline --delete_dir_tags data1 -g group_1
'''

    def init_parser(self, subparser):
        disk_subparsers = subparser.add_subparsers(dest='disk_operation')

        disk_add_subparser = disk_subparsers.add_parser(
            'add',
            help='add random_disk for skv')
        disk_add_subparser.add_argument('-m', '--module',
                                        type=str, required=True, choices=SKV_MODULE_NAME_LIST,
                                        help='module name, skv_offline/skv_online')
        disk_add_subparser.add_argument('-g', '--group',
                                        type=str, required=False, default='all',
                                        help='skv config group')
        disk_add_subparser.add_argument('--add_dirs',
                                        type=str, help=argparse.SUPPRESS)

        disk_delete_subparser = disk_subparsers.add_parser(
            'delete',
            help='delete random_disk for skv')
        disk_delete_subparser.add_argument('-m', '--module',
                                           type=str, required=True, choices=SKV_MODULE_NAME_LIST,
                                           help='module name, skv_offline/skv_online')
        disk_delete_subparser.add_argument('-g', '--group',
                                           type=str, required=False, default='all',
                                           help='skv config group')
        disk_delete_subparser.add_argument('--delete_dir_tags',
                                           type=str, required=True,
                                           help='The decrement of dir_tags, comma-separated')

        disk_balance_subparser = disk_subparsers.add_parser(
            'balance',
            help='balance skv disk data for skv')
        disk_balance_subparser.add_argument('-m', '--module',
                                            type=str, required=True, choices=SKV_MODULE_NAME_LIST,
                                            help='module name, skv_offline/skv_online')
        disk_balance_subparser.add_argument('--dir_tag_capacity_ratio',
                                            type=str,
                                            help='The ratio of data_dirs capacity, defalut 1:1 ..., '
                                                 'e.g. data0:data1:data2=1:2:2')

        self.parser = subparser

    def do(self, args):
        if args.disk_operation == 'add':
            return self._add_disk(args.module, args.group, args.add_dirs)
        elif args.disk_operation == 'delete':
            return self._delete_disk(args.module, args.group, args.delete_dir_tags)
        elif args.disk_operation == 'balance':
            return self._balance_disk(args.module, args.dir_tag_capacity_ratio)
        else:
            self.parser.print_help()

    def __check_server_disk_related_conf(self, module):
        """检查当前模块的server conf replica group disk相关配置是否合法"""
        skv_config_manager = get_skv_config_manager(module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        for g in skv_config_manager.get_config_groups():
            skv_config_manager.get_config_value('replication', 'data_dirs', g)

    def _add_disk(self, module, group, add_dirs):
        """replica_server 加盘操作
参数：
module: skv_offline/skv_online
group: 要减盘的 replica_server group, default='all'
add_dirs: 可以指定加盘路径
        """
        self.__check_server_disk_related_conf(module)
        if not check_health(self.logger, module):
            raise Exception('%s not healthy, not support disk operators!' % module)
        hosts = self.add_dir_in_conf_and_return_hosts(group, add_dirs, module)
        # 重启 replica_server
        self.logger.info('start to restart %d replica server in %s' % (len(hosts), group))
        for i, h in enumerate(hosts):
            skv_config_manager = get_skv_config_manager(module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
            replica_addr = '%s:%d' % (h, skv_config_manager.get_default_port())
            self.logger.info('%d/%d restart %s' % (i + 1, len(hosts), h))
            safely_restart_replica_server(module, self.logger, replica_addr)
        # 重启后立即的 balance 无效，所以等待 10s
        time.sleep(10)
        cluster_type = get_skv_cluster_type(module)
        if cluster_type == SKVClusterType.TWO_NODE:
            nonstandard_balance(module, self.logger)
        else:
            balance_and_wait(module, self.logger)

    def _delete_disk(self, module, group, delete_dir_tags):
        """replica_server 减盘操作
参数：
module: skv_offline/skv_online
group: 要减盘的 replica_server group, default='all'
delete_dir_tags: 要减掉的 dir tags，逗号分隔
        """
        self.__check_server_disk_related_conf(module)
        if not check_health(self.logger, module):
            raise Exception('%s not healthy, not support disk operators!' % module)
        self.check_and_generate_delete_dir_operation_file(group, delete_dir_tags, module)

    def _balance_disk(self, module, dir_tag_capacity_ratio):
        """均衡单个节点的磁盘间的数据
参数：
module: skv_offline/skv_online
dir_tag_capacity_ratio: 指定 data_dirs 中路径的负载数据比例(不填默认为1) e.g. data0:data1:data2=2:1:1
        """
        if not check_health(self.logger, module):
            raise Exception('%s not healthy, not support disk operators!' % module)

        dir_ratio_dict, dir_capacity, dir_capacity_dict = self.get_dir_detail_dict(dir_tag_capacity_ratio, module)
        balance_disk_operation = self.calculate_balance_disk_operation(dir_ratio_dict, dir_capacity, dir_capacity_dict)
        self.generate_operation_file(balance_disk_operation, module)

    def _disk_op_common_check(self, hosts, data_dirs):
        """replica_server data_dirs 的配置中没有多个路径对应一块盘的情景"""
        if socket.getfqdn() not in set(hosts):
            raise Exception('please execute on one of %s!' % str(hosts))
        hardware_info = HardwareInfoHelper(self.logger)
        mount_points = set()
        for d in data_dirs.split(','):
            mount_points.add(hardware_info.get_mount_point_by_path(d.split(':')[1]))
        if len(data_dirs.split(',')) != len(mount_points):
            raise Exception("some mount_point exist more than 1 path in data_dirs, please check it!"
                            " mount_points:%s data_dirs:%s" % (str(mount_points), str(data_dirs)))

    def add_dir_in_conf_and_return_hosts(self, group, add_dirs, module_name):
        """加盘修改 replica_server data_dirs
参数：
module: skv_offline/skv_online
group: 要加盘的 replica_server group
add_dirs: 可以指定加盘路径
        """
        # 单机检查
        if get_skv_cluster_type(module_name) == SKVClusterType.ONE_NODE:
            raise Exception("simplified skv cluster not support add disk")
        skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        groups = skv_config_manager.get_config_groups()
        if group == 'all':
            if len(groups) != 1:
                raise Exception("All replica_server not in the same config group!")
            group = groups[0]
        elif group not in groups:
            raise Exception("%s not in %s replica_server config group" % (group, module_name))

        data_dirs = skv_config_manager.get_config_value('replication', 'data_dirs', group)
        hosts = skv_config_manager.get_config_group_hosts(group)

        self._disk_op_common_check(hosts, data_dirs)

        hardware_info = HardwareInfoHelper(self.logger)
        old_random_storage_dir = set()
        max_tag_index = 0
        for d in data_dirs.split(','):
            # 获得 data_dirs 每个路径的挂载路径
            old_random_storage_dir.add(hardware_info.get_mount_point_by_path(d.split(':')[1]))
            tag_index = int(d.split(':')[0][4:])
            if (tag_index > max_tag_index):
                max_tag_index = tag_index

        new_dirs = ""
        if add_dirs:
            new_dirs = add_dirs
        else:
            disk_type = "random" if module_name == 'skv_offline' else "online_random"
            # 获得每个 host 的 disk_type 挂载点
            host_to_random_storage_dir = {
                h: tuple(sorted(InnerNodeInfo.get_instance().get_storage_dir_path_by_type(h, disk_type))) for h in hosts
            }
            self.logger.debug('host_to_random_storage_dir: %s' % str(host_to_random_storage_dir))
            # 同组 host 的挂载点不一致，说明机器磁盘资源不一致
            if len(set(host_to_random_storage_dir.values())) > 1:
                self.logger.error('host_to_random_storage_dir: %s' % str(host_to_random_storage_dir))
                raise Exception('host_to_random_storage_dir is different!')

            new_random_storage_dir = set()
            for _dir in host_to_random_storage_dir[hosts[0]]:
                # 获得新的 random 盘的的挂载路径
                new_random_storage_dir.add(hardware_info.get_mount_point_by_path(_dir))
            # 加盘前的挂载点必须是加盘后的子集
            if not old_random_storage_dir.issubset(new_random_storage_dir):
                self.logger.error('now data_dirs in config is %s, it\'s random storage_dir is %s' % (data_dirs, str(host_to_random_storage_dir)))
                self.logger.error('new random storage_dir is %s' % str(new_random_storage_dir))
                raise Exception('old_random_storage_dir is not subset of new_random_storage_dir')
            if new_random_storage_dir == old_random_storage_dir:
                self.logger.error('now data_dirs in config is %s, it\'s random storage_dir is %s' % (data_dirs, str(host_to_random_storage_dir)))
                self.logger.error('new random storage_dir is %s' % str(new_random_storage_dir))
                raise Exception("No new dir can be add in random disk, please check it!")

            for mount_point in new_random_storage_dir.difference(old_random_storage_dir):
                new_dirs = '%s,%s' % (new_dirs, os.path.join(mount_point, module_name)) if new_dirs else os.path.join(mount_point, module_name)

        # 在 hosts 里将新建目录创建
        for _dir in new_dirs.split(','):
            for h in hosts:
                ssh_client = SSHConnector.get_instance(h)
                cmd = "test -d %s" % _dir
                if ssh_client.run_cmd(cmd, self.logger.debug)['ret'] == 0:
                    ssh_client.check_call("mv %s %s_%s" % (_dir, _dir, datetime.datetime.now().strftime('%Y%m%d-%H%M%S')), self.logger.debug)
                ssh_client.check_call("mkdir %s" % _dir, self.logger.debug)
            max_tag_index += 1
            data_dirs = '%s,data%d:%s' % (data_dirs, max_tag_index, _dir)

        skv_config_manager.set_config_value('replication', 'data_dirs', data_dirs, group)

        # 修改client_conf的partition_factor参数
        partition_factor = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)._calc_partition_factor()
        _update_client_conf(module_name, 'partition_factor', partition_factor)
        return hosts

    def check_and_generate_delete_dir_operation_file(self, group, delete_dir_tags, module_name):
        """减盘修改 replica_server data_dirs
参数：
module: skv_offline/skv_online
group: 要减盘的 replica_server group
delete_dir_tags: 要减掉的 dir tags，逗号分隔
               """
        # 单机检查
        if get_skv_cluster_type(module_name) == SKVClusterType.ONE_NODE:
            raise Exception("simplified skv cluster not support delete disk")
        # 检查 replica_count >= 3
        SkvAdminApi(self.logger, module_name).check_all_avaliable_table_replica_count(3)

        skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        groups = skv_config_manager.get_config_groups()
        if group == 'all':
            if len(groups) != 1:
                raise Exception("All replica_server not in the same config group!")
            group = groups[0]
        elif group not in groups:
            raise Exception("%s not in %s replica_server config group" % (group, module_name))

        data_dirs = skv_config_manager.get_config_value('replication', 'data_dirs', group)
        hosts = skv_config_manager.get_config_group_hosts(group)

        self._disk_op_common_check(hosts, data_dirs)
        slog_dir = skv_config_manager.get_config_value('replication', 'slog_dir', group)
        slog_dir_tag = ''
        # dir_dict = {'data0': '/xxx0', 'data1': '/xxx1'}
        dir_dict = {}
        for d in data_dirs.split(','):
            dir_dict[d.split(':')[0]] = d.split(':')[1]
            if slog_dir == d.split(':')[1]:
                slog_dir_tag = d.split(':')[0]

        delete_tags_set = set(delete_dir_tags.split(','))
        # 判断被删除的 dir 不能为 slog_dir
        if slog_dir_tag in delete_tags_set:
            raise Exception('slog_dir cannot remove from data_dirs!')

        current_tags_set = set(dir_dict)
        # 检查传入的 tag 是否是当前 tags 的子集
        if len(delete_tags_set.difference(current_tags_set)) != 0:
            raise Exception('arg --delete_dir_tags %s error, current data_dirs tags %s, dirs %s' % (
                delete_dir_tags, str(current_tags_set), str(dir_dict)))
        # 检查减掉的盘是否为空
        for h in hosts:
            ssh_client = SSHConnector.get_instance(h)
            for tag in delete_tags_set:
                ret = ssh_client.check_output('ls %s' % os.path.join(dir_dict[tag], 'replica', 'reps'), self.logger.debug)
                if len(ret) != 0:
                    raise Exception('%s delete tag %s dir %s not empty!!!' % (h, tag, dir_dict[tag]))

        new_tags_set = current_tags_set.difference(delete_tags_set)

        new_data_dirs = ''
        for t in new_tags_set:
            new_data_dirs = "%s:%s" % (t, dir_dict[t]) \
                if new_data_dirs == '' else "%s,%s:%s" % (new_data_dirs, t, dir_dict[t])
        self.logger.debug("delete dir_tag %s in %s" % (delete_dir_tags, str(hosts)))

        # 计算新的分区因子 修改配置
        data_dirs = skv_config_manager.get_config_value('replication', 'slog_dir', group)
        new_partition_factor = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)._calc_partition_factor()

        """生成减盘执行文件"""
        file_name = os.path.join(DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME), '%s_disk_delete_operation_%s.sh' % (
            module_name, datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
        with open(file_name, 'w+') as f:
            f.write('#!/bin/bash\nset -e\nset -x\n\n')
            f.write('skvadmin config set -m %s -r replica_server -s replication -n data_dirs -g %s -v %s -y\n' % (
                module_name, group, new_data_dirs))
            for h in hosts:
                f.write('skvadmin single_replica_server safe_restart -m %s --host %s\n' % (module_name, h))
            f.write("spadmin config set client -p skv -r replica_server -m %s -n partition_factor -v %d\n" % (module_name, new_partition_factor))
            # 重启后立即的 balance 无效，故等待 10s
            f.write('sleep 10s\n')
            f.write('skvadmin balance start -m %s' % module_name)
        self.logger.debug("generate %s" % file_name)
        self.logger.info("please execute [sh %s] to balance disk data." % file_name)

    def get_dir_detail_dict(self, dir_tag_capacity_ratio, module_name):
        """检查传入的 dir_tag_capacity_ratio 是否合规, 并返回计算均衡策略所需的 dict
参数：
dir_tag_capacity_ratio: 指定 data_dirs 中路径的负载数据比例(不填默认为1) e.g. data0:data1:data2=2:1:1
return:
dir_ratio_dict: data_dirs 中路径均衡目标大小比
dir_capacity: data_dirs 中路径的实际大小比
dir_capacity_dict: data_dirs 中每个目录所含的 reps 的文件大小拓扑（单位MB）
        """
        host = socket.getfqdn()
        skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        config_group = skv_config_manager.get_config_group_by_host(host)
        data_dirs = skv_config_manager.get_config_value('replication', 'data_dirs', config_group)
        self._disk_op_common_check([host], data_dirs)

        # dir_ratio_dict {dir_name: ratio_num}
        dir_ratio_dict = {}

        if dir_tag_capacity_ratio:
            # 检查传参 dir_tag_capacity_ratio 是否合规
            dir_tag_list = dir_tag_capacity_ratio.split('=')[0].split(':')
            ratio_list = []
            for num in dir_tag_capacity_ratio.split('=')[1].split(':'):
                ratio_list.append(int(num))
            # 检查传入的 dir_tag_capacity_ratio 的 dir_tag 和 ratio 长度是否对应
            if len(dir_tag_list) != len(ratio_list):
                raise Exception('dir_tag_capacity_ratio error, dir_tag_list: %s, ratio_list: %s' % (
                    str(dir_tag_list), str(ratio_list)
                ))
            # 检查传入的 dir_tag 和 data_dirs 中的 tag 是否一致
            dir_tag_set = set()
            for d in data_dirs.split(','):
                dir_tag_set.add(d.split(':')[0])
            if dir_tag_set != set(dir_tag_list):
                raise Exception('dir_tag_capacity_ratio error, data_tag_set: %s, dir_tag_capacity_ratio: %s' % (
                    str(dir_tag_set), str(dir_tag_list)
                ))

            # tag_ratio_dict {dir_tag: ratio_num}
            tag_ratio_dict = dict(zip(dir_tag_list, ratio_list))
            for d in data_dirs.split(','):
                dir_ratio_dict[os.path.join(d.split(':')[1], 'replica', 'reps')] = tag_ratio_dict[d.split(':')[0]]
        else:
            for d in data_dirs.split(','):
                dir_ratio_dict[os.path.join(d.split(':')[1], 'replica', 'reps')] = 1
        self.logger.debug("balance dir_ratio: %s" % str(dir_ratio_dict))

        # dir_capacity {dir_name: capacity(MB)}
        dir_capacity = {}
        for _dir in dir_ratio_dict:
            output = shell_utils.check_output("du -sm %s" % _dir, self.logger.debug)
            dir_capacity[_dir] = int(output.split()[0])

        # dir_capacity {dir_name: sub_reps_dir{replica: capacity(MB)}}
        dir_capacity_dict = {}
        for _dir in dir_ratio_dict:
            dir_capacity_dict[_dir] = {}
            for rep in shell_utils.check_output("ls %s" % _dir, self.logger.debug).split():
                dir_capacity_dict[_dir][rep] = int(shell_utils.check_output("du -sm %s" % os.path.join(_dir, rep), self.logger.debug).split()[0])

        return dir_ratio_dict, dir_capacity, dir_capacity_dict

    def add_move_replica_dir(self, from_reps_dir, to_reps_dir, cap, dir_capacity_dict):
        move_operation_list = []
        reps_list = list(dir_capacity_dict)
        random.shuffle(reps_list)
        sum_cap = 0
        for rep in reps_list:
            if sum_cap + dir_capacity_dict[rep] < cap:
                sum_cap += dir_capacity_dict[rep]
                dir_capacity_dict.pop(rep)
                move_operation_list.append((os.path.join(from_reps_dir, rep), os.path.join(to_reps_dir, rep)))
            else:
                break
        return move_operation_list

    def calculate_balance_disk_operation(self, dir_ratio_dict, dir_capacity, dir_capacity_dict):
        """计算均衡磁盘需要的 replica 移动操作
参数：
dir_ratio_dict: data_dirs 中路径的负载数据比例 dict，由 disk_operator.check_and_return_dir_ratio_dict() 得出
dir_capacity: data_dirs 中路径的实际大小比例 dict, 由 disk_operator.check_and_return_dir_ratio_dict() 得出
dir_capacity_dict: data_dirs 中每个目录所含的 reps 的文件大小拓扑, 由 disk_operator.check_and_return_dir_ratio_dict() 得出
        """
        sum_ratio_num = 0
        sum_capacity = 0
        for _dir in dir_capacity:
            sum_capacity += dir_capacity[_dir]
        for _dir in dir_ratio_dict:
            sum_ratio_num += dir_ratio_dict[_dir]

        self.logger.debug("before move, the sum of capacity(MB) %d, current data_dirs capacity(MB) %s" % (
            sum_capacity, str(dir_capacity)))

        # 计算 dir 当前大小和目标值大小的差值
        single_capacity = int(sum_capacity / sum_ratio_num)
        dir_gap_dict = {}
        for _dir in dir_capacity:
            goal = int(single_capacity * dir_ratio_dict[_dir])
            self.logger.debug('%s target capacity(MB) is %d' % (_dir, goal))
            dir_gap_dict[_dir] = dir_capacity[_dir] - goal

        # 按照 value 排序 capacity
        dir_gap_dict = dict(sorted(dir_gap_dict.items(), key=lambda x: x[1], reverse=True))
        self.logger.debug("target gap capacity(MB) %s" % str(dir_gap_dict))

        dir_list = list(dir_gap_dict.keys())
        from_index, to_index = 0, len(dir_list) - 1
        move_operation_list = []

        while from_index < to_index:
            if dir_gap_dict[dir_list[from_index]] + dir_gap_dict[dir_list[to_index]] > 0:
                dir_gap_dict[dir_list[from_index]] += dir_gap_dict[dir_list[to_index]]
                move_operation_list.extend(
                    self.add_move_replica_dir(
                        dir_list[from_index], dir_list[to_index], abs(dir_gap_dict[dir_list[to_index]]), dir_capacity_dict[dir_list[from_index]]))
                to_index -= 1
            else:
                dir_gap_dict[dir_list[to_index]] += dir_gap_dict[dir_list[from_index]]
                move_operation_list.extend(
                    self.add_move_replica_dir(
                        dir_list[from_index], dir_list[to_index], abs(dir_gap_dict[dir_list[from_index]]), dir_capacity_dict[dir_list[from_index]]))
                from_index += 1

        # 支持减盘，减 dir 移空
        from_dir_list = []
        to_dir = ""
        for _dir in dir_ratio_dict:
            if dir_ratio_dict[_dir] == 0:
                from_dir_list.append(_dir)
            else:
                to_dir = _dir
        for _dir in from_dir_list:
            for rep in list(dir_capacity_dict[_dir]):
                move_operation_list.append((os.path.join(_dir, rep), os.path.join(to_dir, rep)))
        return move_operation_list

    def generate_operation_file(self, move_operation_list, module_name):
        if len(move_operation_list) == 0:
            self.logger.info("All disk are balanced, nothing to be done.")
            return
        """生成 balance 执行文件"""
        file_name = os.path.join(DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME), '%s_disk_balance_operation_%s.sh' % (
            module_name, datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
        cluster_type = get_skv_cluster_type(module_name)
        with open(file_name, 'w+') as f:
            f.write('#!/bin/bash\nset -e\nset -x\n\n')
            if cluster_type == SKVClusterType.GE_THREE_NODE:
                f.write('skvadmin single_replica_server safe_stop -m %s --host %s\n' % (module_name, socket.getfqdn()))
            else:
                f.write('spadmin stop -m %s -r replica_server -p skv\n' % module_name)
            for from_dir, to_dir in move_operation_list:
                f.write("mkdir -p %s\n" % ("%s.err" % to_dir))
                f.write("cp -a %s %s\n" % (from_dir, ("%s.err" % to_dir)))
                f.write("mv %s %s\n" % (os.path.join(("%s.err" % to_dir), os.path.basename(from_dir)), os.path.dirname(to_dir)))
                f.write("mv %s %s\n" % (from_dir, ("%s.err" % from_dir)))
                f.write("rm -rf %s\n" % ("%s.err" % from_dir))
                f.write("rm -rf %s\n" % ("%s.err" % to_dir))
            if cluster_type == SKVClusterType.GE_THREE_NODE:
                f.write('skvadmin single_replica_server start_and_check -m %s --host %s\n' % (module_name, socket.getfqdn()))
                # 重启后立即的 balance 无效，故等待 10s
                f.write('sleep 10s\n')
                f.write('skvadmin balance start -m %s' % module_name)
            else:
                f.write('spadmin start -m %s -r replica_server -p skv\n' % module_name)
        self.logger.debug("generate %s" % file_name)
        self.logger.info("please execute [sh %s] to balance disk data." % file_name)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    operator = DiskTool()
    operator.logger = logging.getLogger()

    # test_1
    dir_ratio_dict = {'/data/test_1': 2, '/data/test_2': 1, '/data/test_3': 2}
    dir_capacity_dict = {'/data/test_1': {'1.0': 300, '2.0': 300, '3.1': 400, '3.2': 2000, '3.5': 1000, '4.1': 500,
                                          '4.3': 2500, '4.6': 800, '5.2': 1200, '6.7': 300, '7.9': 700},
                         '/data/test_2': {'3.3': 100, '1.6': 900, '4.2': 700, '6.2': 300},
                         '/data/test_3': {}}
    dir_cap_val = {}
    sum_cap = 0
    max_cap = 0
    for dir_cap in dir_capacity_dict:
        dir_cap_val[dir_cap] = 0
        for rep in dir_capacity_dict[dir_cap]:
            dir_cap_val[dir_cap] += dir_capacity_dict[dir_cap][rep]
            sum_cap += dir_capacity_dict[dir_cap][rep]
            max_cap = max(max_cap, dir_capacity_dict[dir_cap][rep])

    move_operation_list = operator.calculate_balance_disk_operation(dir_ratio_dict, dir_cap_val, dir_capacity_dict)

    for f, t in move_operation_list:
        from_dir = os.path.join('/', 'data', f.split('/')[2])
        to_dir = os.path.join('/', 'data', t.split('/')[2])
        dir_cap_val[from_dir] -= dir_capacity_dict[from_dir][f.split('/')[3]]
        dir_cap_val[to_dir] += dir_capacity_dict[from_dir][f.split('/')[3]]

    sing_data = sum_cap / len(dir_ratio_dict)
    assert abs(dir_cap_val['/data/test_2'] - sing_data) < max_cap
    assert abs(dir_cap_val['/data/test_3'] - 2 * sing_data) < max_cap
    print(move_operation_list)

    # test_2
    dir_ratio_dict = {'/data/test_1': 1, '/data/test_2': 1, '/data/test_3': 1}
    dir_capacity_dict = {'/data/test_1': {'1.0': 100, '2.0': 100, '3.1': 100, '3.2': 100, '3.5': 100, '4.1': 100,
                                          '4.3': 100, '4.6': 100, '5.2': 100, '6.7': 100, '7.9': 100},
                         '/data/test_2': {'3.3': 100, '1.6': 100, '4.2': 100, '6.2': 100},
                         '/data/test_3': {}}
    dir_cap_val = {}
    for dir_cap in dir_capacity_dict:
        dir_cap_val[dir_cap] = 0
        for rep in dir_capacity_dict[dir_cap]:
            dir_cap_val[dir_cap] += dir_capacity_dict[dir_cap][rep]

    move_operation_list = operator.calculate_balance_disk_operation(dir_ratio_dict, dir_cap_val, dir_capacity_dict)

    for f, t in move_operation_list:
        from_dir = os.path.join('/', 'data', f.split('/')[2])
        to_dir = os.path.join('/', 'data', t.split('/')[2])
        dir_cap_val[from_dir] -= dir_capacity_dict[from_dir][f.split('/')[3]]
        dir_cap_val[to_dir] += dir_capacity_dict[from_dir][f.split('/')[3]]

    assert dir_cap_val['/data/test_1'] == dir_cap_val['/data/test_2']
    assert dir_cap_val['/data/test_3'] == dir_cap_val['/data/test_2']

    # test_3
    dir_ratio_dict = {'/data/test_1': 1, '/data/test_2': 1, '/data/test_3': 1}
    dir_capacity_dict = {'/data/test_1': {'1.0': 100, '2.0': 100, '3.1': 100, '3.2': 100, '3.5': 100, '4.1': 100},
                         '/data/test_2': {'3.3': 100, '1.6': 100, '4.2': 100, '6.2': 100, '7.7': 100, '8.1': 100},
                         '/data/test_3': {'3.8': 100, '1.1': 100, '1.2': 100, '0.2': 100, '1.7': 100, '5.1': 100}}
    dir_cap_val = {}
    for dir_cap in dir_capacity_dict:
        dir_cap_val[dir_cap] = 0
        for rep in dir_capacity_dict[dir_cap]:
            dir_cap_val[dir_cap] += dir_capacity_dict[dir_cap][rep]

    move_operation_list = operator.calculate_balance_disk_operation(dir_ratio_dict, dir_cap_val, dir_capacity_dict)

    assert len(move_operation_list) == 0
