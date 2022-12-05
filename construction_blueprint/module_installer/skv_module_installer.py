# -*- coding: UTF-8 -*-

"""
Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)

by jinsilan 2/18 2022:
    sp 2.0对skv offline 的安装代码在sp里面, 如果只装了offline然后升级skv 2.0之后加online节点，对应的部署代码在skv里面
    sp 2.1对skv的安装代码在skv的shim里面
"""

import os
import socket
import sys
import time
import shutil

from construction_vehicle.module_installer.stateful_module_installer import StatefulModuleInstaller
from hyperion_element.global_properties import GlobalProperties
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo
from hyperion_client.deploy_topo import DeployTopo
from hyperion_client.config_manager import ConfigManager
from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_client.directory_info import DirectoryInfo
from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo
from hyperion_client.module_service import ModuleService

import utils.shell_wrapper

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import SKV_PRODUCT_NAME, SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME, fix_shell_config, \
    SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME, SKV_HOST_SPECIFIED_ITEMS, is_hubble_installed, get_installed_skv_modules
from skv_admin_api import SkvAdminApi
from recipes import balance_no_wait, check_balance, move_primary, inactive_replica, wait_replica_server, \
    restart_primary_meta_server, safely_stop_replica_server, safely_restart_replica_server, get_skv_config_manager

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'construction_blueprint'))
from skv_config_manager import generate_std_config,  \
    init_replica_server_group_config, init_meta_server_group_config

# 适配系数，解决同规格硬件实际容量上存在的的差异导致的误判
FIT_COEFFICIENT = 0.9
# saas标准化磁盘数最小值
SAAS_SKV_MIN_DISK_NUM = 4


class SkvModuleInstaller(StatefulModuleInstaller):
    """
    skv的部署
    """
    def __init__(self, logger):
        super().__init__(logger)

        self.my_ip = socket.gethostbyname(self.my_host)

        self._skv_role_map = {}
        self._skv_server_conf = {}
        self._skv_port_map = {}
        self._skv_start_command_map = {}
        self._skv_structure_version = None

        self._zookeeper_server_list = []

    @property
    def skv_role_map(self):
        if not self._skv_role_map:
            self._skv_role_map = self.roles_map[self.module_name]
        return self._skv_role_map

    @property
    def skv_server_conf(self):
        if self._skv_server_conf:
            return self._skv_server_conf

        self._skv_server_conf = self.module.get_conf_by_type('server_conf') or ConfigManager().get_server_conf(
            self.product_name, self.module_name)
        self.logger.info("skv_server_conf = " + str(self._skv_server_conf))
        return self._skv_server_conf

    def get_role_name_list_of_my_node(self):
        role_name_list = []

        for (role_name, server_list) in self.skv_role_map.items():
            for host_port in server_list:
                host_port_list = host_port.split(':')
                host = host_port_list[0]

                ip = socket.gethostbyname(host)
                if ip == self.my_ip:
                    role_name_list.append(role_name)

        return role_name_list

    @property
    def zookeeper_server_list(self):
        if self._zookeeper_server_list:
            return self._zookeeper_server_list

        self._zookeeper_server_list = GlobalProperties.get_instance().zookeeper.connect

        return self._zookeeper_server_list

    def install_module_progress(self):
        """
        得到实际的各个步骤
        :return:返回值是一个二元组
        """

        # 判断是否是混部 skv
        if (
                hasattr(self.runtime_conf, 'external_conf') and
                SKV_OFFLINE_MODULE_NAME in self.runtime_conf.external_conf
        ):
            return [
                ('Init zookeeper for skv', self.store_module),
                ('Render skv shell config', self._render_shell_and_dispatch),
            ]

        step_list = [
            ('Init zookeeper for skv', self.store_module),
            ('Stop all skv services if already started', self.stop_all_services),
            ('Deploy skv config file', self.deploy_configure_file),
            ('Make skv dirs', self.make_dirs),
            ('Start all skv services', self.start_all_services),
            ('Update replica client config', lambda: ConfigManager().set_client_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, 'major_version', '2.0')),
        ]
        return step_list

    def start_all_services(self):
        self.start_meta_server()
        self.start_replica_server()

    def start_meta_server(self):
        return self.start_service_by_role(SKV_META_SERVER_ROLE_NAME)

    def start_replica_server(self):
        return self.start_service_by_role(SKV_REPLICA_SERVER_ROLE_NAME)

    def start_service_by_role(self, role_name):
        """
        启动服务
        :param role_name: 启动服务的角色名称
        :return:
        """
        if role_name not in self.skv_role_map:
            return False

        for host_port in self.skv_role_map[role_name]:
            host_port_list = host_port.split(':')
            host = host_port_list[0]
            port = host_port_list[1]

            ip = socket.gethostbyname(host)
            if ip != self.my_ip:
                continue

            if self.check_alive_by_port(port):
                return False

            command = "cd ${SKV_HOME}/construction_blueprint && python3 skv_server.py start -m %s -r %s -i" % (
                self.module_name, role_name)
            utils.shell_wrapper.check_call(command, self.logger.info)
            return True

        return False

    def stop_all_services(self):
        """
        停止当前skv的所有服务
        """
        self.stop_replica_server()
        self.stop_meta_server()

    def stop_meta_server(self):
        return self.stop_service_by_role(SKV_META_SERVER_ROLE_NAME)

    def stop_replica_server(self):
        return self.stop_service_by_role(SKV_REPLICA_SERVER_ROLE_NAME)

    def stop_service_by_role(self, role_name):
        """
        停止服务
        :param role_name: 停止服务的角色名称
        :return:
        """
        if role_name not in self.roles_map[self.module_name]:
            return False

        port = get_skv_config_manager(self.module_name, role_name, self.logger).get_default_port()
        ret, pid, _ = self.get_pid_by_port(port)
        if ret != 0:
            self.logger.info(
                "{module_name} {role_name} on port {port} is not running.".format(
                    module_name=self.module_name, role_name=role_name, port=port
                )
            )
            return False

        self.logger.info(
            "{module_name} {role_name} on port {port} is running. Try to stop it.".format(
                module_name=self.module_name, role_name=role_name, port=port
            )
        )

        self.kill_py_pid(pid)
        # 等待端口不绑定
        while True:
            ret, pid, _ = self.get_pid_by_port(port)
            if ret != 0:
                self.logger.info(
                    "{module_name} {role_name} on port {port} is stopped.".format(
                        module_name=self.module_name, role_name=role_name, port=port
                    )
                )
                break
            self.logger.info(
                "{module_name} {role_name} on port {port} is still running..".format(
                    module_name=self.module_name, role_name=role_name, port=port
                )
            )
            time.sleep(1)

        return True

    def check_alive_by_port(self, port):
        ret, _, _ = self.get_pid_by_port(port)
        return ret == 0

    def get_pid_by_port(self, port):
        command = "lsof -t -i tcp:{port} -s tcp:LISTEN".format(port=port)
        result = utils.shell_wrapper.run_cmd(command, self.logger.info)
        stdout = result['stdout']
        if stdout is None:
            stdout = ''
        return result['ret'], stdout.strip(), result['stderr']

    def kill_py_pid(self, pid):
        command = "kill {pid}".format(pid=pid)
        utils.shell_wrapper.check_call(command, self.logger.info)

    def deploy_configure_file(self):
        role_name_list = self.get_role_name_list_of_my_node()
        for role_name in role_name_list:
            # 生成后不检查
            generate_std_config(self.module_name, role_name, self.skv_server_conf, None, self.logger)

        # 更新shell.ini
        # skv 2.0的安装逻辑 一定是在安装skv online 且skv offline一定已经安装完毕了
        if self.module_name != SKV_ONLINE_MODULE_NAME:
            raise Exception('unexpected module %s! should be %s!' % (self.module_name, SKV_ONLINE_MODULE_NAME))
        module_name_list = [SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME]
        offline_meta_server_list = ConfigManager().get_client_conf_by_key(SKV_PRODUCT_NAME, SKV_OFFLINE_MODULE_NAME, 'meta_server_list')
        online_meta_server_list = list()
        # fqdn 转 ip
        for element in self.roles_map[SKV_ONLINE_MODULE_NAME][SKV_META_SERVER_ROLE_NAME]:
            fqdn = element.split(':')[0]
            port = element.split(':')[1]
            ip = socket.gethostbyname(fqdn)
            online_meta_server_list.append(':'.join([ip, port]))

        meta_server_nodes_map = {
            SKV_OFFLINE_MODULE_NAME: offline_meta_server_list,
            SKV_ONLINE_MODULE_NAME: online_meta_server_list
        }
        dest_hosts = DeployTopo().get_all_host_list()
        fix_shell_config(module_name_list, meta_server_nodes_map, dest_hosts, self.logger)

    def render_shell_config(self):
        module_name_list = get_installed_skv_modules()
        meta_server_nodes_map = {
            module: ConfigManager().get_client_conf_by_key(SKV_PRODUCT_NAME, module, 'meta_server_list')
            for module in module_name_list}
        fix_shell_config(module_name_list, meta_server_nodes_map, None, self.logger)

    def _render_shell_and_dispatch(self):
        """生成shell 然后再分发到每台机器"""
        module_name_list = get_installed_skv_modules()
        meta_server_nodes_map = {
            module: ConfigManager().get_client_conf_by_key(SKV_PRODUCT_NAME, module, 'meta_server_list')
            for module in module_name_list}
        dest_hosts = DeployTopo().get_all_host_list()
        fix_shell_config(module_name_list, meta_server_nodes_map, dest_hosts, self.logger)

    def get_dir_to_make_list_by_role_list(self, role_name_list):
        dir_list = []
        for role_name in role_name_list:
            role_specfic_server_conf = self.skv_server_conf[SKV_HOST_SPECIFIED_ITEMS][role_name]
            self.logger.debug('%s specific server conf %s' % (role_name, role_specfic_server_conf))
            group_name = role_specfic_server_conf['hosts_to_groups'][self.my_host]
            dir_list.append(role_specfic_server_conf['group_config'][group_name]['core']['data_dir'])

        if SKV_REPLICA_SERVER_ROLE_NAME not in role_name_list:
            return dir_list

        replica_specfic_server_conf = self.skv_server_conf[SKV_HOST_SPECIFIED_ITEMS][SKV_REPLICA_SERVER_ROLE_NAME]
        group_name = replica_specfic_server_conf['hosts_to_groups'][self.my_host]
        replica_config = replica_specfic_server_conf['group_config'][group_name]
        dir_list.append(replica_config['replication']['slog_dir'])

        tag_data_dir_list = replica_config['replication']['data_dirs'].split(',')
        for tag_data_dir in tag_data_dir_list:
            tag_data_dir_pair = tag_data_dir.strip().split(':')
            if len(tag_data_dir_pair) < 2:
                raise ValueError("{tag_data_dir} should contains both tag and data dir".format(
                    tag_data_dir=tag_data_dir,
                ))
            dir_list.append(tag_data_dir_pair[1])

        return dir_list

    def mkdir_as_horizontal_scale_up(self, hosts_roles_map):
        """回调函数， 在水平扩展时创建目录"""
        # 扩data节点时就是replica_server
        role_name_list = hosts_roles_map[socket.getfqdn()]
        dir_list = self.get_dir_to_make_list_by_role_list(role_name_list)
        for d in dir_list:
            shutil.rmtree(d, ignore_errors=True)
            os.makedirs(d)
            self.logger.debug('make dir %s' % d)

    def dirs_to_make(self):
        """回调函数 返回需要创建的目录"""
        role_name_list = self.get_role_name_list_of_my_node()
        dir_list = self.get_dir_to_make_list_by_role_list(role_name_list)

        return dir_list

    def _get_addr(self, role, host=None):
        """返回host:port形式的endpoint"""
        port = get_skv_config_manager(self.module_name, role, self.logger).get_default_port()
        if host:
            return socket.gethostbyname(host) + ':' + str(port)
        else:
            return socket.gethostbyname(socket.getfqdn()) + ':' + str(port)

    def decommission(self, hosts_roles_map):
        """
        触发集群中指定机器上本模块的decommission
        Returns:
        """
        api = SkvAdminApi(self.logger, self.module_name)
        # 检查副本个数是否允许
        min_replica_count, max_replica_count = api.get_cluster_replica_count_range()
        if len(hosts_roles_map) >= min_replica_count:
            raise Exception('cannot decommission %d hosts because there are tables that only contains %d replica!' % (
                len(hosts_roles_map), min_replica_count))
        decommission_replica_count = len([x for x, roles in hosts_roles_map.items() if 'replica_server' in roles])
        current_replica_server_count = api.get_replica_server_num()
        if current_replica_server_count - decommission_replica_count < max_replica_count:
            raise Exception('cannot decommission %d hosts because there are tables that contains %d replica!' % (
                len(hosts_roles_map), max_replica_count))
        unhealthy_app_count = api.get_unhealthy_app_count()
        if unhealthy_app_count != 0:
            raise Exception('cannot decommission because there are %d unhealthy app!' % unhealthy_app_count)
        # 检查是否在进行 balance, 二者都会进行分片迁移，如果正在进行 balance 可能会导致 decommission 失效。
        if api.META_LEVEL_LIVELY == api.get_meta_level() and not check_balance(self.module_name, self.logger):
            raise Exception('cannot decommission because the skv cluster is unbalanced!')
        api.set_meta_level(api.META_LEVEL_STEADY)
        # 设置黑名单
        black_list = []
        for host, roles in hosts_roles_map.items():
            if 'replica_server' in roles:
                black_list.append(self._get_addr('replica_server', host))
        api.set_replica_server_black_list(','.join(black_list))
        # 将assign_delay_ms设为10，这样做的目的是让节点下线后，立即在其他节点上补充备份
        api.set_lb_assign_delay_ms(10)
        for host, roles in hosts_roles_map.items():
            if 'replica_server' not in roles:
                continue
            addr = self._get_addr('replica_server', host)

            # 把待下线节点上的primary副本全部挪走
            move_primary(self.module_name, self.logger, addr)
            # 将待下线机器上的secondary replica转移走，使用downgrade_node是用来标记secondary为Inactive，从而触发数据转移
            inactive_replica(self.module_name, self.logger, addr)

    def _check_decommsion_on_host(self, host):
        """对单台机器的decommission检查 主要看主副本数是否为0 由于inactive的执行太玄学了 我放弃了 只要主挪走就行"""
        addr = self._get_addr('replica_server', host)
        api = SkvAdminApi(self.logger, self.module_name)
        count = api.get_primary_count_on_server(addr)
        if count == 0:
            self.logger.info('check decommission done for %s' % addr)
            return True
        else:
            self.logger.warn('check decommission undone for %s: still has %d primary replica' % (addr, count))
            return False

    def check_decommission(self, hosts_roles_map):
        """
        查看集群中本模块的decommision是否完成
        主要看副本数是否为0
        Returns: true/false
        """
        ret = True
        for h, roles in hosts_roles_map.items():
            if 'replica_server' in roles and not self._check_decommsion_on_host(h):
                ret = False
        return ret

    def check_balance(self):
        """
        查看集群中本模块的balance是否完成
        Returns: true/false
        """
        if check_balance(self.module_name, self.logger):
            self.logger.info('check balance done')
            # 完成后需要关闭设置steady
            api = SkvAdminApi(self.logger, self.module_name)
            api.set_meta_level(api.META_LEVEL_STEADY)
            return True
        return False

    def balance(self):
        """
        发起负载均衡策略
        """
        balance_no_wait(self.module_name, self.logger)

    def is_role_auto_scale_up(self, role_name):
        """
        本模块是否在自动随着机器扩充进行水平扩展
        默认自动进行水平扩展
        Returns:

        """
        if role_name == 'replica_server':
            return True
        elif role_name == 'meta_server':
            if len(InnerDeployTopo.get_instance().get_host_list_by_role_name(product_name=self.product_name,
                                                                             module_name=self.module_name,
                                                                             role_name=role_name)) == 2:
                return True
        return False

    def is_role_can_scale_down(self, role_name):
        """
        本模块的各个角色是否可以减少
        默认可以减少
        Args:
            role_name:

        Returns:

        """
        if role_name == 'replica_server':
            return True
        return False

    def _get_min_single_disk_capacity(self, host, dirs):
        """获取指定 host 的 dirs 的最小磁盘容量（单位MB）"""
        connector = SSHConnector.get_instance(host)
        dir_value = {}
        for d in dirs:
            output = connector.check_output('df -m %s' % d)
            fields = output.splitlines()[-1].split()
            value = int(fields[1])
            dir_value[d] = value
        storage_type = 'online_random' if self.module_name == SKV_ONLINE_MODULE_NAME else 'random'
        self.logger.debug('%s %s disk capacity(MB): %s' % (host, storage_type, str(dir_value)))
        return min(dir_value.values())

    def _find_min_disk_capacity(self, host, dirs):
        """ 找到 dirs 中空间最小的磁盘，并放在 dirs[0] 处 """
        connector = SSHConnector.get_instance(host)
        min_capacity = 0
        for i in range(len(dirs)):
            output = connector.check_output('df -m %s' % dirs[i])
            fields = output.splitlines()[-1].split()
            value = int(fields[1])
            if 0 == i:
                min_capacity = int(fields[1])
                continue
            if value < min_capacity:
                tmp = dirs[0]
                dirs[0] = dirs[i]
                dirs[i] = tmp
        return dirs

    def _get_mem_gb_bound(self):
        """获取当前集群内 replica_server 机器内存的最大&最小值"""
        host_to_mem_gb = {
            server.split(':')[0]: InnerNodeInfo().get_machine_mem_gb(socket.getfqdn(server.split(':')[0]))
            for server in SkvAdminApi(self.logger, self.module_name).get_all_replica_server()
        }
        return min(host_to_mem_gb.values()), max(host_to_mem_gb.values())

    def _get_cluster_disk_info(self):
        """获取当前集群内 replica_server 所在机器 data_dirs 信息
return
    1.data_dirs 中 disk 数量的最小值
    2.data_dirs 中 disk 数量的最大值
    3.最小 random disk 的容量（单位 MB）"""
        server_conf = ConfigManager().get_server_conf(SKV_PRODUCT_NAME, self.module_name)
        group_config = server_conf['host_group']['replica_server']['group_config']
        hosts_to_groups = server_conf['host_group']['replica_server']['hosts_to_groups']

        hosts_to_disk_num = {}
        hosts_to_min_single_disk_capacity = {}

        for host, group in hosts_to_groups.items():
            dirs = {d.split(':')[1] for d in group_config[group]['replication']['data_dirs'].split(',')}
            hosts_to_disk_num[host] = len(dirs)
            hosts_to_min_single_disk_capacity[host] = self._get_min_single_disk_capacity(host, dirs)
        return max(hosts_to_disk_num.values()), min(hosts_to_disk_num.values()), min(hosts_to_min_single_disk_capacity.values())

    def before_horizontal_scale_up_progress(self, hosts_roles_map):
        """
        水平扩展前模块的行为。集群调用一次，可重入。

        skv此处主要是修改server conf
        """
        # meta server升级直接返回
        if len(hosts_roles_map) == 1 and list(hosts_roles_map.values())[0] == ['meta_server']:
            return []

        # online节点有些不同
        if self.module_name == 'skv_online':
            role = 'meta_server'
            current_cnt = len(InnerDeployTopo.get_instance().get_host_list_by_role_name(SKV_PRODUCT_NAME, self.module_name, role))
            if current_cnt == 1:
                raise Exception('Horizontal deploy for standalone skv online is not supported!')
            elif current_cnt == 2:
                # 只支持2->3
                if set(hosts_roles_map[self.my_host]) != {'replica_server', 'meta_server'}:
                    raise Exception('skv online only support 2 replica+meta -> 3 replica+meta!')
                return []

        for host, role_list in hosts_roles_map.items():
            if role_list != ['replica_server']:
                raise Exception('only support horizontal deploy replica_server!')

        # 以下场景都是扩replica server
        # 1. 更新replica server list
        old_replica_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'replica_server_list')
        new_set = {self._get_addr('replica_server', x) for x in hosts_roles_map}
        new_replica_list = list(set(old_replica_list) | new_set)
        # 2. 新增配置组
        host_group_conf = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS)

        min_mem_gb, max_mem_gb = self._get_mem_gb_bound()
        max_disk_num, min_disk_num, min_single_disk_capacity = self._get_cluster_disk_info()

        # 更新skv_offline client conf中的partition_factor
        partition_factor = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)._calc_partition_factor()

        # 检查新增的配置是不是同构的
        host, random_dirs, mem_gb = None, None, None
        host_to_single_disk_min_capacity = {}
        storage_type = 'online_random' if self.module_name == SKV_ONLINE_MODULE_NAME else 'random'
        for h in hosts_roles_map:
            r = DirectoryInfo().get_storage_data_dir_by_hostname(h, storage_type)
            m = InnerNodeInfo().get_machine_mem_gb(h)
            host_to_single_disk_min_capacity[h] = self._get_min_single_disk_capacity(h, r)
            if host:
                if r != random_dirs:
                    raise Exception('random dirs on %s[%s] is different from %s[%s]!' % (r, h, random_dirs, host))
                # 新增机器内存检查，新增机器与每台机器之间内存差不得大于新增机器内存的 10%
                if abs(m - mem_gb) > mem_gb * (1 - FIT_COEFFICIENT):
                    raise Exception('memory on %s[%s] is different from %s[%s]!' % (m, h, mem_gb, host))
            host, random_dirs, mem_gb = h, r, m
            partition_factor += min(len(r), int(mem_gb / 28))
        self.logger.info('random_dirs=%s mem_gb=%s' % (random_dirs, mem_gb))

        # 检查内存资源，新增 replica_server 机器的内存不得低于现有集群最低的 10%
        if mem_gb < min_mem_gb * FIT_COEFFICIENT:
            raise Exception('new replica_server memory(gb) should greater than %d' % min_mem_gb)
        # 检查磁盘资源，新增 replica_server 机器的 random 盘数量不得低于现有集群最低的，每块盘容量不得低于现有最低的 10%
        if len(random_dirs) < min_disk_num:
            raise Exception('new replica_server %s disk number should greater than %d' % (storage_type, min_disk_num))
        if min(host_to_single_disk_min_capacity.values()) < min_single_disk_capacity * FIT_COEFFICIENT:
            self.logger.error('host_to_single_disk_min_capacity: %s' % str(host_to_single_disk_min_capacity))
            raise Exception('new replica_server %s disk min single disk capacity should greater than %s current min capacity %d (MB)' % (
                storage_type, self.module_name, min_single_disk_capacity
            ))

        if max_mem_gb < mem_gb:
            mem_gb = max_mem_gb
        if max_disk_num < len(random_dirs):
            random_dirs = random_dirs[:max_disk_num]
        # 当安装Hubble,并且老环境最小磁盘数大于3,则认为是标准saas环境，可将一个最小的磁盘用于存放slog，其余的用于数据落盘
        # 这里有个bug，比如新加两个节点磁盘空间为 [100,200,300,400], [200, 100, 300, 400]大小排序不一致,对应[data0,data1,data2,data3]在上面的检查，认为是同构的，将data1作为slog磁盘
        # 但是第一个节点的slog会是200G的那个磁盘，不知道斥候有没有做对应的检查
        is_saas = False
        if is_hubble_installed() and min_disk_num >= SAAS_SKV_MIN_DISK_NUM:
            random_dirs = self._find_min_disk_capacity(host, random_dirs)
            is_saas = True
        # 修改配置组信息
        new_group_conf = init_replica_server_group_config(self.module_name, random_dirs, mem_gb, is_saas)
        max_group_id = host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['max_group_id'] + 1
        new_group_name = 'group_%d' % max_group_id
        for h in hosts_roles_map:
            host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'][h] = new_group_name
        host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['group_config'][new_group_name] = new_group_conf
        host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['max_group_id'] = max_group_id
        return [
            ('update server list', lambda: ConfigManager().set_server_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, 'replica_server_list', new_replica_list)),
            ('update group config', lambda: ConfigManager().set_server_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS, host_group_conf)),
            ('update replica client config', lambda: ConfigManager().set_client_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, 'partition_factor', partition_factor)),
            ('wait zk sync', lambda: time.sleep(10)),
        ]

    def horizontal_deploy_module_progress(self, hosts_roles_map):
        """
        本机水平扩展模块时的调用的回掉函数
        返回一个list，其中是多个tuple 每个tuple包含一个说明message和一个function
        要求可重入。
        """
        roles = hosts_roles_map[self.my_host]
        if SKV_META_SERVER_ROLE_NAME in roles:
            # 支持2->3 丁炎那边会检查
            if self.module_name == 'skv_online' and SKV_REPLICA_SERVER_ROLE_NAME in roles:
                return self.skv_2_to_3_progress(hosts_roles_map)
            # 支持增加单个meta server 这是我们自己的meta_server transfer工具会做的
            elif len(hosts_roles_map) == 1 and list(hosts_roles_map.values())[0] == ['meta_server']:
                return [
                    ('Stop meta server', self.stop_meta_server),
                    ('Make skv dirs', lambda: self.mkdir_as_horizontal_scale_up(hosts_roles_map)),
                    ('Start meta server', lambda: self._start_on_horizontal_deploy('meta_server')),
                    ('render shell config and dispatch', self._render_shell_and_dispatch),
                ]
            raise Exception('add meta server is not supported!')

        # 否则就是扩replica
        return [
            ('Stop replica server', self.stop_replica_server),
            ('Make skv dirs', lambda: self.mkdir_as_horizontal_scale_up(hosts_roles_map)),
            ('Start replica server', lambda: self._start_on_horizontal_deploy('replica_server')),
            ('render shell config and dispatch', self.render_shell_config),
        ]

    def after_horizontal_scale_up_progress(self, hosts_roles_map):
        """
        水平扩展后模块的行为。集群调用一次，可重入。

        skv此处主要是发起balance
        """
        return [
            ('send balance', self.balance),
        ]

    def before_horizontal_scale_down_progress(self, hosts_roles_map):
        """
        水平缩容前模块对自己元数据的修改。集群调用一次，可重入。

        skv此处主要是参数检查+把replica 从server conf里面去掉
        """
        # meta server升级直接返回
        if len(hosts_roles_map) == 1 and list(hosts_roles_map.values())[0] == ['meta_server']:
            return []
        # 删节点只支持对replica操作 并且删完需要还有至少3个replica server
        for host, role_list in hosts_roles_map.items():
            if role_list != ['replica_server']:
                raise Exception('only support horizontal scale down on replica_server!')
        role = 'replica_server'
        down_set = set(hosts_roles_map.keys())
        current_set = set(InnerDeployTopo.get_instance().get_host_list_by_role_name(SKV_PRODUCT_NAME, self.module_name, role))
        after_set = current_set - down_set
        if len(after_set) < 3:
            raise Exception(
                'failed to horizontal scale down replica server: only %d replica server alive%s, require at least 3!' % (
                    len(after_set), after_set))
        # 检查是否已经decommission完成
        if not self.check_decommission(hosts_roles_map):
            raise Exception('check decommission undone! please wait for decommission!')

        # 删除replica server list
        old_replica_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'replica_server_list')
        del_replica_set = {self._get_addr('replica_server', h) for h in down_set}
        new_replica_list = list(set(old_replica_list) - del_replica_set)
        # 从配置组里面删除
        host_group_conf = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS)
        all_groups = set()
        for h in hosts_roles_map:
            all_groups.add(host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'][h])
            host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'].pop(h)
        remove_groups = []
        for group in all_groups:
            for h, g in host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'].items():
                if g == group:
                    break
            else:
                # 说明已经没有这个配置组了 需要自动清理掉相关配置组
                remove_groups.append(group)
        for g in remove_groups:
            host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['group_config'].pop(g)
        # 分区因子
        partition_factor = 0
        for new_host in new_replica_list:
            hostname = socket.getfqdn(new_host.split(':')[0])
            new_group = host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['hosts_to_groups'][hostname]
            # 用于保存通过磁盘数来计算的分区因子
            partition_factor_by_disk_count = len(host_group_conf[SKV_REPLICA_SERVER_ROLE_NAME]['group_config'][new_group]['replication']['data_dirs'].split(","))
            # 用于保存通过内存来计算的分区因子
            partition_factor_by_mem = int(InnerNodeInfo().get_machine_mem_gb(hostname) / 28)
            partition_factor += min(partition_factor_by_disk_count, partition_factor_by_mem)

        return [
            ('update server list', lambda: ConfigManager().set_server_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, 'replica_server_list', new_replica_list)),
            ('update group config', lambda: ConfigManager().set_server_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS, host_group_conf)),
            ('update replica client config', lambda: ConfigManager().set_client_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, "partition_factor", partition_factor)),
            ('wait zk sync', lambda: time.sleep(10)),
        ]

    def horizontal_scale_down_progress(self, hosts_roles_map):
        """本机删除机器时的回调，本地做模块自身需要的清理 默认为空
        要求可重入。"""
        if len(hosts_roles_map) == 1 and list(hosts_roles_map.values())[0] == ['meta_server']:
            # skv 转移meta server工具需要调用这个加角色
            def change_conf():
                new_meta_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server_list')
                ConfigManager().set_client_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server_list', new_meta_list)
                host_group_conf = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS)
                host_group_conf[SKV_META_SERVER_ROLE_NAME]['hosts_to_groups'].pop(self.my_host)
                ConfigManager().set_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS, host_group_conf)
                # 等待zk同步..
                self.logger.info('wait zk sync')
                time.sleep(10)
            return [
                ('stop old meta server', lambda: self._stop_meta_servers([self.my_host])),
                ('change conf', change_conf),
                ('render shell config and dispatch', self._render_shell_and_dispatch),
            ]
        return [
            ('Stop replica server', lambda: safely_stop_replica_server(module_name=self.module_name, logger=self.logger, replica_server_addr=self._get_addr('replica_server'), exec_by_shell=True)),
        ]

    def after_horizontal_scale_down_progress(self, hosts_roles_map):
        """
        水平缩容后模块对自己元数据的修改。集群调用一次，可重入。

        skv此处主要是重启meta server
        """
        if len(hosts_roles_map) == 1 and list(hosts_roles_map.values())[0] == ['meta_server']:
            return []
        return [
            ('restart primary meta server', lambda: restart_primary_meta_server(self.module_name, self.logger)),
        ]

    def skv_2_to_3_progress(self, hosts_roles_map):
        """
        调用skv 2->3的步骤
        """
        ret = []
        config_manager = ConfigManager()
        server_to_add = self._get_addr('meta_server')
        old_meta_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server_list')
        new_meta_list = list(set(old_meta_list) | {server_to_add})

        def __update_configs():
            # 更新meta server list
            self.logger.info('add skv meta server list: %s' % new_meta_list)
            config_manager.set_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server_list', new_meta_list)

            # 更新replica server list
            old_replica_list = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'replica_server_list')
            new_replica_list = list(set(old_replica_list) | {self._get_addr('replica_server')})
            self.logger.info('add skv replica server list: %s' % new_replica_list)
            config_manager.set_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'replica_server_list', new_replica_list)

            # 2->3 balance type变成greedy 副本数变成3
            meta_server_conf = config_manager.get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server')
            meta_server_conf['meta_server']['server_load_balancer_type'] = 'greedy_load_balancer'
            meta_server_conf['meta_server']['min_live_node_count_for_unfreeze'] = '2'
            meta_server_conf['replication.app']['max_replica_count'] = '3'
            self.logger.info('change meta server conf: %s' % meta_server_conf)
            config_manager.set_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'meta_server', meta_server_conf)

            # 新增配置
            host_group_conf = ConfigManager().get_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS)
            if self.module_name == SKV_ONLINE_MODULE_NAME:
                meta_dir = self.get_online_random_storage_dir()[0]
                random_dirs = self.get_online_random_storage_dir()
            else:
                meta_dir = self.get_meta_storage_dir()
                random_dirs = DirectoryInfo().get_storage_data_dir_by_hostname(self.my_host, 'random')
            mem_gb = InnerNodeInfo().get_machine_mem_gb(self.my_host)
            new_group_conf = {
                SKV_META_SERVER_ROLE_NAME: init_meta_server_group_config(self.module_name, meta_dir),
                SKV_REPLICA_SERVER_ROLE_NAME: init_replica_server_group_config(self.module_name, random_dirs, mem_gb),
            }
            for role in [SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME]:
                max_group_id = host_group_conf[role]['max_group_id'] + 1
                new_group_name = 'group_%d' % max_group_id
                host_group_conf[role]['hosts_to_groups'][self.my_host] = new_group_name
                host_group_conf[role]['group_config'][new_group_name] = new_group_conf[role]
                host_group_conf[role]['max_group_id'] = max_group_id
            self.logger.info('change host group: %s' % host_group_conf)
            config_manager.set_server_conf_by_key(SKV_PRODUCT_NAME, self.module_name, SKV_HOST_SPECIFIED_ITEMS, host_group_conf)
            partition_factor = get_skv_config_manager(self.module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)._calc_partition_factor()
            config_manager.set_client_conf_by_key(SKV_PRODUCT_NAME, self.module_name, 'partition_factor', partition_factor)
            # 改完zk等待同步
            self.logger.info('wait zk sync')
            time.sleep(10)

        # 1. 初始化本机
        ret.extend([
            ('change server conf', __update_configs),
            ('Stop all skv services if already started', self.stop_all_services),
            ('Make skv dirs', lambda: self.mkdir_as_horizontal_scale_up(hosts_roles_map)),
            ('render shell config', self.render_shell_config),
        ])

        api = SkvAdminApi(self.logger, self.module_name)
        current_primary_server = api.get_primary_meta_server()
        current_primary_host = current_primary_server.split(':')[0]
        old_meta_server_addrs = InnerDeployTopo.get_instance().get_host_list_by_role_name(SKV_PRODUCT_NAME, self.module_name,
                                                                                          'meta_server')
        non_primary_server = [x for x in old_meta_server_addrs if x != current_primary_server][0]
        non_primary_host = non_primary_server.split(':')[0]
        replica_server_hosts = InnerDeployTopo.get_instance().get_host_list_by_role_name(SKV_PRODUCT_NAME, self.module_name,
                                                                                         'replica_server')

        ret.extend([
            # 2. 先启动新的replica server 不然重启meta会报错
            ('start new replica server', lambda: self._start_on_horizontal_deploy(SKV_REPLICA_SERVER_ROLE_NAME)),
            # 3. 重启之前的非primary meta server 2. 重启primary, 会切主
            ('restart old meta server',
             lambda: self._restart_meta_servers([non_primary_host, current_primary_host])),
            # 4. 启动新的meta server
            ('start new meta server', lambda: self._start_on_horizontal_deploy(SKV_META_SERVER_ROLE_NAME)),
            # 5. 更新client conf
            ('update client conf', lambda: ConfigManager().set_client_conf_by_key(
                SKV_PRODUCT_NAME, self.module_name, 'meta_server_list', new_meta_list)),
            # 6. 滚动重启所有老的replica server
            ('restart all replica server', lambda: [safely_restart_replica_server(
                self.module_name, self.logger, self._get_addr('replica_server', h)) for h in replica_server_hosts]),
        ])

        # 7. todo： 修改为3副本？
        return ret

    def _start_on_horizontal_deploy(self, role):
        """扩缩容时启动角色"""
        cmd = "cd ${SKV_HOME}/construction_blueprint && python3 skv_server.py start -m %s -r %s" % (
            self.module_name, role)
        utils.shell_wrapper.check_call(cmd, self.logger.debug)
        if role == 'replica_server':
            wait_replica_server(self.module_name, self.logger, [self._get_addr(role)])

    def _restart_meta_servers(self, hosts):
        """重启多个主机上的meta server"""
        module_service = ModuleService()
        for host in hosts:
            module_service.start(SKV_PRODUCT_NAME, self.module_name, 'meta_server', socket.getfqdn(host))

    def _stop_meta_servers(self, hosts):
        module_service = ModuleService()
        for host in hosts:
            module_service.stop(SKV_PRODUCT_NAME, self.module_name, 'meta_server', socket.getfqdn(host))
