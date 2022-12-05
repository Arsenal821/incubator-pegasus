#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

安全关闭、启动 replcia server的操作

参考https://doc.sensorsdata.cn/pages/viewpage.action?pageId=104865024#skv%E6%9C%8D%E5%8A%A1%E7%AB%AF%E5%8D%87%E7%BA%A7&%E5%9B%9E%E6%BB%9A%E6%96%B9%E6%A1%88-%E7%83%AD%E5%8D%87%E7%BA%A7%E6%95%B4%E4%BD%93%E6%B5%81%E7%A8%8B

关闭前
    1. 关闭负载均衡
    2. 禁掉meta server的add secondary操作
    3. 将replica server上的primary replica迁走
    4. 将replica server上的secondary都降级为inactive
    5. 关闭replica server上所有的replica 以触发flush操作
    6. flush logging
    7. 增加meta server的add secondary操作的max count for one node

启动后:
    1. 等待nodes正常
    2. 等待分片正常
"""
import socket
import time

from hyperion_guidance.ssh_connector import SSHConnector

from skv_admin_api import SkvAdminApi
from recipes.ops.move_primary_runner import move_primary
# from recipes.ops.inactive_replica_runner import inactive_replica
# from recipes.ops.kill_partition_runner import kill_partition
from recipes.ops.wait_replica_server_runner import wait_replica_server
from recipes.ops.wait_replica_server_load_table_runner import wait_replica_server_load_table
from recipes.ops.wait_table_healthy_runner import wait_table_healthy
from recipes.platform_adapter import get_service_controller


def prepare_safely_stop_replica_server(module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None):
    """对外接口 安全关闭replica server 准备工作(不实际停止)"""
    s = SafelyRestartReplicaServer(module_name, logger, replica_server_addr, print_progress_fun, timeout)
    if not s.check_status(s.ALIVE_STATUS, None):
        s.prepare_stop()
    return s


def safely_stop_replica_server(module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None,
                               my_meta_server_excluded=False, exec_by_shell=False):
    """
    对外接口 安全关闭replica server
    exec_by_shell: skv 缩容由于各种原因 captain 不能通过 api 停止实例，这里添加 flag 使用 local stop 停止
    """
    s = SafelyRestartReplicaServer(module_name, logger, replica_server_addr, print_progress_fun, timeout,
                                   my_meta_server_excluded)
    if not s.check_status(s.ALIVE_STATUS, s.DEAD_STATUS):
        s.prepare_stop()
        s.do_stop(exec_by_shell)
    return s


def start_and_check_replica_server(module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None,
                                   my_meta_server_excluded=False):
    """对外接口 安全启动replica server"""
    s = SafelyRestartReplicaServer(module_name, logger, replica_server_addr, print_progress_fun, timeout,
                                   my_meta_server_excluded)
    s.do_start()
    s.check_after_start()
    return s


def check_after_start_replica_server(module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None, total_replica_count=None):
    """对外接口 启动replica server后的检查工作"""
    s = SafelyRestartReplicaServer(module_name, logger, replica_server_addr, print_progress_fun, timeout, total_replica_count=total_replica_count)
    if not s.check_status(s.ALIVE_STATUS, None):
        s.check_after_start()
    return s


def safely_restart_replica_server(module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None):
    """对外接口 安全重启replica server"""
    s = SafelyRestartReplicaServer(module_name, logger, replica_server_addr, print_progress_fun, timeout)
    if not s.check_status(s.ALIVE_STATUS, s.DEAD_STATUS):
        s.prepare_stop()
    s.do_restart()
    s.check_after_start()
    return s


def safely_restart_all_replica_server(module_name, logger, print_progress_fun=None, timeout=None):
    """对外接口 安全重启所有replica server"""
    print_progress_fun = print_progress_fun if print_progress_fun else logger.info
    start_time = time.time()
    api = SkvAdminApi(logger, module_name)
    replica_server_list = api.get_all_replica_server()
    for i, replica_server in enumerate(replica_server_list):
        print_progress_fun('%d/%d restart %s' % (i + 1, len(replica_server_list), replica_server))
        if timeout is None:
            cmd_timeout = None
        else:
            cmd_timeout = timeout - (time.time() - start_time)
        return safely_restart_replica_server(module_name, logger, replica_server, print_progress_fun, cmd_timeout)


class SafelyRestartReplicaServer:
    ALIVE_STATUS = 'ALIVE'
    DEAD_STATUS = 'DEAD'

    def __init__(self, module_name, logger, replica_server_addr, print_progress_fun=None, timeout=None,
                 my_meta_server_excluded=False, total_replica_count=None):
        self.module_name = module_name
        self.logger = logger
        host, self.replica_port = replica_server_addr.split(':')
        self.replica_fqdn = socket.getfqdn(host)
        ip = socket.gethostbyname(host)
        self.replica_server_ip_addr = ':'.join((ip, self.replica_port))
        self.replica_server_addr = ':'.join((self.replica_fqdn, self.replica_port))
        self.print_progress_fun = print_progress_fun if print_progress_fun else logger.info
        self.api = SkvAdminApi(logger, module_name)
        if my_meta_server_excluded:
            new_endpoint_list = list(filter(
                lambda endpoint: ip != socket.gethostbyname(endpoint.split(':')[0]),
                self.api.meta_server_endpoint.split(',')
            ))
            self.api.meta_server_endpoint = ','.join(new_endpoint_list)
            self.logger.info(
                "meta server list is: {meta_server_endpoint}, with {ip} excluded".format(
                    meta_server_endpoint=self.api.meta_server_endpoint, ip=ip,
                )
            )
        self.start_time = time.time()
        self.timeout = timeout
        # 至少三个节点
        self.replica_server_num = self.api.get_replica_server_num()
        if self.replica_server_num < 3:
            raise Exception('at least 3 replica is required!')
        # 检查 replica count 不能为小于 2，否则将无法将 primary 身份转移
        self.api.check_all_avaliable_table_replica_count(2)
        # 记录总的分片数 restart的时候验证是否全部加载
        self.total_replica_count = total_replica_count
        self.service_controller = get_service_controller(self.logger, self.module_name)

    def _reset_timeout(self):
        """操作步骤太多了 每次操作需要检查超时后返回新的超时"""
        if not self.timeout:
            return None
        now = time.time()
        self.timeout = self.timeout - (now - self.start_time) / 1000
        self.start_time = now
        if self.timeout <= 0:
            raise Exception('execute timeout %d seconds!' % self.timeout)
        return self.timeout

    def prepare_stop(self):
        """停止前的检查"""
        # 1. 关闭负载均衡
        self.api.set_meta_level(self.api.META_LEVEL_STEADY)
        # 2. 禁掉meta server的add secondary操作
        self.api.set_add_secondary_max_count_for_one_node(0)
        # 3. 将replica server上的primary replica迁走
        move_primary(self.module_name, self.logger, self.replica_server_ip_addr, self.print_progress_fun, check_timeout_seconds=self._reset_timeout())
        # 牵走之后需要sleep 5s 等待所有客户端更新路由
        time.sleep(5)
        # 记录下本机的 replica 个数
        self.total_replica_count = self.api.get_serving_replica_count(self.replica_server_ip_addr)
        # 下面几个操作实测行为不是很稳定 先注释掉了
        # # 4. 将replica server上的secondary都降级为inactive
        # inactive_replica(self.module_name, self.logger, self.replica_server_ip_addr, self.print_progress_fun, check_timeout_seconds=self._reset_timeout())
        # # 5. 关闭replica server上所有的replica 以触发flush操作
        # kill_partition(self.module_name, self.logger, self.replica_server_ip_addr, self.print_progress_fun)
        # # 6. flush logging
        # self.api.flush_logging(self.replica_server_ip_addr)
        self.api.set_add_secondary_max_count_for_one_node(100)
        self.print_progress_fun('prepare stop done')

    def check_after_start(self):
        """启动后的检查"""
        # 1. 等待所有的replica server变为alive
        wait_replica_server(self.module_name, self.logger, [self.replica_server_ip_addr], 'ALIVE', self.print_progress_fun, check_timeout_seconds=self._reset_timeout())
        # 2. 等待replica server加载所有分片
        wait_replica_server_load_table(self.module_name, self.logger, self.replica_server_ip_addr, self.total_replica_count, self.print_progress_fun, check_timeout_seconds=self._reset_timeout())
        # 3. 等待所有的partition变为healthy
        wait_table_healthy(self.module_name, self.logger, self.print_progress_fun, check_timeout_seconds=self._reset_timeout())
        self.api.set_add_secondary_max_count_for_one_node('DEFAULT')
        self.print_progress_fun('start succeed')

    def do_stop(self, exec_by_shell=False):
        """停止服务"""
        self.print_progress_fun('stopping replica server on %s...' % self.replica_server_addr)
        if exec_by_shell:
            c = SSHConnector.get_instance(self.replica_fqdn)
            cmd = 'spadmin local stop -m %s -r replica_server -p skv' % str(self.module_name)
            self.print_progress_fun('execute %s on %s' % (cmd, self.replica_fqdn))
            c.run_cmd(cmd, self.logger.debug)
        else:
            self.service_controller.stop_replica_server(self.replica_fqdn)
        # stop之后等待节点状态为unalive
        wait_replica_server(self.module_name, self.logger, [self.replica_server_ip_addr], 'UNALIVE', self.print_progress_fun, check_timeout_seconds=self._reset_timeout())

    def do_start(self):
        """启动服务"""
        self.print_progress_fun('starting replica server on %s...' % self.replica_server_addr)
        self.service_controller.start_replica_server(self.replica_fqdn)

    def do_restart(self):
        """重启服务"""
        self.print_progress_fun('restarting replica server on %s...' % self.replica_server_addr)
        self.service_controller.restart_replica_server(self.replica_fqdn)

    def check_status(self, expect_init_status, expect_end_status):
        # 此处用端口号检测 云平台不提供即使查询结果
        c = SSHConnector.get_instance(self.replica_fqdn)
        cmd = 'lsof -i tcp:%s -s tcp:LISTEN' % str(self.replica_port)
        ret = c.run_cmd(cmd, self.logger.debug)
        if ret['ret'] == 0:
            status = self.ALIVE_STATUS
        elif ret['ret'] == 1 and not ret['stdout'] and not ret['stderr']:
            status = self.DEAD_STATUS
        else:
            raise Exception('failed to run %s: %s' % (cmd, ret))
        if expect_end_status and status == expect_end_status:
            self.logger.info('%s is already %s' % (self.replica_server_addr, expect_end_status))
            return True
        if status != expect_init_status:
            raise Exception('unexpect status %s for %s! expect %s/%s!' % (status, self.replica_server_addr, expect_init_status, expect_end_status))
        return False
