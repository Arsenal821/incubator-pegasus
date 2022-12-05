#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

启停meta server相关的操作
重启meta server之后cluster_info等待primary meta server正常
"""
import socket
import time
import traceback

from skv_admin_api import SkvAdminApi
from recipes.common.async_wait_runner import AsyncWaitRunner
from recipes.ops.wait_meta_server_runner import wait_all_meta_servers_available
from recipes.platform_adapter import get_service_controller


def stop_meta_server(module_name, logger, meta_server_addr, print_progress_fun=None, timeout=None):
    """停止单个meta server"""
    runner = RestartMetaServer(module_name, logger, meta_server_addr, print_progress_fun, timeout=timeout)
    runner.do_stop()


def start_meta_server(module_name, logger, meta_server_addr, print_progress_fun=None, timeout=None):
    """停止单个meta server"""
    runner = RestartMetaServer(module_name, logger, meta_server_addr, print_progress_fun, timeout=timeout)
    runner.do_start()


def restart_meta_server(module_name, logger, meta_server_addr, print_progress_fun=None, timeout=None):
    """重启单个meta server"""
    runner = RestartMetaServer(module_name, logger, meta_server_addr, print_progress_fun, timeout=timeout)
    runner.do_restart()


def restart_primary_meta_server(module_name, logger, print_progress_fun=None, timeout=None):
    """只重启primary meta server"""
    api = SkvAdminApi(logger, module_name)
    primary_meta_server = api.get_primary_meta_server()
    runner = RestartMetaServer(module_name, logger, primary_meta_server, print_progress_fun, timeout=timeout)
    runner.do_restart()


def stop_all_meta_servers(module_name, logger, print_progress_fun=None):
    """停止所有meta servers"""
    print_progress_fun = print_progress_fun if print_progress_fun else logger.info
    print_progress_fun('stopping all meta servers of %s' % module_name)
    get_service_controller(logger, module_name).stop_all_meta_server()


def start_all_meta_servers(module_name, logger, print_progress_fun=None):
    """启动所有meta servers"""
    print_progress_fun = print_progress_fun if print_progress_fun else logger.info
    print_progress_fun('starting all meta servers of %s' % module_name)
    get_service_controller(logger, module_name).start_all_meta_server()
    wait_all_meta_servers_available(module_name, logger, print_progress_fun)


def restart_all_meta_server(module_name, logger, print_progress_fun=None, timeout=None):
    """重启所有meta server 最后重启primary 减少切主"""
    api = SkvAdminApi(logger, module_name)
    primary_meta_server = api.get_primary_meta_server()
    other_meta_server_list = [x for x in api.meta_server_endpoint.split(',') if x != primary_meta_server]
    start_time = time.time()
    for meta_server in other_meta_server_list + [primary_meta_server]:
        runner = RestartMetaServer(module_name, logger, meta_server, print_progress_fun, timeout=timeout)
        runner.do_restart()
        if timeout is not None:
            timeout = timeout - (time.time() - start_time)
            if timeout <= 0:
                raise Exception('failed to restart all meta server: timeout!')


class RestartMetaServer(AsyncWaitRunner):
    def __init__(self, module_name, logger, meta_server_addr, print_progress_fun=None, check_interval_seconds=1, timeout=None):
        self.module_name = module_name
        self.logger = logger
        host, self.meta_port = meta_server_addr.split(':')
        self.meta_fqdn = socket.getfqdn(host)
        ip = socket.gethostbyname(host)
        self.meta_server_ip_addr = ':'.join((ip, self.meta_port))
        self.meta_server_addr = ':'.join((self.meta_fqdn, self.meta_port))
        self.start_time = time.time()
        self.print_progress_fun = print_progress_fun if print_progress_fun is not None else self.logger.info
        super().__init__(check_interval_seconds, timeout)

    def _reset_time_and_wait(self):
        # 重启之后需要重置timeout
        now = time.time()
        if self.check_timeout_seconds:
            self.check_timeout_seconds = self.check_timeout_seconds - (now - self.start_time) / 1000
        super().run()

    def do_restart(self):
        self.print_progress_fun('restarting %s..' % self.meta_server_addr)
        get_service_controller(self.logger, self.module_name).restart_meta_server(self.meta_fqdn)
        self._reset_time_and_wait()

    def do_stop(self):
        self.print_progress_fun('stopping %s..' % self.meta_server_addr)
        get_service_controller(self.logger, self.module_name).stop_meta_server(self.meta_fqdn)
        self._reset_time_and_wait()

    def do_start(self):
        self.print_progress_fun('starting %s..' % self.meta_server_addr)
        get_service_controller(self.logger, self.module_name).start_meta_server(self.meta_fqdn)
        self._reset_time_and_wait()

    def async_do(self):
        """纯等待 什么都不做"""
        primary_meta_server = self.execute_check()
        self.print_progress(primary_meta_server)

    def execute_check(self):
        """执行一次检查"""
        try:
            api = SkvAdminApi(self.logger, self.module_name)
            return api.get_primary_meta_server()
        except Exception:
            self.logger.debug('trying to get primary meta server but got exception, this exception can be ignored..')
            self.logger.debug(traceback.format_exc())
            return None

    def check_wait_done(self, primary_meta_server):
        """检查结果 是否可以结束等待 result是execute_check()的结果 返回True/False"""
        return primary_meta_server is not None

    def print_progress(self, primary_meta_server):
        """打印进度 result是execute_check()的结果"""
        if primary_meta_server:
            self.print_progress_fun('current primary meta server is %s' % primary_meta_server)
        else:
            self.print_progress_fun('waiting for primary meta server selected..')
