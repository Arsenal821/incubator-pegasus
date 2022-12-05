# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import traceback

from recipes.common.async_wait_runner import AsyncWaitRunner
from skv_admin_api import SkvAdminApi


def wait_all_meta_servers_available(module_name, logger, print_progress_fun=None,
                                    check_interval_seconds=1, check_timeout_seconds=None):
    """
    对外接口: 等待所有meta servers都启动, 且选举出primary meta server
    """
    runner = WaitMetaServerRunner(module_name, logger, print_progress_fun,
                                  check_interval_seconds, check_timeout_seconds)
    runner.run()


class WaitMetaServerRunner(AsyncWaitRunner):
    def __init__(self, module_name, logger, print_progress_fun=None,
                 check_interval_seconds=1, check_timeout_seconds=None):
        self.module_name = module_name
        self.logger = logger
        self.print_progress_fun = print_progress_fun if print_progress_fun else self.logger.info

        super().__init__(check_interval_seconds, check_timeout_seconds)

    def async_do(self):
        """纯等待 什么都不做"""
        meta_server_info = self.execute_check()
        self.print_progress(meta_server_info)

    def execute_check(self):
        """执行一次检查"""
        api = SkvAdminApi(self.logger, self.module_name)

        primary_meta_server = None
        try:
            primary_meta_server = api.get_primary_meta_server()
        except Exception:
            self.logger.debug("trying to get primary meta server but got exception, this exception can be ignored..")
            self.logger.debug(traceback.format_exc())

        unalive_meta_server_list = api.meta_server_endpoint.split(',')
        try:
            unalive_meta_server_list = api.get_unalive_meta_server_list()
        except Exception:
            self.logger.debug("trying to get unalive meta servers but got exception, this exception can be ignored..")
            self.logger.debug(traceback.format_exc())

        return primary_meta_server, unalive_meta_server_list

    def check_wait_done(self, meta_server_info):
        """检查结果 是否可以结束等待 result是execute_check()的结果 返回True/False"""
        return meta_server_info[0] and not meta_server_info[1]

    def print_progress(self, meta_server_info):
        """打印进度 result是execute_check()的结果"""
        if meta_server_info[0]:
            if meta_server_info[1]:
                self.print_progress_fun(
                    "there are still some unalive meta servers: {unalive_meta_server_list}".format(
                        unalive_meta_server_list=meta_server_info[1],
                    )
                )
            else:
                self.print_progress_fun(
                    "current primary meta server is {primary_meta_server}, "
                    "and all meta servers are alive".format(
                        primary_meta_server=meta_server_info[0],
                    )
                )
        else:
            self.print_progress_fun("waiting for primary meta server selected..")
