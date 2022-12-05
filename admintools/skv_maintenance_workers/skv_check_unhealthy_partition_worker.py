#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv partition healthy 检测任务
"""

from skv_admin_api import SkvAdminApi
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME, SKV_META_SERVER_ROLE_NAME, \
    get_program_start_log_path_by_role, get_server_log_dir_by_role
from skv_maintenance_workers.base_worker import BaseWorker


class SkvCheckUnhealthyPartitionWorker(BaseWorker):
    def is_state_abnormal(self):
        api = SkvAdminApi(self.logger, self.module)
        unhealthy_table_list = api.get_unhealthy_app_list()

        if len(unhealthy_table_list) != 0:
            self.logger.error('table {table_list} unhealthy.'.format(table_list=unhealthy_table_list))
            return True
        return False

    def diagnose(self):
        self._parse_message_log()

    def repair(self):
        self._parse_message_log()

    def _parse_message_log(self):
        """解析日志有效信息"""
        api = SkvAdminApi(self.logger, self.module)
        unhealthy_gpid = api.get_unhealthy_gpid_list()
        self.logger.error('gpid {list} is unhealthy!'.format(list=str(unhealthy_gpid)))

        if len(unhealthy_gpid) <= 0:
            return

        # 这里只进一步看第一个有问题的分片的日志信息
        diagnosing_gpid = unhealthy_gpid[0]
        regex_gpid = "\"\\<" + diagnosing_gpid.replace(".", "\\.") + "\\>\""

        primary_master_server = api.get_primary_meta_server()

        # 首先输出 primary meta server 的启动日志到本地
        meta_startup_log_command_template = "egrep " + regex_gpid + " " + "%s" + " | tail -n 10"
        self._dump_remote_startup_log_message_to_local(module=self.module, role=SKV_META_SERVER_ROLE_NAME,
                                                       log_shell_template=meta_startup_log_command_template,
                                                       server_list=[primary_master_server])

        # 输出 primary meta server 的固定大小日志到本地
        meta_fixed_size_log_command_template = "egrep " + regex_gpid + " " + "%s" + " | tail -n 10"
        self._dump_remote_log_message_to_local(module=self.module, role=SKV_META_SERVER_ROLE_NAME,
                                               log_shell_template=meta_fixed_size_log_command_template,
                                               server_list=[primary_master_server])

        # 再接着尝试获取replica server的日志信息
        replica_server_list = api.get_all_replica_server()
        if len(replica_server_list) <= 3:
            # 这里如果满足 (1)meta_server 与 replica_server不在同一日志目录
            # (2) replica server集群本身数量没有超过3个，就把所有弄到的日志信息dump到本地

            # dump replica server startup log
            one_startup_log_command_template = "egrep " + regex_gpid + " " + "%s" + " | tail -n 10"
            self._dump_remote_startup_log_message_to_local(module=self.module, role=SKV_REPLICA_SERVER_ROLE_NAME,
                                                           log_shell_template=one_startup_log_command_template,
                                                           server_list=replica_server_list)

            # dump replica server fixed size log
            one_daily_log_command_template = "egrep " + regex_gpid + " " + "%s" + " | tail -n 10"
            self._dump_remote_log_message_to_local(module=self.module, role=SKV_REPLICA_SERVER_ROLE_NAME,
                                                   log_shell_template=one_daily_log_command_template,
                                                   server_list=replica_server_list)

        else:
            startup_log_path = get_program_start_log_path_by_role(self.module, SKV_REPLICA_SERVER_ROLE_NAME)
            for replica_server in replica_server_list:
                msg = '{replica_server} current server startup log path is {path}\n You can read it .'.format(
                    replica_server=replica_server,
                    path=startup_log_path
                )
                self.logger.warn(msg)

            one_replica_log_dir = get_server_log_dir_by_role(self.module, SKV_REPLICA_SERVER_ROLE_NAME)
            msg = 'replica_server current server daily log dir is {dir}\nFind the last one log file read it .'.format(
                dir=one_replica_log_dir
            )
            self.logger.warn(msg)
