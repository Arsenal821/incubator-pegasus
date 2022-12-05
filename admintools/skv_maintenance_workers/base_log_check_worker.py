#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import logging
import os
import re
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_program_start_log_path_by_role
from skv_maintenance_workers.base_worker import BaseWorker
from recipes import get_skv_config_manager


class LogMatcher:
    def __init__(self, re_str, name, max_line_count=1000):
        """参数是 正则匹配参数 和 规则名称"""
        self.pattern = re.compile(re_str)
        self.name = name
        self.max_line_count = max_line_count
        self.host_to_match_result_list = {}

    def match(self, host, line):
        """如果对应的行匹配正则，则记录到结果里面"""
        match_result = self.pattern.search(line)
        if match_result:
            if host not in self.host_to_match_result_list:
                self.host_to_match_result_list[host] = []
            self.host_to_match_result_list[host].append(match_result)

    def conclusion(self, module, all_hosts):
        """返回结论"""
        if self.host_to_match_result_list:
            return '%s %d/%d hosts matcheds' % (module, len(self.host_to_match_result_list), len(all_hosts))
        else:
            return ''


class BaseLogCheckWorker(BaseWorker):
    role = None
    matcher_list = None
    max_line_count = 1000

    def get_log_files(self):
        """返回机器对应的文件 默认返回的是启动日志"""
        return [get_program_start_log_path_by_role(self.module, self.role)]

    def copy_log_files_to_local(self, host):
        """将必要的日志拷贝到本地"""
        local_log_files = []
        for log_file in self.get_log_files():
            cmd = 'tail -n %d %s' % (self.max_line_count, log_file)
            local_log = self._dump_remote_shell_command_result_to_local_log(self.module, self.role, 'start_log', {host: cmd}, logging.DEBUG)
            local_log_files.append(local_log)
        self.logger.debug('dumpped %s %d logs to local: %s' % (host, len(local_log_files), local_log_files))
        return local_log_files

    def check_log_on_host(self, host, log_files):
        """检查某个机器上的一系列日志是否匹配错误"""
        # 遍历所有日志
        for log_file in log_files:
            with open(log_file) as f:
                # 遍历所有行
                for line in f:
                    # 尝试匹配各个规则
                    for matcher in self.matcher_list:
                        matcher.match(host, line)

    def is_state_abnormal(self):
        skv_config_manager = get_skv_config_manager(self.module, self.role, self.logger)
        # 遍历主机和日志解析
        host_lists = skv_config_manager.get_host_list()
        for host in host_lists:
            log_files = self.copy_log_files_to_local(host)
            self.check_log_on_host(host, log_files)
        # 输出结论
        abnormal = False
        for matcher in self.matcher_list:
            conclusion = matcher.conclusion(self.module, host_lists)
            if conclusion:
                self.logger.info('matched %s: %s' % (matcher.name, conclusion))
                abnormal = True
        return abnormal

    def diagnose(self):
        self.logger.info('diagnose is printed as above')

    def repair(self):
        self.logger.info('diagnose is printed as above')
