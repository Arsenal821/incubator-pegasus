#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import datetime
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_log_check_worker import BaseLogCheckWorker, LogMatcher

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'construction_blueprint'))
from skv_common import SKV_PRODUCT_NAME, SKV_REPLICA_SERVER_ROLE_NAME, is_skv_in_mothership


class SecondaryTimeoutLogMatcher(LogMatcher):
    """副本同步超时
E2021-08-14 12:37:25.346 (1628915845346722481 4e44) replica.replica0.020400000000068e: replica_failover.cpp:68:handle_remote_failure(): 3.3@10.2.1.163:8171: handle remote failure caused by prepare, error = ERR_TIMEOUT, status = replication::partition_status::PS_SECONDARY, node = 10.2.1.166:8171
    """
    def __init__(self):
        pattern = r'E([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}).*handle remote failure caused by prepare, error = ERR_TIMEOUT, status = replication::partition_status::PS_SECONDARY'
        super().__init__(pattern, 'secondary_timeout')

    def conclusion(self, module, all_hosts):
        if not self.host_to_match_result_list:
            return ''
        ret = '%d/%d hosts matcheds' % (len(self.host_to_match_result_list), len(all_hosts))
        # 详细分析某个单个节点的日志
        one_host = list(self.host_to_match_result_list.keys())[0]
        ret += '; start analyse host[%s] time range:\n' % one_host
        # 匹配一下相关的时间段 这是个session切割算法。。10分钟以内算是一个session
        datetime_ragnes = []
        start_time, last_time = None, None
        for match_result in self.host_to_match_result_list[one_host]:
            log_time = datetime.datetime.strptime(match_result.group(1), '%Y-%m-%d %H:%M:%S')
            if not start_time:  # 第一次执行
                start_time, last_time = log_time, log_time
            elif (log_time - last_time).total_seconds() > 600:  # 距离上次超过10分钟 session切割
                datetime_ragnes.append((start_time, last_time))
                start_time, last_time = log_time, log_time
            else:
                last_time = log_time
        if start_time:
            datetime_ragnes.append((start_time, last_time))
        ret += ', '.join(['%s-%s' % (f, t) for (f, t) in datetime_ragnes])
        # MS2 环境去掉了 sp2.1 下的监控项, 这里提示去大盘看 health status of partitions 指标
        if is_skv_in_mothership(module):
            msg = 'Please go to skv-grafana to query the prometheus monitoring indicator [health status of partitions], check whether frequently unhealthy'
            ret += '\n%s' % msg
        else:
            cmd = 'spadmin alarm list -p %s 2>&1 | grep unhealthy_partition_count' % SKV_PRODUCT_NAME
            ret += '\nplease execute[%s] to see if partition frequently unhealthy' % cmd
        # 输出提示 检查cpu和io
        ret += '\nplease check io[iostat -x 1 10] or cpu[sar -u] to see if machine is overloaded'
        # 输出提示修改超时 不输出命令 避免实施无脑调整超时
        ret += '\nyou can change prepare_timeout_ms_for_potential_secondaries/prepare_timeout_ms_for_secondaries temporarily'
        return ret


class CheckReplicaServerLogWorker(BaseLogCheckWorker):
    role = SKV_REPLICA_SERVER_ROLE_NAME
    matcher_list = [
        SecondaryTimeoutLogMatcher(),  # 检查副本同步超时
    ]
