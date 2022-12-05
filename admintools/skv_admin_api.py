#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

封装skv的shell命令
"""
import json
import re
import os
import signal
import subprocess
import sys
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))

SKV_OFFLINE_MODULE_NAME = 'skv_offline'
SKV_REPLICA_SERVER_ROLE_NAME = 'replica_server'

# 在容器中启动实例尽可能少的引用 hyersion 接口，这里使用 skv_shim 自己背的 shell_wrapper
sys.path.append(os.path.join(os.environ['SKV_HOME'], 'skv_shim/scripts/skv_utils'))
from shell_wrapper import check_call, check_output


class SkvAdminApi:
    # SkvAdminApi.get_meta_level() 的返回值
    META_LEVEL_LIVELY = 'lively'
    META_LEVEL_STEADY = 'steady'
    # 这些定义了app_stat -q -j 读写的操作是哪些字段
    APP_QPS_STAT_WRITE_COLUMN_LIST = ['PUT', 'MPUT', 'DEL', 'MDEL', 'INCR', 'CAS', 'CAM']
    APP_QPS_STAT_READ_COLUMN_LIST = ['GET', 'MGET', 'BGET', 'SCAN']
    APP_QPS_STAT_OP_COLUMN_LIST = APP_QPS_STAT_WRITE_COLUMN_LIST + APP_QPS_STAT_READ_COLUMN_LIST

    def is_skv_in_mothership(self, module_name):
        """
        判断module_name对应的模块是不是被mothership管理
        在容器中启动实例尽可能少的引用 hyersion 接口，skv_common 中有大量引用
        故这里拷贝 skv_common 中的实现, 未引用 skv_common 的接口
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

    def __init__(self, logger, cluster_name=SKV_OFFLINE_MODULE_NAME, meta_server_endpoint=None):
        self.logger = logger
        self.cluster_name = cluster_name
        if meta_server_endpoint:
            self.meta_server_endpoint = meta_server_endpoint
        else:
            from recipes.platform_adapter import _get_client_conf
            self.meta_server_endpoint = ','.join(_get_client_conf(self.cluster_name, 'meta_server_list'))

        self.skv_root = os.path.join(os.environ['SKV_HOME'], self.cluster_name)
        if self.is_skv_in_mothership(cluster_name):
            self.skv_tool_run_script = os.path.join(self.skv_root, SKV_REPLICA_SERVER_ROLE_NAME, 'tools/run.sh')
        else:
            self.skv_tool_run_script = os.path.join(self.skv_root, 'tools/run.sh')
        self.skv_shell_output_pattern = re.compile(r'''The config file is: (.*)
The cluster name is: (.*)
The cluster meta list is: ([^\n]*)
(.*)
dsn exit with code ([0-9]+)''', re.MULTILINE | re.DOTALL)

    def get_big_table_default_partition_count(self, min_replica_per_disk=2):
        """获取大表的 partition个数 TODO 这个和建表逻辑有重复 但是建表接口维护在sp"""
        from recipes.platform_adapter import _get_client_conf
        replica_factor = _get_client_conf(self.cluster_name, 'partition_factor')
        replica_factor = replica_factor * min_replica_per_disk
        if replica_factor <= 8:
            return 8
        elif replica_factor <= 16:
            return 16
        elif replica_factor <= 32:
            return 32
        elif replica_factor <= 64:
            return 64
        elif replica_factor <= 172:
            return 128
        else:
            return 256

    def _get_execute_shell_stdout_and_stderr(self, exec_cmd, timeout=600):
        '''pegasus shell真nb
正常情况下 日志在stderr 有效输出在stdout
而pegasus 日志和输出 有时候 一起在stdout 有时候 日志打在stdout 有效输出打在stderr
不知道内部咋解析的 反正我们生成两个方法 一个是获取清洗后的stdout 一个是获取完整的stderr
        '''
        # 设置不产生core; skv shell动不动就core 很容易打满磁盘 而且我们基本不会看这个core 因此都关掉
        cmd = 'ulimit -c 0;%s shell --cluster %s' % (self.skv_tool_run_script, self.meta_server_endpoint)
        self.logger.debug('skv shell command: %s' % cmd)
        self.logger.debug('start execute command: %s' % exec_cmd)
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                stdin=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            out, err = proc.communicate(exec_cmd.encode('utf-8'), timeout=timeout)
        except subprocess.TimeoutExpired as e:
            os.killpg(proc.pid, signal.SIGKILL)
            out, err = proc.communicate()
            self.logger.debug('timeout!')
            self.logger.debug('stdout:\n%s' % out)
            self.logger.debug('stderr:\n%s' % err)
            raise e
        out, err = out.decode('utf-8'), err.decode('utf-8')
        self.logger.debug('return: %d' % proc.returncode)
        self.logger.debug('stdout:\n%s' % out)
        self.logger.debug('stderr:\n%s' % err)
        return out, err

    def _shell_output_strip(self, output):
        '''获取清洗后的stdout

stdout举例:
W1970-01-01 08:00:00.000 (0 1b57) unknown.io-thrd.06999: overwrite default thread pool for task RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX from THREAD_POOL_META_SERVER to THREAD_POOL_DEFAULT
W1970-01-01 08:00:00.000 (0 1b57) unknown.io-thrd.06999: overwrite default thread pool for task RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX_ACK from THREAD_POOL_META_SERVER to THREAD_POOL_DEFAULT
Pegasus Shell 1.12.3-0.4.0
Type "help" for more information.
Type "Ctrl-D" or "Ctrl-C" to exit the shell.

The config file is: /sensorsdata/main/program/sp/skv_offline/tools/config-shell.ini.6989
The cluster name is: skv_offline
The cluster meta list is: 10.129.130.228:8170,10.129.138.17:8170,10.129.141.26:8170
[cluster_info]
meta_servers                  : 10.129.130.228:8170,10.129.138.17:8170,10.129.141.26:8170
primary_meta_server           : 10.129.141.26:8170
zookeeper_hosts               : hybrid02.debugboxcreate3958.sensorsdata.cloud:2181,hybrid03.debugboxcreate3958.sensorsdata.cloud:2181,hybrid01.debugboxcreate3958.sensorsdata.cloud:2181
zookeeper_root                : /sensors_analytics/backpack/sp/skv_offline
meta_function_level           : steady
balance_operation_count       : move_pri=0,copy_pri=0,copy_sec=0,total=0
primary_replica_count_stddev  : 0.00
total_replica_count_stddev    : 0.00
dsn exit with code 0

返回的部分:
[cluster_info]
meta_servers                  : 10.129.130.228:8170,10.129.138.17:8170,10.129.141.26:8170
primary_meta_server           : 10.129.141.26:8170
zookeeper_hosts               : hybrid02.debugboxcreate3958.sensorsdata.cloud:2181,hybrid03.debugboxcreate3958.sensorsdata.cloud:2181,hybrid01.debugboxcreate3958.sensorsdata.cloud:2181
zookeeper_root                : /sensors_analytics/backpack/sp/skv_offline
meta_function_level           : steady
balance_operation_count       : move_pri=0,copy_pri=0,copy_sec=0,total=0
primary_replica_count_stddev  : 0.00
total_replica_count_stddev    : 0.00
        '''
        g = self.skv_shell_output_pattern.search(output)
        if not g:
            self.logger.error('cannot read skv shell output:\n%s' % output)
            raise Exception('invalid output!')
        exit_code = int(g.group(5))
        if exit_code != 0:
            self.logger.error('execute result\n%s' % output)
            raise Exception('dsn exit code %s' % exit_code)
        self.logger.debug('result:\n%s' % g.group(4))
        return g.group(4)

    def _get_execute_shell_output(self, exec_cmd, timeout=600, host=None):
        out, _ = self._get_execute_shell_stdout_and_stderr(exec_cmd, timeout)
        return self._shell_output_strip(out)

    def _get_cluster_info(self):
        """获取cluster_info结构化输出 返回下面这个json
>>> cluster_info -j
{
    "cluster_info": {
        "meta_servers": "10.129.130.228:8170,10.129.138.17:8170,10.129.141.26:8170",
        "primary_meta_server": "10.129.141.26:8170",
        "zookeeper_hosts": "hybrid02.debugboxcreate3958.sensorsdata.cloud:2181,hybrid03.debugboxcreate3958.sensorsdata.cloud:2181,hybrid01.debugboxcreate3958.sensorsdata.cloud:2181",
        "zookeeper_root": "/sensors_analytics/backpack/sp/skv_offline",
        "meta_function_level": "steady",
        "balance_operation_count": "move_pri=0,copy_pri=0,copy_sec=0,total=0",
        "primary_replica_count_stddev": "0.00",
        "total_replica_count_stddev": "0.00"
    }
}
        """
        output = self._get_execute_shell_output('cluster_info -j')
        return json.loads(output)['cluster_info']

    def get_primary_meta_server(self):
        """返回当前primary的meta server"""
        return self._get_cluster_info()['primary_meta_server']

    def get_primary_and_total_replica_count_stddev(self):
        """返回均衡的方差指标 primary_replica_count_stddev, total_replica_count_stddev"""
        cluster_info = self._get_cluster_info()
        return float(cluster_info['primary_replica_count_stddev']), float(cluster_info['total_replica_count_stddev'])

    def get_balance_operation_count(self):
        """返回balance步骤数"""
        return self._get_cluster_info()['balance_operation_count']

    def get_meta_level(self):
        """返回meta_function_level"""
        return self._get_cluster_info()['meta_function_level']

    def set_meta_level(self, level):
        """设置均衡策略 level可选为META_LEVEL_LIVELY META_LEVEL_STEADY
>>> set_meta_level lively
control meta level ok, the old level is fl_steady
        """
        output = self._get_execute_shell_output('set_meta_level %s' % level)
        if 'control meta level ok' not in output:
            self.logger.error('invalid response: %s' % output)
            raise Exception('set meta level to %s failed' % level)

    def get_cluster_root(self):
        """获取zk跟路径"""
        return self._get_cluster_info()['zookeeper_root']

    def _get_nodes_details(self):
        """获取nodes -d结构化输出 返回下面这个json
>>> nodes -j -d
{
    "details": {
        "10.129.130.228:8171": {
            "address": "10.129.130.228:8171",
            "status": "ALIVE",
            "replica_count": "72",
            "primary_count": "24",
            "secondary_count": "48"
        },
        "10.129.138.17:8171": {
            "address": "10.129.138.17:8171",
            "status": "ALIVE",
            "replica_count": "72",
            "primary_count": "24",
            "secondary_count": "48"
        },
        "10.129.141.26:8171": {
            "address": "10.129.141.26:8171",
            "status": "ALIVE",
            "replica_count": "72",
            "primary_count": "24",
            "secondary_count": "48"
        }
    },
    "summary": {
        "total_node_count": "3",
        "alive_node_count": "3",
        "unalive_node_count": "0"
    }
}
        """
        output = self._get_execute_shell_output('nodes -j -d')
        return json.loads(output)

    def _get_replica_server_details(self, server_endpoint):
        return self._get_nodes_details()['details'][server_endpoint]

    def get_primary_count_on_server(self, server_endpoint):
        """给定replica server endpoint，返回当前的primary count"""
        return int(self._get_replica_server_details(server_endpoint)['primary_count'])

    def get_replica_count_on_server(self, server_endpoint):
        """给定replica server endpoint，返回当前的replica count"""
        return int(self._get_replica_server_details(server_endpoint)['replica_count'])

    def get_replica_server_num(self):
        """返回集群的replica server个数"""
        return int(self._get_nodes_details()['summary']['total_node_count'])

    def get_replica_server_status(self, server_endpoint):
        """获取单个replica server的状态"""
        return self._get_replica_server_details(server_endpoint)['status']

    def get_all_replica_server(self):
        """返回集群所有的replica server"""
        return list(self._get_nodes_details()['details'].keys())

    def _get_all_table_info(self):
        """获取ls -j的结构化输出 返回下面的json
>>> ls -j
{
    "general_info": {
        "1": {
            "app_id": "1",
            "status": "AVAILABLE",
            "app_name": "__detect",
            "app_type": "pegasus",
            "partition_count": "8",
            "replica_count": "3",
            "is_stateful": "true",
            "create_time": "2021-05-11_11:13:25",
            "drop_time": "-",
            "drop_expire": "-",
            "envs_count": "0"
        },
        "2": {
            "app_id": "2",
            "status": "AVAILABLE",
            "app_name": "__stat",
            "app_type": "pegasus",
            "partition_count": "8",
            "replica_count": "3",
            "is_stateful": "true",
            "create_time": "2021-05-11_11:13:25",
            "drop_time": "-",
            "drop_expire": "-",
            "envs_count": "0"
        },
        ...
        "10": {
            "app_id": "10",
            "status": "AVAILABLE",
            "app_name": "sdf_last_seen_time",
            "app_type": "pegasus",
            "partition_count": "8",
            "replica_count": "3",
            "is_stateful": "true",
            "create_time": "2021-05-11_11:29:36",
            "drop_time": "-",
            "drop_expire": "-",
            "envs_count": "0"
        }
    },
    "summary": {
        "total_app_count": "10"
    }
}
        """
        output = self._get_execute_shell_output('ls -j')
        return json.loads(output)

    def get_table_count(self):
        """返回集群中表个数"""
        return int(self._get_all_table_info()['summary']['total_app_count'])

    def get_cluster_replica_count(self):
        """返回集群中所有分片个数（对每个副本partition*replica)"""
        count = 0
        for table_info in self._get_all_table_info()['general_info'].values():
            self.logger.debug('table {app_name} has {partition_count} partitions and {replica_count} replicas' % table_info)
            count += int(table_info['partition_count']) * int(table_info['replica_count'])
        return count

    def get_cluster_replica_count_range(self):
        """返回集群中分片数量的范围 返回tuple(min_replica_count, max_replica_count)"""
        replica_count_list = [int(table_info['replica_count']) for table_info in self._get_all_table_info()['general_info'].values()]
        return min(replica_count_list), max(replica_count_list)

    def _get_table_detail_info(self):
        """返回ls -d的结果json格式化的值
>>> ls -d -j
{
    "general_info": {
        "1": {
            "app_id": "1",
            "status": "AVAILABLE",
            "app_name": "__detect",
            "app_type": "pegasus",
            "partition_count": "8",
            "replica_count": "3",
            "is_stateful": "true",
            "create_time": "2020-12-18_14:34:53",
            "drop_time": "-",
            "drop_expire": "-",
            "envs_count": "0"
        },
        ...
    "healthy_info": {
        ...
        "168": {
            "app_id": "168",
            "app_name": "sfo_popup_p63",
            "partition_count": "8",
            "fully_healthy": "8",
            "unhealthy": "0",
            "write_unhealthy": "0",
            "read_unhealthy": "0"
        }
    },
    "summary": {
        "total_app_count": "162",
        "fully_healthy_app_count": "162",
        "unhealthy_app_count": "0",
        "write_unhealthy_app_count": "0",
        "read_unhealthy_app_count": "0"
    }
}
        """
        output = self._get_execute_shell_output('ls -d -j')
        return json.loads(output)

    def get_unhealthy_app_count(self):
        return int(self._get_table_detail_info()['summary']['unhealthy_app_count'])

    def get_write_unhealthy_app_count(self):
        return int(self._get_table_detail_info()['summary']['write_unhealthy_app_count'])

    def get_read_unhealthy_app_count(self):
        return int(self._get_table_detail_info()['summary']['read_unhealthy_app_count'])

    def _send_remote_command(self, success_pattern, command_key,
                             command_value='', send_to_role=None, send_to_addr=None):
        """发送远程命令 注意这个输出结果到stderr上了 鬼知道为啥
>>> remote_command -t meta-server meta.lb.assign_delay_ms 10
COMMAND: meta.lb.assign_delay_ms 10

CALL [meta-server] [10.120.9.250:8710] succeed: OK
CALL [meta-server] [10.120.9.250:8711] succeed: unknown command 'meta.lb.assign_delay_ms'
CALL [meta-server] [10.120.9.250:8712] succeed: unknown command 'meta.lb.assign_delay_ms'

Succeed count: 3
Failed count: 0
        """
        if send_to_role:
            send_to = '-t %s' % send_to_role
        elif send_to_addr:
            send_to = '-l %s' % send_to_addr
        else:
            raise Exception('please specify send to role/addr!')
        cmd = 'remote_command %s %s %s' % (send_to, command_key, command_value)
        _, output = self._get_execute_shell_stdout_and_stderr(cmd)
        g = re.search(success_pattern, output, re.MULTILINE)
        if not g:
            self.logger.error('cmd %s, invalid output: %s' % (cmd, output))
            raise Exception('cannot read output of command %s' % cmd)

    def _send_remote_command_to_meta_server(self, command_key, command_value=''):
        """给meta server发送远程命令"""
        self._send_remote_command(r'CALL \[meta-server\].*succeed: .*[ok|OK]',
                                  command_key, command_value, send_to_role='meta-server')

    def set_replica_server_black_list(self, addr):
        """给replica server设置黑名单"""
        self._send_remote_command_to_meta_server('meta.lb.assign_secondary_black_list', addr)

    def get_replica_server_black_list(self):
        """获取设置的replica server的黑名单"""
        cmd = "remote_command -t meta-server meta.lb.assign_secondary_black_list"
        _, output = self._get_execute_shell_stdout_and_stderr(cmd)
        for line in output.split('\n'):
            if 'get ok' in line:
                return list() if line.split()[-1] == 'ok:' else line.split()[-1].split(',')

    def set_lb_assign_delay_ms(self, ms):
        """设置多久补充副本"""
        self._send_remote_command_to_meta_server('meta.lb.assign_delay_ms', str(ms))

    def _send_remote_command_to_node(self, addr, command_key, command_value=''):
        """给单个节点发送远程命令"""
        self._send_remote_command(r'Succeed count: 1\nFailed count: 0\n',
                                  command_key, command_value, send_to_addr=addr)

    def set_add_secondary_max_count_for_one_node(self, cnt):
        """设置为0 则禁掉add_secondary操作 设置为DEFAULT恢复"""
        return self._send_remote_command_to_meta_server('meta.lb.add_secondary_max_count_for_one_node', str(cnt))

    def kill_partition(self, node):
        """通过shell向replica server发送远程命令，将所有replica都关闭，以触发flush操作，将数据都落地"""
        self._send_remote_command_to_node(node, 'replica.kill_partition')

    def flush_logging(self, node):
        """flush logging 这个是在文档中没有写 但是王聃在pegasus脚本里面发现的"""
        self._send_remote_command_to_node(node, 'flush-log')

    def _get_replica_count_by_status_pattern(self, node, pattern, shell_timeout=None):
        """获取某个节点上某个类型的副本个数 通过正则表达式匹配"""
        cmd = ("timeout {timeout}\n".format(timeout=shell_timeout) if shell_timeout else "") + "remote_command -l {node} perf-counters '.*replica(Count)'".format(
            node=node)
        _, error = self._get_execute_shell_stdout_and_stderr(cmd)
        if not error:
            raise Exception(
                "stderr is empty while executing '{cmd}'".format(cmd=cmd)
            )

        output_lines = error.split('\n')
        for line in output_lines:
            match = re.search(pattern, line)
            if not match:
                continue
            return int(match.group(1))
        raise Exception("failed to extract replica count from perf counters")

    def get_serving_replica_count(self, node, shell_timeout=None):
        return self._get_replica_count_by_status_pattern(
            node, r'replica_stub.replica\(Count\)","type":"NUMBER","value":([0-9]*)', shell_timeout)

    def get_opening_replica_count(self, node):
        return self._get_replica_count_by_status_pattern(
            node, r'replica_stub.opening.replica\(Count\)","type":"NUMBER","value":([0-9]*)'
        )

    def get_closing_replica_count(self, node):
        return self._get_replica_count_by_status_pattern(
            node, r'replica_stub.closing.replica\(Count\)","type":"NUMBER","value":([0-9]*)'
        )

    def move_all_primary_on_host(self, replica_server_addr):
        """将某个replica server上的所有primary replica挪走 pegasus提供了一个工具执行"""
        cmd = 'ulimit -c 0; %s migrate_node --cluster %s -n %s' % (self.skv_tool_run_script, self.meta_server_endpoint, replica_server_addr)
        check_call(cmd, self.logger.debug)
        cmd += ' -t run'
        check_call(cmd, self.logger.debug, timeout=1200)

    def inactive_all_replica_on_host(self, replica_server_addr):
        """将某个replica server上所有的副本降级 pegasus提供了一个工具执行"""
        cmd = 'ulimit -c 0; %s downgrade_node --cluster %s -n %s' % (self.skv_tool_run_script, self.meta_server_endpoint, replica_server_addr)
        check_call(cmd, self.logger.debug)
        cmd += ' -t run'
        check_call(cmd, self.logger.debug, timeout=1200)

    def get_unhealthy_app_list(self):
        """返回所有不健康的表名"""
        healthy_info_map = self._get_table_detail_info()['healthy_info']
        return [kv['app_name'] for kv in healthy_info_map.values() if kv['fully_healthy'] != kv['partition_count']]

    def get_cluster_all_node_quota_details(self):
        """
>>> nodes -q -j
{
    "details": {
        "10.120.233.36:8171": {
            "address": "10.120.233.36:8171",
            "status": "ALIVE",
            "get_qps": "0.00",
            "mget_qps": "0.00",
            "bget_qps": "0.00",
            "read_cu": "0.00",
            "put_qps": "0.00",
            "mput_qps": "0.00",
            "write_cu": "0.00",
            "get_p99(ms)": "2.10",
            "mget_p99(ms)": "0.00",
            "bget_p99(ms)": "1.27",
            "put_p99(ms)": "9.00",
            "mput_p99(ms)": "0.00"
        }
    },
    "summary": {
        "total_node_count": "1",
        "alive_node_count": "1",
        "unalive_node_count": "0"
    }
}
        """
        output = self._get_execute_shell_output('nodes -q -j')
        return json.loads(output)['details']

    def _get_cluster_node_list_by_status(self, status):
        """返回的是ip:port的list"""
        return [addr for addr, kv in self._get_nodes_details()['details'].items() if kv['status'] == status]

    def get_alive_node_list(self):
        return self._get_cluster_node_list_by_status('ALIVE')

    def get_unalive_node_list(self):
        return self._get_cluster_node_list_by_status('UNALIVE')

    def get_meta_server_list_by_status(self, status):
        '''根据meta状态(Failed/Succeed)获取 addr list'''
        _, error = self._get_execute_shell_stdout_and_stderr('server_info')
        count_pat = re.compile('%s count: \\d+' % status, re.DOTALL)
        count_line = count_pat.findall(error)
        if len(count_line) == 0:
            raise Exception(self.cluster_name + ' is not running')
        index = 8 + len(status)
        meta_server_count = count_line[0][index:]
        if meta_server_count == '0':
            return []

        meta_server_pat = re.compile(r'CALL \[meta-server\] \[(\S*)\] %s' % status.lower(), re.DOTALL)
        return meta_server_pat.findall(error)

    def get_unalive_meta_server_list(self):
        return self.get_meta_server_list_by_status('Failed')

    def get_alive_meta_server_list(self):
        return self.get_meta_server_list_by_status('Succeed')

    @staticmethod
    def _exclude_total_from_all_stats(all_stat_map):
        app_stat_map = {}
        for key, obj in all_stat_map.items():
            if obj.app_name.startswith('(total') and int(obj.app_id) == 0:
                continue

            app_stat_map[key] = obj

        return app_stat_map

    def _get_table_to_opstat(self):
        """
>>> app_stat -q -j
{
    "app_stat": {
        "__detect": {
            "app_name": "__detect",
            "app_id": "1",
            "pcount": "8",
            "GET": "0.00",
            "MGET": "0.00",
            "BGET": "0.00",
            "PUT": "0.00",
            "MPUT": "0.00",
            "DEL": "0.00",
            "MDEL": "0.00",
            "INCR": "0.00",
            "CAS": "0.00",
            "CAM": "0.00",
            "SCAN": "0.00",
            "RCU": "0.00",
            "WCU": "0.00",
            "expire": "0.00",
            "filter": "0.00",
            "abnormal": "0.00",
            "delay": "0.00",
            "reject": "0.00",
            "hit_rate": "0.00"
        },
        ...
                "(total:3)": {
            "app_name": "(total:3)",
            "app_id": "0",
            "pcount": "24",
            "GET": "0.00",
            "MGET": "0.00",
            "BGET": "0.00",
            "PUT": "0.00",
            "MPUT": "0.00",
            "DEL": "0.00",
            "MDEL": "0.00",
            "INCR": "0.00",
            "CAS": "0.00",
            "CAM": "0.00",
            "SCAN": "0.00",
            "RCU": "0.00",
            "WCU": "0.00",
            "expire": "0.00",
            "filter": "0.00",
            "abnormal": "0.00",
            "delay": "0.00",
            "reject": "0.00",
            "hit_rate": "0.00"
        }
    }
}


返回table->{op: value}
        """
        d = self.get_app_stat('app_stat -q -j')
        # 去掉最后的total行 反正正常表名是不能包含括号的
        return {table: opstat for table, opstat in d['app_stat'].items() if not table.startswith('(total')}

    @classmethod
    def _has_ops(cls, stat):
        for op in cls.APP_QPS_STAT_OP_COLUMN_LIST:
            if stat[op] != '0.00':
                return True
        return False

    def get_all_tables_has_op(self):
        """返回所有有读写操作的表的列表"""
        return [table for table, stat in self._get_table_to_opstat().items() if self._has_ops(stat)]

    def check_table_has_ops(self, table_name):
        """检查单个表是否有读写操作"""
        return self._has_ops(self._get_table_to_opstat()[table_name])

    def get_table_to_quota_by_op(self, op):
        """获取所有包含某种读写操作的 表->值"""
        return {table: stat[op] for table, stat in self._get_table_to_opstat().items() if stat[op] != '0.00'}

    def get_server_version(self, addr):
        """获取单个 server 的版本信息
            参数为 addr(ip:port), 对应 meta_server或者replica_server 一个具体的实例
        """
        cmd = 'server_info'
        _, error = self._get_execute_shell_stdout_and_stderr(cmd)
        if not error:
            raise Exception(
                "stderr is empty while executing '{cmd}'".format(cmd=cmd)
            )

        version = None
        output_lines = error.split('\n')
        for line in output_lines:
            line = line.lower()

            if line.find('failed') >= 0 and line.find('call') >= 0:
                raise Exception(
                    "failed to get version in {cluster}, "
                    "because execute \"server_info\" err: {output}".format(
                        cluster=self.cluster_name,
                        output=line.split()[-1].upper(),
                    )
                )

            begin = line.find(addr)
            if begin < 0:
                continue
            pos = line.find('succeed', begin)
            if pos < 0:
                continue

            begin = pos + len('succeed')

            pos = line.find('server', begin)
            if pos < 0:
                continue
            else:
                begin = pos + len('server')

            for end_keyword in ['release', 'debug']:
                end = line.find(end_keyword, begin)
                if end >= 0:
                    break
            else:
                continue

            version = line[begin:end].strip()
            if not version:
                raise Exception(
                    "there is no skv version info in {cluster}".format(
                        cluster=self.cluster_name)
                )
            return version

    def get_version(self, retry_num=1):
        """
>>> server_info
COMMAND: server-info

CALL [meta-server] [10.120.233.36:8170] succeed: Meta Server 1.12.3-0.6.0 (51f765f8c338fa094e951e2aa08eb5a410501a7c) Release, Started at 2022-02-18 10:30:36
CALL [replica-server] [10.120.233.36:8171] succeed: Replica Server 1.12.3-0.6.0 (51f765f8c338fa094e951e2aa08eb5a410501a7c) Release, Started at 2022-02-18 10:30:37

Succeed count: 2
Failed count: 0
        """
        cmd = "server_info"
        # 返回1.12.3-0.6.0 (51f765f8c338fa094e951e2aa08eb5a410501a7c)
        pattern = r'CALL .* succeed: (Meta Server|Replica Server) ([0-9\.\-]+ \([a-z0-9]+\)) (Release|Debug).*'
        while retry_num > 0:
            retry_num -= 1
            _, error = self._get_execute_shell_stdout_and_stderr(cmd)
            lines = re.findall(pattern, error, re.MULTILINE)
            if not lines:
                self.logger.warning('invalid stderr: %s' % error)
                if retry_num > 0:
                    time.sleep(5)
                continue
            # [('Meta Server', '1.12.3-0.6.0 (51f765f8c338fa094e951e2aa08eb5a410501a7c)', 'Release'), ('Replica Server', '1.12.3-0.6.0 (51f765f8c338fa094e951e2aa08eb5a410501a7c)', 'Release')]
            distinct_versions = set([x[1] for x in lines])
            if len(distinct_versions) != 1:
                raise Exception('there are %d different skv versions in %s! %s' % (
                    len(distinct_versions), self.cluster_name, distinct_versions))
            return list(distinct_versions)[0]
        raise Exception('failed to get version!')

    def create_table(self, table_name, partition_count, replica_count):
        """建表命令 注意这个命令实际会ssh到机器上执行"""
        self.logger.info('start to create table[%s] partition_count[%d] replica_count[%d]' % (table_name, partition_count, replica_count))
        cmd = 'create %s -p %d -r %d' % (table_name, partition_count, replica_count)
        output = self._get_execute_shell_output(cmd)
        success_log = 'create app "%s" succeed' % table_name
        if success_log in output:
            self.logger.info('end created table[%s] partition_count[%d] replica_count[%d]' % (table_name, partition_count, replica_count))
        else:
            self.logger.error('bad stdout: %s' % output)
            raise Exception('failed to create table %s!' % table_name)

    def count_table(self, table_name, timeout=None, precise=True):
        """计算行号 注意这个命令实际会ssh到机器上执行
2.0+以后的版本需要加-c true/false 指定是否精确count
        """
        # -t后面的超时是api超时时间 不是整体超时时间 国豪已和春老师+杜老师确认
        cmd = 'use %s\ncount_data -t 200000 -c %s' % (table_name, precise)
        # count_data必须在集群里面调用
        _, output = self._get_execute_shell_stdout_and_stderr(cmd, timeout)
        # Count done, total 415488 rows.
        last_line = output.strip().splitlines()[-1]
        g = re.match(r'Count done, total ([0-9]+) rows', last_line)
        if not g:
            self.logger.error('invalid output %s' % output)
            self.logger.error('last_line: %s' % last_line)
            raise Exception('failed to count_table %s: invalid output!' % table_name)
        return int(g.group(1))

    def set_kv(self, table, hash_key, sort_key, value, timeout=None):
        """写kv 注意这个命令实际会ssh到机器上执行"""
        cmd = 'use %s\nset %s %s %s' % (table, hash_key, sort_key, value)
        _, output = self._get_execute_shell_stdout_and_stderr(cmd, timeout)
        lines = [x.strip() for x in output.strip().splitlines()]
        if lines[0] == 'OK' and lines[1] == 'OK':
            return
        self.logger.error('invalid output: %s' % output)
        raise Exception('failed to set kv!table[%s] hash_key[%s] sort_key[%s] value[%s]' % (
            table, hash_key, sort_key, value))

    def get_kv(self, table, hash_key, sort_key, timeout=None):
        """读kv 注意这个命令实际会ssh到机器上执行"""
        cmd = 'use %s\nget %s %s' % (table, hash_key, sort_key)
        _, output = self._get_execute_shell_stdout_and_stderr(cmd, timeout)
        lines = [x.strip() for x in output.strip().splitlines()]
        if lines[0] == 'OK':
            return json.loads(lines[1])
        self.logger.error('invalid output: %s' % output)
        raise Exception('failed to get kv!table[%s] hash_key[%s] sort_key[%s]' % (table, hash_key, sort_key))

    def exist(self, table, hash_key, sort_key, timeout=None):
        """判断某条数据是否存在"""
        cmd = 'use %s\nexist %s %s' % (table, hash_key, sort_key)
        _, output = self._get_execute_shell_stdout_and_stderr(cmd, timeout)
        lines = [x.strip() for x in output.strip().splitlines()]
        if lines[0] == 'OK' and len(lines) > 1:
            return lines[1]
        self.logger.error('invalid output: %s' % output)
        raise Exception('failed to get whether kv exist!table[%s] hash_key[%s] sort_key[%s]' % (table, hash_key, sort_key))

    def get_active_replica_count_on_host(self, replica_server_addr):
        """查看需要降级的副本个数 使用Pegasus工具的dry run查看"""
        cmd = 'ulimit -c 0; %s downgrade_node --cluster %s -n %s' % (self.skv_tool_run_script, self.meta_server_endpoint, replica_server_addr)
        output = check_output(cmd, self.logger.debug)
        pattern = re.compile(r'propose --gpid ([0-9\.]+)')
        all_pattern = pattern.findall(output, re.MULTILINE)
        self.logger.debug('find %d active replica: %s' % (len(all_pattern), all_pattern))
        return len(all_pattern)

    def get_table_id_by_name(self, table_name):
        for table_info in self._get_all_table_info()['general_info'].values():
            if table_name == table_info["app_name"]:
                return table_info["app_id"]
        raise Exception('table %s is not exist!!!' % (table_name))

    def get_all_avaliable_table_name(self):
        """获取当前 ls 输出的所有表名"""
        return [table_info["app_name"] for table_info in self._get_all_table_info()['general_info'].values()]

    def balance_move_primary(self, gpid, from_address, to_address):
        self._get_execute_shell_output("balance -g %s -p move_pri -f %s -t %s" % (
            gpid, from_address, to_address
        ))

    def get_table_primary_replica(self, table_name, replica_server):
        table_detail = json.loads(self._get_execute_shell_output('app %s -d -j' % table_name))
        app_id = table_detail['general']['app_id']
        gpid_list = []
        for replica_detail in table_detail['replicas'].values():
            if replica_detail['primary'] == replica_server:
                gpid_list.append('%s.%s' % (app_id, replica_detail['pidx']))
        return gpid_list

    def get_table_primary_map(self, table_name):
        """获取表的 primary replica 位置"""
        table_detail = json.loads(self._get_execute_shell_output('app %s -d -j' % table_name))
        app_id = table_detail['general']['app_id']
        primary_map = {}
        for replica_detail in table_detail['replicas'].values():
            primary_map['%s.%s' % (app_id, replica_detail['pidx'])] = replica_detail['primary']
        return primary_map

    def app_disk_used(self, table_name, timeout=None):
        """获得表的磁盘使用量(单位 MB)"""
        output = self._get_execute_shell_output('app_disk %s -j' % table_name, timeout)
        return float(json.loads(output)['result']['disk_used_for_all_replicas(MB)'])

    def check_all_avaliable_table_replica_count(self, min_replica_count):
        """检查是集群否存在 replica_count 不合规的表，有抛异常"""
        for table_info in self._get_all_table_info()['general_info'].values():
            if int(table_info['replica_count']) < min_replica_count:
                raise Exception('table %s replica_count less than %d, is %s' % (
                    table_info['app_name'], min_replica_count, table_info['replica_count']))

    def get_table_partition_count(self, table_name):
        """返回某个table的partition个数
>>> app sdf_id_mapping -j

{
    "parameters": {
        "app_name": "sdf_id_mapping",
        "detailed": "false"
    },
    "general": {
        "app_name": "sdf_id_mapping",
        "app_id": "15",
        "partition_count": "256",
        "max_replica_count": "3"
    }
}
        """
        output = self._get_execute_shell_output('app %s -j' % table_name)
        return int(json.loads(output)['general']['partition_count'])

    def get_table_replica_count(self, table_name):
        """返回某个表的副本数"""
        output = self._get_execute_shell_output('app %s -j' % table_name)
        return int(json.loads(output)['general']['max_replica_count'])

    def copy_table(self, from_table, to_table, timeout=None):
        """拷贝表 注意这个命令实际会ssh到机器上执行"""
        # -t后面的超时是api超时时间 不是整体超时时间 国豪已和春老师+杜老师确认
        cmd = 'use %s\ncopy_data -c %s -a %s -n -t 200000' % (from_table, self.cluster_name, to_table)
        # copy_data必须在集群里面调用
        _, output = self._get_execute_shell_stdout_and_stderr(cmd, timeout)
        # Copy done, total 6 rows.
        last_line = output.strip().splitlines()[-1]
        g = re.match(r'Copy done, total ([0-9]+) rows', last_line)
        if not g:
            self.logger.error('invalid output %s' % output)
            self.logger.error('last_line: %s' % last_line)
            raise Exception('failed to copy %s -> %s: invalid output!' % (from_table, to_table))
        return int(g.group(1))

    def drop_table(self, table_name):
        """删表命令 注意这个命令实际会ssh到机器上执行"""
        self.logger.info('start to drop table[%s]' % table_name)
        cmd = 'drop %s' % table_name
        output = self._get_execute_shell_output(cmd)
        success_log = 'drop app %s succeed' % table_name
        if success_log in output:
            self.logger.info('end drop table[%s]' % table_name)
        else:
            self.logger.error('bad stdout: %s' % output)
            raise Exception('failed to drop table %s!' % table_name)

    def get_app_id_by_table(self, table_name):
        """获取某个表的app id， app_id目前只有在recall之类的方法会用到"""
        output = self._get_execute_shell_output('app %s -j' % table_name)
        return json.loads(output)['general']['app_id']

    def recall_table(self, app_id, table_name):
        """恢复某个表 需要指定app_id 返回table_name"""
        self.logger.info('start to recall table %s->%s' % (app_id, table_name))
        cmd = 'recall %s %s' % (app_id, table_name)
        output = self._get_execute_shell_output(cmd)
        # recall app 6 succeed
        success_log = 'recall app %s succeed' % app_id
        if success_log in output:
            self.logger.info('end recall table %s->%s' % (app_id, table_name))
        else:
            self.logger.error('bad stdout: %s' % output)
            raise Exception('failed to recall table %s->%s!' % (app_id, table_name))

    def get_all_table_to_partition_count(self):
        partition_count_to_table = {}
        for table_info in self._get_all_table_info()['general_info'].values():
            partition_count_to_table[table_info['app_name']] = int(table_info['partition_count'])
        return partition_count_to_table

    def get_all_table_to_replica_count(self):
        replica_count_to_table = {}
        for table_info in self._get_all_table_info()['general_info'].values():
            replica_count_to_table[table_info['app_name']] = int(table_info['replica_count'])
        return replica_count_to_table

    def get_cluster_all_table_file_mb(self):
        """获取集群内表落盘文件总大小"""
        detail = self.get_app_stat()
        total_key = list(detail['app_stat'].keys())[-1]
        return int(float(detail['app_stat'][total_key]['file_mb']))

    def get_app_stat(self, cmd='app_stat -j', shell_timeout=10000):
        cmd = "timeout {timeout}\n".format(timeout=shell_timeout) + cmd

        output = self._get_execute_shell_output(cmd)
        # 当服务不稳定时，比如存在正在滚动重启，执行 app_stat 会报 "ERROR: query app stat from server failed" 的错误
        if "ERROR: query app stat from server failed" in output:
            self.logger.error('failed to exec [%s] on pegasus shell, please check skv status or retry!' % cmd)
            raise Exception(output)
        try:
            result = json.loads(output)
        except Exception:
            raise
        return result

    def set_table_env(self, table_name, key, value):
        """设置表的环境变量"""
        _, err = self._get_execute_shell_stdout_and_stderr('use {table_name}\nset_app_envs {key} {value}'.format(
            table_name=table_name, key=key, value=value))
        if 'OK' not in err:
            raise Exception('table %s failed to set_table_env %s:%s' % table_name, key, value)

    def get_table_env(self, table_name):
        output = self._get_execute_shell_output('use {table_name}\nget_app_envs -j'.format(table_name=table_name))
        return json.loads(output)

    def clear_table_envs(self, table_name):
        """清空表的环境变量"""
        self._get_execute_shell_output('use {table_name}\nclear_app_envs -a'.format(table_name=table_name))

    def del_table_envs(self, table_name, key):
        """清空表的环境变量"""
        self._get_execute_shell_output('use {table_name}\ndel_app_envs {key}'.format(
            table_name=table_name, key=key))

    def get_unhealthy_gpid_list(self):
        """获取不健康的 pid"""
        gpid_list = []
        for table_name in self.get_unhealthy_app_list():
            table_detail = json.loads(self._get_execute_shell_output('app %s -d -j' % table_name))
            app_id = table_detail['general']['app_id']
            for pidx, info in table_detail['replicas'].items():
                if info['replica_count'].split('/')[0] != info['replica_count'].split('/')[1]:
                    gpid_list.append('{app_id}.{pidx}'.format(app_id=app_id, pidx=pidx))
        return gpid_list

    def get_table_file_mb_and_num(self, table_name):
        """获取 app 的 file_mb & file_num"""
        detail = self.get_app_stat()
        if table_name not in detail['app_stat']:
            raise Exception('%s not have table %s' % (self.cluster_name, table_name))
        return int(float(detail['app_stat'][table_name]['file_mb'])), int(detail['app_stat'][table_name]['file_num'])

    def get_big_capacity_table_list(self, big_filter=lambda t: float(t['file_mb']) / int(t['pcount']) > 1000):
        """获取大表 表的内容通过filter过滤"""
        detail = self.get_app_stat()
        table_list = []
        for tale_name, table_detail in detail['app_stat'].items():
            if table_detail['app_id'] == '0':
                continue
            if big_filter(table_detail):
                table_list.append(tale_name)
        return table_list

    def trigger_checkpoint(self, replica_server_addr, gpids=None):
        """触发某个replica server的异步checkpoint"""
        cmd = 'replica.trigger-checkpoint'
        if gpids:
            cmd += ' ' + gpids
        self._send_remote_command_to_node(replica_server_addr, cmd)

    def get_local_replicas(self, table_name, replica_server_addr):
        """获取某个表在某个replica server的分片列表"""
        table_detail = json.loads(self._get_execute_shell_output('app %s -d -j' % table_name))
        local_replicas = []
        app_id = table_detail['general']['app_id']
        for pidx, info in table_detail['replicas'].items():
            # 集群版 {"pidx": "3", "ballot": "183", "replica_count": "3/3", "primary": "10.141.0.27:8171", "secondaries": "[10.141.0.21:8171,10.141.0.44:8171]"}
            # 单机版 {"pidx": "3", "ballot": "9", "replica_count": "1/1", "primary": "10.129.132.165:8171", "secondaries": "[]"}
            secondaries = info['secondaries'][1:-1].split(',')
            if replica_server_addr == info['primary'] or replica_server_addr in secondaries:
                local_replicas.append('.'.join((app_id, pidx)))
        return local_replicas

    def get_gpid_to_addr(self, table_name, gpid):
        """
        解析单个表分片 replica server (primary_addr, secondaries_addr_list) 信息
        return ('10.129.140.14:8171', ['10.129.140.15:8171', '10.129.140.16:8171'])
        """
        table_detail = json.loads(self._get_execute_shell_output('app %s -d -j' % table_name))
        """
>>> app test -d -j
{
    "parameters": {
        "app_name": "test",
        "detailed": "true"
    },
    "general": {
        "app_name": "test",
        "app_id": "14",
        "partition_count": "4",
        "max_replica_count": "1"
    },
    "replicas": {
        "0": {
            "pidx": "0",
            "ballot": "1",
            "replica_count": "1/1",
            "primary": "10.129.137.208:8171",
            "secondaries": "[]"
        },
        "1": {
            "pidx": "1",
            "ballot": "1",
            "replica_count": "1/1",
            "primary": "10.129.137.208:8171",
            "secondaries": "[]"
        },
        "2": {
            "pidx": "2",
            "ballot": "1",
            "replica_count": "1/1",
            "primary": "10.129.137.208:8171",
            "secondaries": "[]"
        },
        "3": {
            "pidx": "3",
            "ballot": "1",
            "replica_count": "1/1",
            "primary": "10.129.137.208:8171",
            "secondaries": "[]"
        }
    },
    "nodes": {
        "10.129.137.208:8171": {
            "node": "10.129.137.208:8171",
            "primary": "4",
            "secondary": "0",
            "total": "4"
        },
        "": {
            "node": "",
            "primary": "4",
            "secondary": "0",
            "total": "4"
        }
    },
    "healthy": {
        "fully_healthy_partition_count": "4",
        "unhealthy_partition_count": "0",
        "write_unhealthy_partition_count": "0",
        "read_unhealthy_partition_count": "0"
    }
}
        """
        app_id = table_detail['general']['app_id']
        # 防御性检查, gpid 与 table_name 的 app_id 保持一致
        if app_id != gpid.split('.')[0]:
            raise Exception('table[%s] app_id is %s, inconsistent with gpid[%s]!' % (table_name, app_id, gpid))
        partition_count = table_detail['general']['partition_count']
        # 防御性检查, gpid 中的分片序号 pidx 须小于表的分片数
        if int(partition_count) <= int(gpid.split('.')[1]):
            raise Exception('table[%s] partition_count is %s, wrong gpid(%s) value!' % (table_name, partition_count, gpid))
        for replica_detail in table_detail['replicas'].values():
            if gpid.split('.')[1] == replica_detail['pidx']:
                secondaries_str = replica_detail['secondaries'].lstrip('[').rstrip(']')
                secondaries_addr = secondaries_str.split(',') if secondaries_str else []
                return (replica_detail['primary'], secondaries_addr)
        raise Exception('faild to get gpid_to_addr!')

    def check_table_exists(self, table_name):
        """判断表是否存在"""
        output = self._get_execute_shell_output('app %s -j' % table_name)
        return False if 'ERR_OBJECT_NOT_FOUND' in output else True


if __name__ == '__main__':
    import logging
    # logging.basicConfig(level=logging.DEBUG)
    logging.basicConfig(level=logging.INFO)

    api = SkvAdminApi(logging.getLogger())
    # logging.info('cluster primary: %s' % api.get_primary_meta_server())
    # logging.info('primary_and_total_replica_count_stddev: ')
    # logging.info(api.get_primary_and_total_replica_count_stddev())
    # logging.info('10.129.141.26:8171 replica count: %s')
    # logging.info(api.get_replica_count_on_server('10.129.141.26:8171'))
    # api.set_meta_level(api.META_LEVEL_STEADY)
    # api.send_remote_command('meta-server', 'meta.lb.assign_delay_ms', 10)
    # api.create_table('test_xxx', 2, 2)
    # print(api.count_table('test_xxx'))
    # api.set_kv('test_xxx', 'aaa', 'bbb', 'ccc')
    # print(api.get_kv('test_xxx', 'aaa', 'bbb'))
    # print(api.count_table('test_xxx'))
    # print(api.get_alive_meta_server_list())
    # print(api.get_alive_node_list())
    # print(api.get_server_version('10.129.135.136:8171'))
    # print(api.get_server_version('10.129.134.34:8170'))
    # print(api.get_version())
