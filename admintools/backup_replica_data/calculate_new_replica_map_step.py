#!/bin/env python
# -*- coding: UTF-8 -*-

"""
1.采集 tar_replica_data_step 产生的各个节点的数据信息

2.根据上一步采集的各个节点的信息，组成完整的数据分布

3.根据算法计算新环境的数据分布以及旧环境的数据转移路径

4.将新集群数据分布信息记录到 new_cluser_data_map ，生成发送给新环境节点的 scp 脚本
"""

import os
import socket
import sys
import yaml

from hyperion_guidance.ssh_connector import SSHConnector
sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import BaseBackupStep, STATIC_REPLICA_SERVER_FILENAME, \
    NEW_CLUSTER_REPLICA_MAP_FILENAME, REPLICA_MIGRATE_WAY_FILENAME, STATIC_CLUSTER_FILENAME
from skv_admin_api import SkvAdminApi

from hyperion_utils.shell_utils import check_call


class CalculateNewReplicaMapStep(BaseBackupStep):
    def do_update(self):
        # 本地备份无需此步骤操作
        if not self.new_replica_server_list:
            self.print_msg_to_screen('skip because not need migrate')
            return

        # 1.读取 replica 节点的 static_replica_server.yml
        # 1.1 初始化当前环境 local_replica_map
        # local_replica_map = { gpid: { host: {'md5': md5, 'size_mb': size_mb} } }
        # e.g. {'1.1': {'host1': {'md5': 'xxxxx', 'size_mb': 1024}, 'host2': {'md5': 'qqqqq', 'size_mb': 1024}}}
        local_replica_map = {}
        api = SkvAdminApi(self.logger, self.module_name)
        for table_name, partition_count in api.get_all_table_to_partition_count().items():
            app_id = api.get_app_id_by_table(table_name)
            for i in range(partition_count):
                gpid = '{app_id}.{partition_id}'.format(app_id=app_id, partition_id=i)
                local_replica_map[gpid] = {}

        # 1.2 将 replica_server 节点的 static_replica_server.yml 文件发送到当前执行机并读取
        remote_file_path = os.path.join(self.backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)
        for host in self.get_replica_server_hosts():
            ip = socket.gethostbyname(host)
            connector = SSHConnector.get_instance(host)
            local_file_path = os.path.join(
                self.get_stepworker_work_dir(), '{ip}_{file}'.format(ip=ip, file=STATIC_REPLICA_SERVER_FILENAME))
            connector.copy_from_remote(remote_file_path, local_file_path, self.logger.debug)
            with open(local_file_path) as f:
                replica_server_statics = yaml.safe_load(f)
            for gpid, mesg in replica_server_statics['tar_mesg_to_gpids'].items():
                local_replica_map[gpid][ip] = mesg

        # 2.计算新环境数据分布以及数据转移路径
        new_replica_server_host_list = self.get_new_replica_server_ips()

        # 2.1 初始化新集群数据分布 remote_host_replica_map
        remote_host_replica_map = {host: {} for host in new_replica_server_host_list}

        # 2.2 初始化老集群数据迁移路径 way_for_replica_on_host
        way_for_replica_on_host = {socket.gethostbyname(host): {} for host in self.get_replica_server_hosts()}

        # 2.3 计算, 基本逻辑为顺序循环将旧环境分片分给新环境节点
        index = 0
        for gpid, replica_map in local_replica_map.items():
            for host, mesg in replica_map.items():
                dest_host = new_replica_server_host_list[index % len(new_replica_server_host_list)]
                remote_host_replica_map[dest_host][gpid] = mesg
                way_for_replica_on_host[host][gpid] = dest_host
                index += 1

        # 3.将计算的结果写到文件 new_cluster_replica_map.yaml & replica_migrate_way.yaml
        new_cluster_replica_map_file = os.path.join(self.get_stepworker_work_dir(), NEW_CLUSTER_REPLICA_MAP_FILENAME)
        with open(new_cluster_replica_map_file, 'w+') as f:
            yaml.dump(remote_host_replica_map, f, default_flow_style=False)

        temporary_replica_migrate_way_file = os.path.join(self.get_stepworker_work_dir(), REPLICA_MIGRATE_WAY_FILENAME)
        replica_migrate_way_file = os.path.join(self.backup_path_on_each_host, REPLICA_MIGRATE_WAY_FILENAME)
        with open(temporary_replica_migrate_way_file, 'w+') as f:
            yaml.dump(way_for_replica_on_host, f, default_flow_style=False)

        # 4.将 replica_migrate_path.yaml 拷贝到所有 replica_server 机器
        for host in self.get_replica_server_hosts():
            connector = SSHConnector.get_instance(host)
            connector.copy_from_local(temporary_replica_migrate_way_file, replica_migrate_way_file, self.logger.debug)

        # 5.生成 scp 脚本将 remote_host_replica_map 发送给每个新集群机器
        scp_data_script = os.path.join(self.backup_path_on_each_host, 'scp_metadata.sh')
        with open(scp_data_script, 'w+') as f:
            f.write('''#!/bin/sh
set -ex

''')

        for host in new_replica_server_host_list:
            # 创建远端目录
            cmd = 'ssh {ip} "mkdir -p {dir}"'.format(ip=host, dir=self.remote_migration_dir)
            check_call('echo "{cmd}" >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)

            # 检查目录是否为空
            cmd = 'ret=\\`ssh sa_cluster@{ip} "ls -1 {dir} | wc -l"\\`'.format(ip=host, dir=self.remote_migration_dir)
            check_call('echo {cmd} >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)
            cmd = 'if [ \\$ret -ne 0 ] ;then echo "error: {dir} is not empty on {ip}" && exit 1; fi'.format(ip=host, dir=self.remote_migration_dir)
            check_call('echo "{cmd}" >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)

            # 生成拷贝元数据文件命令
            cmd = 'scp {local_file} sa_cluster@{ip}:{remote_dir}'.format(
                local_file=new_cluster_replica_map_file,
                ip=host,
                remote_dir=self.remote_migration_dir,
            )
            check_call('echo "{cmd}" >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)
            cmd = 'scp {local_file} sa_cluster@{ip}:{remote_dir}'.format(
                local_file=os.path.join(self.backup_path_on_each_host, STATIC_CLUSTER_FILENAME),
                ip=host,
                remote_dir=self.remote_migration_dir,
            )
            check_call('echo "{cmd}" >> {file}'.format(cmd=cmd, file=scp_data_script), self.logger.debug)
