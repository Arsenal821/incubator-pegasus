#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

包检查
检查磁盘空间够不够
检查数据盘tag是否一致
检查replica server addr是否一致

【2.0新增逻辑】
4. 检查本机ip是否一致
5. 检查数据的md5是否正确
6. 检查slog的md5是否正确
"""

import os
import socket
import sys
import yaml

from hyperion_utils.shell_utils import check_output, check_call

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from backup_replica_data.base_backup_step import STATIC_CLUSTER_FILENAME, STATIC_REPLICA_SERVER_FILENAME, NEW_CLUSTER_REPLICA_MAP_FILENAME
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep, GPIDS_DATA_ASSIGNATION_FILENAME
from skv_admin_api import SkvAdminApi


class PrepareStep(BaseRestoreStep):

    def check_and_return_migrate_mesg(self):
        # 解析本机统计信息
        tar_mesg_to_gpids_yml_file = os.path.join(self.restore_from_backup_path_on_each_host, NEW_CLUSTER_REPLICA_MAP_FILENAME)
        local_ip = socket.gethostbyname(socket.getfqdn())
        with open(tar_mesg_to_gpids_yml_file) as f:
            try:
                tar_mesg_to_gpids = yaml.safe_load(f)[local_ip]
            except Exception:
                raise Exception('%s not in metadata file %s' % (local_ip, tar_mesg_to_gpids_yml_file))

        total_size_mb = 0
        for gpid, tar_mesg in tar_mesg_to_gpids.items():
            # 检查 md5
            tar_path = os.path.join(self.restore_from_backup_path_on_each_host, '%s.tar' % gpid)
            md5 = check_output('md5sum %s' % tar_path, self.logger.debug).split()[0]
            if md5 != tar_mesg['md5']:
                raise Exception('%s md5 not match, please check %s!' % (tar_path, tar_mesg_to_gpids_yml_file))
            # 计算数据总大小
            total_size_mb += tar_mesg['size_mb']

        self.logger.info('checked md5 ok')
        return tar_mesg_to_gpids, total_size_mb

    def check_and_return_backup_mesg(self):
        # 解析本机统计信息
        tar_mesg_to_gpids_yml_file = os.path.join(self.restore_from_backup_path_on_each_host, STATIC_REPLICA_SERVER_FILENAME)
        with open(tar_mesg_to_gpids_yml_file) as f:
            ymal_mesg = yaml.safe_load(f)
        tar_mesg_to_gpids = ymal_mesg['tar_mesg_to_gpids']

        # 计算数据量总大小
        total_size_mb = 0
        for data_tag_mesg in ymal_mesg['old_tag_info'].values():
            total_size_mb += data_tag_mesg['size_mb']

        # 检查 md5
        timeout_sec = total_size_mb / 40 + 600  # 粗略预估 每秒读40MB
        cmd = 'cd %s && md5sum -c md5' % self.restore_from_backup_path_on_each_host
        check_call(cmd, self.logger.debug, timeout=timeout_sec)
        self.logger.info('checked md5 ok')
        return tar_mesg_to_gpids, total_size_mb

    def do_update(self):
        if not self.is_replica_server():
            self.print_msg_to_screen('skip because not replica server host')
            return

        # 1. 在第一个replica server机器上 解析集群的统计信息 检查集群replica server是否一致
        if self.is_first_replica_server():
            static_data_yml = os.path.join(self.restore_from_backup_path_on_each_host, STATIC_CLUSTER_FILENAME)
            with open(static_data_yml) as f:
                static_data = yaml.safe_load(f)

            api = SkvAdminApi(self.logger, self.module_name)
            my_replica_server_list = api.get_all_replica_server()
            if sorted(static_data['replica_server_list']) != sorted(my_replica_server_list):
                self.logger.error('old replica_server_list: %s' % static_data['replica_server_list'])
                self.logger.error('current replica server list: %s' % my_replica_server_list)
                raise Exception('invalid replica server list!')
            self.logger.info('checked replica server list ok')

            # 检查版本是否一致
            major_version = api.get_version().split()[0]
            if static_data.get('version') is None:
                static_data['version'] = "1.12.3-0.6.0"
            if major_version != static_data['version']:
                raise Exception('version unmatch, not support restore diff verison! local %s, restore by %s.' % (major_version, static_data['version']))

        # 2.检查恢复目录的元数据信息，返回 gpid 元信息 & 数据总大小
        # e.g. tar_mesg_to_gpids {'1.1': {'md5': 'xxxxx', 'size_mb': 1024}, '1.2': {'md5': 'qqqqq', 'size_mb': 1024}}
        tar_mesg_to_gpids, total_size_mb = self.check_and_return_migrate_mesg() if self.is_migrate() else self.check_and_return_backup_mesg()

        # 3.检查数据目录可用空间
        # e.g. available_mb_to_data_tag {'data0': 100000, 'data1': 120000}
        available_mb_to_data_tag = {}
        # 当前机器 skv 数据目录总可用的空间
        total_available_mb = 0
        for tag, data_dir in self.get_data_dir_map().items():
            cmd = 'df -m %s' % data_dir
            output = check_output(cmd, self.logger.debug)
            fields = ' '.join(output.splitlines()[1:]).split()
            all_mb, used_mb = int(fields[1]), int(fields[2])
            available_mb = all_mb - used_mb
            available_mb_to_data_tag[tag] = available_mb
            total_available_mb += available_mb

        # 这里拍一个值，可用空间需要大于当前数据大小的 5 倍 (单副本扩充为三副本，再给一些冗余)
        if total_available_mb < total_size_mb * 8:
            raise Exception('Available mb not enough! %d < %d' % (total_available_mb, total_size_mb * 8))
        self.logger.info('checked replica server disk space ok')

        data_tag_to_gpids = self.calculate_tag_to_gpids(tar_mesg_to_gpids, available_mb_to_data_tag, total_size_mb)
        data_tag_assignation_by_gpid_yml_file = os.path.join(self.restore_from_backup_path_on_each_host, GPIDS_DATA_ASSIGNATION_FILENAME)
        with open(data_tag_assignation_by_gpid_yml_file, 'w+') as f:
            yaml.dump(data_tag_to_gpids, f, default_flow_style=False)

    def calculate_tag_to_gpids(self, tar_mesg_to_gpids, available_mb_to_data_tag, total_size_mb):
        total_available_mb = 0
        for mb in available_mb_to_data_tag.values():
            total_available_mb += mb
        # 计划分配给每个 data_tag 的数据量
        # e.g. assignation_mb_to_data_tag {'data0': 100000, 'data1': 120000}
        assignation_mb_to_data_tag = {}
        for tag, mb in available_mb_to_data_tag.items():
            assignation_mb_to_data_tag[tag] = int(mb / total_available_mb * total_size_mb)

        # 当前分配给每个 data_tag 的数据量
        # e.g. current_mb_to_data_tag {'data0': 100000, 'data1': 120000}
        current_mb_to_data_tag = {tag: 0 for tag in assignation_mb_to_data_tag.keys()}
        data_tags = list(available_mb_to_data_tag.keys())
        data_tag_index = 0
        data_tag_to_gpids = {tag: list() for tag in data_tags}
        # 计算当前机器 gpid 数据分配
        # 整体逻辑为每个 data tag 按照可用空间平均预分配数据量额度
        # 循环所有 data tag: 前一个 tag 实际分配数据量超过预分配量后再给下一个 tag 分配
        for gpid, tar_mesg in tar_mesg_to_gpids.items():
            # 1) 当前 tag 分配的实际数据已经大于计划分配数据，切换到下一个 tag
            if data_tag_index + 1 != len(data_tags) and current_mb_to_data_tag[data_tags[data_tag_index]] >= assignation_mb_to_data_tag[data_tags[data_tag_index]]:
                data_tag_index += 1

            # 2) 判断这个 gpid 数据是否能够塞入当前 tag, 这里给 5G 冗余，否则切换到下一个 tag
            while int(tar_mesg['size_mb']) + current_mb_to_data_tag[data_tags[data_tag_index]] + 5120 > available_mb_to_data_tag[data_tags[data_tag_index]]:
                data_tag_index += 1
                if data_tag_index >= len(data_tags):
                    raise Exception('Available mb not enough for gpid %s' % gpid)

            data_tag_to_gpids[data_tags[data_tag_index]].append(gpid)
            current_mb_to_data_tag[data_tags[data_tag_index]] += int(tar_mesg['size_mb'])
        return data_tag_to_gpids


if __name__ == '__main__':
    # case 1
    gpid_list = ['1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7']
    tar_mesg_to_gpids = {gpid: {'md5': '', 'size_mb': 20} for gpid in gpid_list}
    available_mb_to_data_tag = {'data0': 100, 'data1': 300, 'data2': 200}
    total_size_mb = 0
    for mesg in tar_mesg_to_gpids.values():
        total_size_mb += mesg['size_mb']
    data_tag_to_gpids = PrepareStep().calculate_tag_to_gpids(tar_mesg_to_gpids, available_mb_to_data_tag, total_size_mb)
    assert {'data0': ['1.0', '1.1'], 'data1': ['1.2', '1.3', '1.4', '1.5'], 'data2': ['1.6', '1.7']} == data_tag_to_gpids

    # case 2
    gpid_list = ['1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7']
    tar_mesg_to_gpids = {gpid: {'md5': '', 'size_mb': 30} for gpid in gpid_list}
    available_mb_to_data_tag = {'data0': 400, 'data1': 600, 'data2': 600}
    total_size_mb = 0
    for mesg in tar_mesg_to_gpids.values():
        total_size_mb += mesg['size_mb']
    data_tag_to_gpids = PrepareStep().calculate_tag_to_gpids(tar_mesg_to_gpids, available_mb_to_data_tag, total_size_mb)
    assert {'data0': ['1.0', '1.1'], 'data1': ['1.2', '1.3', '1.4'], 'data2': ['1.5', '1.6', '1.7']} == data_tag_to_gpids
