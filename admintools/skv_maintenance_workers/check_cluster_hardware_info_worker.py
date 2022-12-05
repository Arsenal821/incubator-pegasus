#!/bin/env python
# -*- coding: UTF-8 -*-

from skv_maintenance_workers.base_worker import BaseWorker
from hyperion_client.hyperion_inner_client.inner_node_info import InnerNodeInfo

from recipes import get_skv_config_manager
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME, is_skv_in_mothership


class CheckClusterHardwareInfoWorker(BaseWorker):
    def self_remedy(self):
        return True

    def is_state_abnormal(self):
        # 这个检查会打印 cpu 内核数, 内存数, 以及 rand 盘和 seq 盘是否挂载在同一磁盘下 等提示信息
        # MS2 跑斥候越来越标准化，实际值班作用不大了,加上 ms2 对于这些信息没有提供接口, 所以先直接去掉
        if is_skv_in_mothership(self.module):
            return False

        self.__show_all_cluster_resources()
        return False

    def diagnose(self):
        pass

    def repair(self):
        pass

    def __show_all_cluster_resources(self):
        # show cpu、memory、disk、
        tuple_to_host = {}
        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        for one_host in skv_config_manager.get_host_list():
            hardware_info = InnerNodeInfo.get_instance().get_node_hardwareinfo(one_host)
            one_cpu = hardware_info['cpu']
            one_mem = hardware_info['mem']

            one_rnd_disk_count = 0
            one_same_with_sequence_disk = False
            disks_info = hardware_info['disks_info']
            if 'random' in disks_info:
                random_disks_info = disks_info['random']
                random_disks_info_mount_point = random_disks_info['mount_point']
                random_disks_info_mount_point = sorted(random_disks_info_mount_point)
                one_rnd_disk_count = len(random_disks_info_mount_point)

                sequence_disks_info_mount_point = []
                if 'sequence' in disks_info:
                    sequence_disks_info_mount_point = disks_info['sequence']['mount_point']
                    sequence_disks_info_mount_point = sorted(sequence_disks_info_mount_point)

                if random_disks_info_mount_point == sequence_disks_info_mount_point:
                    one_same_with_sequence_disk = True

            one_feature_tuple = (one_cpu, one_mem, one_rnd_disk_count, one_same_with_sequence_disk)
            if one_feature_tuple not in tuple_to_host:
                tuple_to_host[one_feature_tuple] = []
            tuple_to_host[one_feature_tuple].append(one_host)

        final_show_str = ""
        for one_tuple in tuple_to_host:
            final_show_str += "[cpu-%d, mem-%dG, random_disk_count-%d, is_random_disk_same_with_sequence_disk-%s : number of machine-%s];" % (one_tuple[0], one_tuple[1], one_tuple[2], str(one_tuple[3]), len(tuple_to_host[one_tuple]))

        self.logger.info(final_show_str)
