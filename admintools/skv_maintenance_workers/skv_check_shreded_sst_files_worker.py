#!/bin/env python
# -*- coding: UTF-8 -*-

from skv_admin_api import SkvAdminApi
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager

DEFAULT_RDB_TARGE_FILE_SIZE_BASE = 8 * 1024 * 1024


class SkvCheckShrededSstFilesWorker(BaseWorker):
    def is_state_abnormal(self):
        abnormal = False

        api = SkvAdminApi(logger=self.logger, cluster_name=self.module)
        all_table_name = api.get_all_avaliable_table_name()
        for one_table_name in all_table_name:
            if self.check_one_table_shreded_sst_file(one_table_name, False):
                abnormal = True
                break

        return abnormal

    def diagnose(self):
        self._check_all_table_shreded_sst_file()

    def repair(self):
        self._check_all_table_shreded_sst_file()

    def _check_all_table_shreded_sst_file(self):
        api = SkvAdminApi(logger=self.logger, cluster_name=self .module)
        all_table_name = api.get_all_avaliable_table_name()
        for one_table_name in all_table_name:
            self.check_one_table_shreded_sst_file(one_table_name, True)

    def check_one_table_shreded_sst_file(self, table_name, need_log):
        # rocksdb碎文件情况检查; 如果判定碎文件较多，则提示手动compaction
        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        # 此处简化 选取最大的
        max_rocksdb_target_file_size_base = DEFAULT_RDB_TARGE_FILE_SIZE_BASE
        for group in skv_config_manager.get_config_groups():
            max_rocksdb_target_file_size_base = max(max_rocksdb_target_file_size_base, int(skv_config_manager.get_final_config_value(
                'pegasus.server', 'rocksdb_target_file_size_base', group, DEFAULT_RDB_TARGE_FILE_SIZE_BASE)))
        api = SkvAdminApi(self.logger, self.module)
        table_partition_num = api.get_table_partition_count(table_name=table_name)
        total_sst_file_usage_mb, total_sst_file_num = api.get_table_file_mb_and_num(table_name=table_name)

        if need_log:
            self.logger.debug("table %s table_partition_num %d total_sst_file_usage %d total_sst_file_num %d" % (table_name, table_partition_num, total_sst_file_usage_mb, total_sst_file_num))
        if table_partition_num * 1024 < total_sst_file_usage_mb:
            # 如果平均每个分片平均占用的sst文件空间 大于 等于1G
            sst_file_avg_size_bytes = float(total_sst_file_usage_mb) * 1024 * 1024 / float(total_sst_file_num)
            sst_file_size_threshold_bytes = max_rocksdb_target_file_size_base * 0.9
            if need_log:
                self.logger.debug("table %s sst_file_size_threshold %f sst_file_avg_size %f" % (table_name, sst_file_size_threshold_bytes, sst_file_avg_size_bytes))
            if sst_file_avg_size_bytes < sst_file_size_threshold_bytes:
                # 如果每个sst file平均大小 小于 replica server 的配置 'rocksdb_target_file_size_base'
                # 则说明 零碎的 sst file 过多，需要 compaction
                if need_log:
                    need_compact_cmd = "skvadmin table manual_compaction -m {module} -t {table_name}" \
                        .format(module=self.module, table_name=table_name)
                    self.logger.warn("sst file average size : {sst_file_avg_size} < {sst_file_size_threshold},"
                                     " you'd better execute [{cmd}] to compact the table data first...... "
                                     .format(sst_file_avg_size=sst_file_avg_size_bytes,
                                             sst_file_size_threshold=sst_file_size_threshold_bytes,
                                             cmd=need_compact_cmd))
                return True

        return False
