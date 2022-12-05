#!/bin/env python
# -*- coding: UTF-8 -*-

import socket

from skv_maintenance_workers.base_worker import BaseWorker
from skv_admin_api import SkvAdminApi
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager

NODES_QUOTA_DICT = {
    "get_p99(ms)": {
        # 需要检查的node -q 命令中，相关qps的field是否真有数据
        "check_node_qps_fields": {"get_qps": 10},
        # 和app_info 命令相关的qps field
        "app_info_related_fields": ["GET"],
    },
    "mget_p99(ms)": {
        "check_node_qps_fields": {"mget_qps": 10},
        "app_info_related_fields": ["MGET"],
    },
    "bget_p99(ms)": {
        "check_node_qps_fields": {"bget_qps": 10},
        "app_info_related_fields": ["BGET"],
    },
    "put_p99(ms)": {
        "check_node_qps_fields": {"put_qps": 10},
        "app_info_related_fields": ["PUT"],
    },
    "mput_p99(ms)": {
        "check_node_qps_fields": {"mput_qps": 10},
        "app_info_related_fields": ["MPUT"],
    }
}

SKV_MODULE_LATENCY_LIMIT_DEFAULT = {"skv_online": 100, "skv_offline": 200}


class SkvCheckP99Worker(BaseWorker):
    def is_state_abnormal(self):
        abnormal = False

        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        upper_bound = float(skv_config_manager._get_maintenace_config(
            'check_partition_num_worker',
            'partition_size_mb_bound',
            SKV_MODULE_LATENCY_LIMIT_DEFAULT[self.module]))

        api = SkvAdminApi(self.logger, self.module)
        nodes_quota = api.get_cluster_all_node_quota_details()

        for one_node_address in nodes_quota:
            one_node_quota_obj = nodes_quota[one_node_address]
            for one_quota in NODES_QUOTA_DICT:
                one_dict_obj = NODES_QUOTA_DICT[one_quota]
                node_qps_check_dict = one_dict_obj["check_node_qps_fields"]
                node_check_qps_list = [float(one_node_quota_obj[one_check_qps_field]) >= node_qps_check_dict[one_check_qps_field] for one_check_qps_field in node_qps_check_dict]
                one_quota_value = one_node_quota_obj[one_quota]
                if float(one_quota_value) >= upper_bound and True in node_check_qps_list:
                    abnormal = True
                    break

        return abnormal

    def diagnose(self):
        self.__try_fetch_info_for_p99_reason()

    def repair(self):
        self.__try_fetch_info_for_p99_reason()

    def __try_fetch_info_for_p99_reason(self):
        upper_bound = SKV_MODULE_LATENCY_LIMIT_DEFAULT[self.module]

        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        upper_bound = float(skv_config_manager._get_maintenace_config(
            'check_partition_num_worker',
            'partition_size_mb_bound',
            SKV_MODULE_LATENCY_LIMIT_DEFAULT[self.module]))

        api = SkvAdminApi(self.logger, self.module)

        # fetch suspicious nodes again
        # nodes_quota : {"ip1:port1" : {"get_p99(ms)":***, "mget_p99(ms)":***, "get_qps": ***, ......}} corresponds to 'nodes -q' skv shell command
        nodes_quota = api.get_cluster_all_node_quota_details()

        app_info_related_fields = []
        suspicious_fqdn_problem_dict = dict()
        for one_node_address, one_node_quota_obj in nodes_quota.items():
            one_host_fqdn = socket.getfqdn(one_node_address.split(":")[0])
            for one_quota in NODES_QUOTA_DICT:
                one_quota_dict_obj = NODES_QUOTA_DICT[one_quota]
                node_qps_check_dict = one_quota_dict_obj["check_node_qps_fields"]
                node_check_qps_list = [float(one_node_quota_obj[one_check_qps_field]) >= node_qps_check_dict[one_check_qps_field] for one_check_qps_field in node_qps_check_dict]
                one_quota_value = one_node_quota_obj[one_quota]
                if float(one_quota_value) >= upper_bound and True in node_check_qps_list:
                    for one_app_info_related_field in one_quota_dict_obj["app_info_related_fields"]:
                        if one_app_info_related_field not in app_info_related_fields:
                            app_info_related_fields.append(one_app_info_related_field)
                    if one_host_fqdn not in suspicious_fqdn_problem_dict:
                        suspicious_fqdn_problem_dict[one_host_fqdn] = dict()
                    suspicious_fqdn_problem_dict[one_host_fqdn][one_quota] = str(one_quota_value)

        # print all related information and contact skv engineers to take appropriate operation
        suspicious_host_len = len(suspicious_fqdn_problem_dict)
        if 0 != suspicious_host_len:
            self.logger.warn("{suspicious_host_len}/{total_host_len} nodes has some problematic P99 quotas, detail informations are as following:".format(suspicious_host_len=suspicious_host_len, total_host_len=len(nodes_quota)))

            # print all host related information
            for one_host_fqdn in suspicious_fqdn_problem_dict:
                one_host_fqdn_warn_msg = "host-" + one_host_fqdn + ": "
                for one_quota in suspicious_fqdn_problem_dict[one_host_fqdn]:
                    one_host_fqdn_warn_msg += one_quota + "-" + suspicious_fqdn_problem_dict[one_host_fqdn][one_quota] + ", "
                self.logger.warn(one_host_fqdn_warn_msg)

            for one_app_info_related_field in app_info_related_fields:
                count = 0
                table_to_quota = api.get_table_to_quota_by_op(one_app_info_related_field)
                self.logger.warn("Following tables have {field} qps, they are:".format(field=one_app_info_related_field))
                one_field_msg = "{"
                for table, quota in table_to_quota.items():
                    one_app_info_related_quota = float(quota)
                    if one_app_info_related_quota > 0.1:
                        count += 1
                        # 每一个指标最多显示16张表的记录，防止表过多而刷屏
                        if count > 16:
                            break
                        one_field_msg += table + ":" + quota + ","
                one_field_msg += "}"
                self.logger.warn(one_field_msg)

            self.logger.warn("please contact skv engineers to do some later work......")
