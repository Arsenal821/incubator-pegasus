#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import time

from skv_role_base import SkvRoleBase

from skv_role_base import SKV_PROMETHEUS_PORT


class SkvReplicaServer(SkvRoleBase):
    def __init__(self):
        super(SkvReplicaServer, self).__init__("replica_server")

    def initialization(self):
        super(SkvReplicaServer, self).initialize()

    def _dynamic_and_immutable_config(self):
        replica_server_num = len(self.replica_server_list)
        mutation_2pc_min_replica_count = 2 if replica_server_num > 2 else 1

        return {
            "core": {
                "log_dir": self.log_dir
            },
            "network": {
                "explicit_host_address": self.ip,
            },
            "meta_server": {
                "server_list": ",".join(self.meta_server_list),
            },
            "apps.replica": {
                "ports": str(self.replica_server_port),
            },
            'replication': {
                'mutation_2pc_min_replica_count': str(mutation_2pc_min_replica_count),
                "cluster_name": self.module_name,
                "data_dirs_black_list_file": os.path.join(self.runtime_dir, "conf/.skv_data_dirs_black_list"),
                "cold_backup_root": self.module_name
            },
            'pegasus.server': {
                'perf_counter_enable_prometheus': 'true',
                'perf_counter_sink': 'prometheus',
                "perf_counter_cluster_name": self.module_name,
                "prometheus_port": str(SKV_PROMETHEUS_PORT[self.module_name][self.role_name]),
            },
            "pegasus.clusters": {
                self.module_name: ",".join(self.meta_server_list)
            }
        }

    # non rolling upgrader 先停写入
    def prepare_non_rolling_upgrade(self):
        if not self._is_running_assign_node():
            return
        self.logger.info("prepare non rolling upgrade skv replica, stop all table write operations!")
        table_list = self.api.get_all_avaliable_table_name()
        for table in table_list:
            self.api.set_table_env(table, 'replica.deny_client_write', 'true')
        # 停读写需要一定的时间，这里先让sleep 10s
        retry_times = 3
        while(retry_times):
            time.sleep(10)
            check_pass = True
            # 直接在这里进行检查吧
            for table in table_list:
                if self.api.check_table_has_ops(table):
                    retry_times -= 1
                    check_pass = False
                    break
            if check_pass:
                break
        if not retry_times:
            raise Exception('table %s still has write/read operations!' % table)

    # non rolling upgrade done, 打开写入
    def finalize_non_rolling_upgrade(self):
        if not self._is_running_assign_node():
            return
        self.logger.info("finalize non rolling skv replica, open all table write operations!")
        table_list = self.api.get_all_avaliable_table_name()
        for table in table_list:
            self.api.set_table_env(table, 'replica.deny_client_write', 'false')

    def _is_running_assign_node(self):
        assign_node = self.params_json_dict['cluster_node_info']['replica_server']['nodes'][0]
        if self.params_json_dict['node_params']['hostname'] == assign_node:
            return True
        return False


if __name__ == '__main__':
    replica_server_instance = SkvReplicaServer()
    replica_server_instance.initialization()
    replica_server_instance.do()
