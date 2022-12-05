#!/bin/env python
# -*- coding: UTF-8 -*-
import os
import socket

from skv_role_base import SkvRoleBase
from skv_role_base import SKV_META_SERVER_ROLE_NAME, SKV_PROMETHEUS_PORT


class SkvMetaServer(SkvRoleBase):
    def __init__(self):
        super(SkvMetaServer, self).__init__("meta_server")

    def initialization(self):
        super(SkvMetaServer, self).initialize()

    def _dynamic_and_immutable_config(self):
        replica_server_num = len(self.replica_server_list)

        min_live_node_count_for_unfreeze = 2 if replica_server_num > 2 else 1
        max_replica_count = replica_server_num if replica_server_num < 3 else 3
        mutation_2pc_min_replica_count = 2 if replica_server_num > 2 else 1

        return {
            "apps.meta": {
                "ports": self.meta_server_port
            },
            "meta_server.apps.__detect": {
                "max_replica_count": str(max_replica_count),
                "app_name": "__detect",
                "app_type": "pegasus",
                "package_id": "",
                "partition_count": "8",
                "stateful": "true"
            },
            "meta_server.apps.__stat": {
                "max_replica_count": str(max_replica_count),
                "app_name": "__stat",
                "app_type": "pegasus",
                "package_id": "",
                "partition_count": "8",
                "stateful": "true"
            },
            "replication.app": {
                "max_replica_count": str(max_replica_count)
            },
            "core": {
                "log_dir": self.log_dir,
            },
            "network": {
                "explicit_host_address": socket.gethostbyname(self.params_json_dict["node_params"]["hostname"]),
            },
            "replication": {
                "cluster_name": self.module_name,
                "cold_backup_root": self.module_name,
                "mutation_2pc_min_replica_count": str(mutation_2pc_min_replica_count),
            },
            "meta_server": {
                "min_live_node_count_for_unfreeze": str(min_live_node_count_for_unfreeze),
                "server_list": ",".join(self.meta_server_list),
                "cluster_root": self.cluster_root,
                "distributed_lock_service_parameters": os.path.join(self.cluster_root, "lock")
            },
            "pegasus.server": {
                "perf_counter_enable_prometheus": "true",
                "perf_counter_sink": "prometheus",
                "perf_counter_cluster_name": self.params_json_dict["module_params"]["module_name"],
                "prometheus_port": str(SKV_PROMETHEUS_PORT[self.module_name][SKV_META_SERVER_ROLE_NAME]),
            },
            "zookeeper": {
                "wrapper_log_dir": self.log_dir,
                "helper_log_dir": self.log_dir,
                "hosts_list": ",".join(self.zk_server_list_fqdn)
            },
            "pegasus.clusters": {
                self.module_name: ",".join(self.meta_server_list)
            }
        }


if __name__ == '__main__':
    meta_server_instance = SkvMetaServer()
    meta_server_instance.initialization()
    meta_server_instance.do()
