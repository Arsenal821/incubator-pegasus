#!/bin/env python
# -*- coding: UTF-8 -*-

import socket


def change(params):
    meta_server_port = str(params["cluster_port_info"]["meta_server_ports"]["server_port"]["port"])
    meta_server_list = [socket.gethostbyname(one_host) + ":" + meta_server_port for one_host in params["cluster_node_info"]["meta_server_nodes"]]
    replica_server_number = len(params['cluster_node_info'].get('replica_server_nodes') or [])

    partition_factor = 0
    replica_server_role_group_dict = params["role_config_groups"]["replica_server"]
    for key, value in replica_server_role_group_dict.items():
        data_dirs = value["configurations"]["replica_server.ini"]["replication|data_dirs"]

        partition_factor_by_disk_count = len(data_dirs.split(","))
        partition_factor_by_mem = int(value["configurations"]["role_group_hardware_config"]["mem"] / 28)

        # 统计当前 group 的节点个数
        node_groups = value['node_groups']
        node_group_num = 0
        for _, node_group in node_groups.items():
            node_group_num += len(node_group)

        partition_factor += max(min(partition_factor_by_mem, partition_factor_by_disk_count), 1) * node_group_num

    return {
        'meta_server_list': meta_server_list,
        'table_prefix': "",
        'partition_factor': partition_factor,
        'major_version': "2.4.0",
        'replica_server_number': replica_server_number,
    }
