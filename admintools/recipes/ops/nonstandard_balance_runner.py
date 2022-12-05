#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv 两节点集群负载均衡操作
"""

from skv_admin_api import SkvAdminApi
from recipes.ops.wait_table_healthy_runner import wait_table_healthy


def nonstandard_balance(module_name, logger):
    api = SkvAdminApi(logger, module_name)
    replica_servers = api.get_all_replica_server()
    for table_name in api.get_all_avaliable_table_name():
        primary_replicas = [api.get_table_primary_replica(table_name, replica_server) for replica_server in replica_servers]
        from_index, to_index = 0, 1
        if len(primary_replicas[1]) > len(primary_replicas[0]):
            from_index, to_index = 1, 0
        delta = len(primary_replicas[from_index]) - len(primary_replicas[to_index])
        if delta <= 1:
            continue
        move_replica_list = primary_replicas[from_index][:int(delta / 2)]
        for replica in move_replica_list:
            logger.info("move gpid:%s primary from %s to %s" % (replica, replica_servers[from_index], replica_servers[to_index]))
            api.balance_move_primary(replica, replica_servers[from_index], replica_servers[to_index])
    wait_table_healthy(module_name, logger)
    logger.info('balance done.')


def nonstandard_check_balance(module_name, logger):
    """对外接口 发起负载均衡后调用本接口检查 返回true/false"""
    api = SkvAdminApi(logger, module_name)
    res = dict()
    for s in api.get_all_replica_server():
        res[s] = api.get_primary_count_on_server(s)
    primary_count = list(res.values())
    address_list = list(res.keys())
    primary_count = list(res.values())
    if abs(primary_count[0] - primary_count[1]) <= 1:
        logger.info('Need not to balance.check primary count, %s have %d primary replicas, %s have %d primary replicas' % (
            address_list[0], primary_count[0], address_list[1], primary_count[1]))
        return True
    else:
        logger.info('Need to balance.check primary count, %s have %d primary replicas, %s have %d primary replicas' % (
            address_list[0], primary_count[0], address_list[1], primary_count[1]))
        return False
