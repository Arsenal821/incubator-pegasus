# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""


from recipes.ops.wait_meta_server_runner import wait_all_meta_servers_available
from recipes.ops.wait_replica_server_runner import wait_replica_server
from recipes.ops.wait_table_healthy_runner import wait_table_healthy
from recipes.platform_adapter import get_service_controller


def start_skv_cluster(module_name, logger, print_progress_fun=None):
    get_service_controller(logger, module_name).start_skv()
    wait_all_meta_servers_available(module_name, logger, print_progress_fun=print_progress_fun)
    wait_replica_server(module_name, logger, print_progress_fun=print_progress_fun)
    wait_table_healthy(module_name, logger, print_progress_fun=print_progress_fun)
