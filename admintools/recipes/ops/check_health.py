# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

from skv_admin_api import SkvAdminApi
from skv_common import check_exists_module


def check_health(logger, module_name):
    check_exists_module(module_name)

    api = SkvAdminApi(logger, module_name)

    unalive_meta_server_list = api.get_unalive_meta_server_list()
    if unalive_meta_server_list:
        logger.error("{count} meta-servers unhealthy: ".format(
            count=len(unalive_meta_server_list)))
        for server in unalive_meta_server_list:
            logger.error(server)
    else:
        logger.info('All meta-server healthy.')

    unalive_replica_server_list = api.get_unalive_node_list()
    if unalive_replica_server_list:
        logger.error("{count} replica-servers unhealthy: ".format(
            count=len(unalive_replica_server_list)))
        for server in unalive_replica_server_list:
            logger.error(server)
    else:
        logger.info('All replica-server healthy.')

    unhealthy_app_list = api.get_unhealthy_app_list()
    if unhealthy_app_list:
        logger.error("{count} tables unhealthy: ".format(
            count=len(unhealthy_app_list)))
        for app in unhealthy_app_list:
            logger.error(app)
    else:
        logger.info('All tables healthy.')

    return not (unalive_meta_server_list or unalive_replica_server_list or unhealthy_app_list)
