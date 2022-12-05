#!/bin/env python
# -*- coding: UTF-8 -*-

"""
此脚本会在 skv2.0 的操作计划中执行
    1. 用于在升级 skv2.0 后恢复 skv 的写入
"""

import sys
import os
import logging

from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo

if 'SKV_HOME' not in os.environ:
    os.environ['SKV_HOME'] = os.path.join(os.environ['SENSORS_PLATFORM_HOME'], '../skv')
sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))

from skv_admin_api import SkvAdminApi
from skv_common import SKV_MODULE_NAME_LIST


class InstallPostCheck:
    def __init__(self):
        tmp_log_dir = os.path.dirname(os.path.abspath(__file__))
        logging.basicConfig(
            level=logging.DEBUG,
            filename=os.path.join(tmp_log_dir, "./skv_operation_install_post_check.log"),
            format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger()
        self.modules = []
        for module in SKV_MODULE_NAME_LIST:
            if module in InnerDeployTopo().get_all_module_name_by_product_name('skv'):
                self.modules.append(module)

    # 恢复表可读
    def recover_skv_table_write(self):
        for module in self.modules:
            api = SkvAdminApi(self.logger, module)
            table_list = api.get_all_avaliable_table_name()
            for table in table_list:
                api.set_table_env(table, 'replica.deny_client_write', 'false')


if __name__ == '__main__':
    installer = InstallPostCheck()
    installer.recover_skv_table_write()
