#!/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import time
import logging

sys.path.append(os.path.join(os.environ['MOTHERSHIP_HOME'], 'shim_libs'))
from base_custom_callback import BaseCustomCallback

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_admin_api import SkvAdminApi
from recipes import check_health


class ModuleCustomCallback(BaseCustomCallback):
    """
    module 级别自定义回调，仅在 module 部署的其一机器执行一次
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.module_name = self.params['module_params']['module_name']
        self.api = SkvAdminApi(logging, self.module_name)
        self.log_dir = os.path.join(self.params['runtime_params']['log_dir'], 'upgrade_callback.log')
        logging.basicConfig(
            level=logging.INFO,
            filename=self.log_dir,
            format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger()

    def upgrade_prepare_ready_check(self, **kwargs):
        """ 升级前 prepare 是否就绪检查
        1. 先不考虑滚动升级
        2. 非滚动升级
          - 集群是否健康
          - 检查是否存在读写 （抛异常/打WARN日志 ？ ）
        Args:
            params:
            **kwargs:
        Returns:
        """
        self.logger.info("check skv meta/replcia before supgrding")
        # 检查集群是不是健康的
        if not check_health(self.logger, self.module_name):
            raise Exception('cluster not healthy!')
        table_list = self.api.get_all_avaliable_table_name()
        # 检查冷升级是否仍然有表在读写数据(除temp等可以忽略读写流量的表外)
        table_list = [t for t in table_list if t not in {'temp', 'lumen_historical_profile', 'impala_historical_profile'}]
        for table_name in table_list:
            if self.api.check_table_has_ops(table_name):
                raise Exception('table %s still has write/read operations!' % table_name)

    # 升完后
    def service_check(self, **kwargs):
        """ 服务检查
        2022-05-20
        检查服务是否ok, 这里就只检查meta和replica是否拉起来了,先完成联调跑起来
        Args:
            params:
            **kwargs:

        Returns:

        """
        self.logger.info("do a status check on the skv after the upgrade")
        # 检查集群是不是健康的
        try_times = 3
        while(try_times):
            if not check_health(self.logger, self.module_name):
                self.logger.info("check skv status unhealth, try times = %d" % (4 - try_times))
                time.sleep(10)
                try_times -= 1
            else:
                return


if __name__ == "__main__":
    ModuleCustomCallback(*sys.argv[1:]).execute()
