#!/bin/env python
# -*- coding: UTF-8 -*-
import sys
import os

from stepworker.base_step import BaseStep

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_context_details
from skv_admin_api import SkvAdminApi


class BaseManualCompactionStep(BaseStep):

    def do_init(self):
        details = get_context_details()

        # 模块名
        self.module_name = details['module_name']
        # 表名
        self.table_name = details['table_name']

        self.api = SkvAdminApi(self.logger, self.module_name)

        self.meta_server_list = self.api.meta_server_endpoint

    def do_update(self):
        raise Exception('please implement this method!')

    def update(self):
        self.do_init()
        self.do_update()

    def backup(self):
        pass

    def check(self):
        return True

    def rollback(self):
        pass
