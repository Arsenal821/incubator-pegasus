#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_common import SKV_MODULE_NAME_LIST, SKV_OFFLINE_MODULE_NAME
from skv_maintenance_workers.skv_maintenance_main import diagnose


class HealthTool(BaseTool):
    example_doc = '''
skvadmin health # check current skv status
skvadmin health --full # check fully; this may take a while
'''

    def init_parser(self, parser):
        parser.add_argument('-m', '--module', required=True,
                            type=str, default=SKV_OFFLINE_MODULE_NAME,
                            choices=SKV_MODULE_NAME_LIST,
                            help='module name, skv_offline/skv_online')
        parser.add_argument('--full',
                            action="store_true",
                            help='execute all check worker')

    def do(self, args):
        self.logger.debug(args)
        if args.full:
            return diagnose(args.module, self.logger, 'C')
        else:
            return diagnose(args.module, self.logger)
