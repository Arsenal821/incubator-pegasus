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
from skv_common import SKV_MODULE_NAME_LIST


class TransferMetaServerTool(BaseTool):
    example_doc = '''transfer meta server is not supported!'''

    def init_parser(self, subparser):
        subparser.add_argument(
            '-m', '--module_name', choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online', required=True)
        subparser.add_argument(
            '--from_host', help='old meta server host', required=True)
        subparser.add_argument(
            '--to_host', help='new meta server host', required=True)
        subparser.add_argument(
            '--is_from_host_dead', action='store_true', help='if set, assume old meta server is dead')
        subparser.add_argument(
            '--is_continue', action='store_true', help='if set, will skip check')

    def do(self, args):
        raise Exception('not supported!')
