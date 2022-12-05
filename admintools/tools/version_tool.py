#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

查看skv当前版本
"""
from base_tool import BaseTool
from skv_admin_api import SkvAdminApi

from skv_common import SKV_MODULE_NAME_LIST, SKV_OFFLINE_MODULE_NAME


class VersionTool(BaseTool):
    example_doc = '''
skvadmin version # show skv offline current version
skvadmin version -r # show skv offline current version, by fqdn
'''

    def init_parser(self, subparser):
        subparser.add_argument('-m', '--module',
                               choices=SKV_MODULE_NAME_LIST, default=SKV_OFFLINE_MODULE_NAME,
                               help='module name, skv_offline/skv_online')
        subparser.add_argument('-r', '--resolve',
                               action="store_true",
                               help='resolve ip to hostname')

    def do(self, args):
        self.logger.debug(args)
        if args.resolve:
            exec_cmd = 'server_info -r'
        else:
            exec_cmd = 'server_info'

        api = SkvAdminApi(self.logger, args.module)
        _, stderr = api._get_execute_shell_stdout_and_stderr(exec_cmd)
        if stderr.find('CALL [meta-server]') == -1:
            self.logger.error(args.module + ' is not running, ' + 'ERR_NETWORK_FAILURE')
            return 1
        self.logger.info('The cluster name is: ' + args.module)
        self.logger.info('The cluster meta list is: ' + api.meta_server_endpoint)
        self.logger.info(stderr)

        skv_version = api.get_version()
        self.logger.info("{module_name} current version: {current_version}".format(
            module_name=args.module, current_version=skv_version))
