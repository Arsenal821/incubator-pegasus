#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

查看skv操作历史
"""
import argparse
import json
import os
import sys

from stepworker.server import BaseServer
from utils.sa_utils import SAMysql

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_common import get_operation_history, SKV_TOOLS_STEPWORKER_NAME


class HistoryTool(BaseTool):
    example_doc = '''
skvadmin history # show skv offline operation history
skvadmin history -n 100 # show skv offline operation history, max 100
'''

    def init_parser(self, subparser):
        subparser.add_argument(
            '-n',
            '--max_count',
            type=int,
            default=20,
            help='max histoy count')
        # 隐藏参数
        subparser.add_argument('--reset_context', type=int, default=0, help=argparse.SUPPRESS)

    def do(self, args):
        """展示所有skv的操作记录 重大操作会使用stepworker记录上下文 实用history命令可以看到记录"""
        if args.reset_context:
            return self._reset_context(args.reset_context)
        records = get_operation_history(self.logger, args.max_count)
        self.logger.info('recent %d operations' % len(records))
        for record in records:
            self.logger.info('[{id}]{start_time}-{end_time}: {operation} {status} {server_host}[{tmp_work_path}]\n{details}'.format(**record))

    def _reset_context(self, context_id):
        try:
            context = BaseServer.read_context_by_id(context_id, self.logger, SKV_TOOLS_STEPWORKER_NAME)
        except Exception:
            self.logger.error('operation error, please check the ID is correct! run:[skvadmin history]')
        else:
            details = json.loads(context['details'])
            context['details'] = details
            self.logger.info('operation:{details[operation]} from [{start_time}] to [{end_time}] status:{status}\ndetails:{details}'.format(**context))
            self.logger.info('you will reset the context, please add description and more than 10 characters')
            resp = input()
            if resp is None or len(resp) < 10:
                self.logger.error('description is empty or less than 10 characters, please try again!')
                return
            details['reset_description'] = resp
            sql = 'UPDATE `sp_stepworker_context` SET `status` = "ABORT", `details` = %s WHERE `id` = %s'
            with SAMysql() as cursor:
                cursor.execute(sql, (json.dumps(details), context_id))
                cursor.execute('commit')
            self.logger.info('context reset success!')
