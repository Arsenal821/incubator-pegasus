#!/bin/env python
# -*- coding: UTF-8 -*-

import os
import socket
import sys

from stepworker.server import BaseServer, ContextProcessType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_context_details, assert_context_consistent, SKV_TOOLS_STEPWORKER_NAME,\
    SKV_TOOLS_MANUAL_COMPACTION_STEPS_PATH, SKV_TOOLS_MANUAL_COMPACTION_OPERATION


def skv_manual_compaction(logger, module, table):
    old_context = get_context_details()
    if old_context:
        new_context = {'operation': SKV_TOOLS_MANUAL_COMPACTION_OPERATION, 'table_name': table, 'module_name': module}
        assert_context_consistent(logger, old_context, new_context)
        # 上下文一致继续执行
        logger.error('The context in which table [%s] executes manual compaction already exists' % table)
        server = BaseServer(
            hosts=[socket.getfqdn()],
            name=SKV_TOOLS_STEPWORKER_NAME,
            support_rollback=False,
            step_class_path=SKV_TOOLS_MANUAL_COMPACTION_STEPS_PATH,
            logger=logger,
            context_type=ContextProcessType.NE_CREATE_E_CONTINUE,
            context_details={
                'execute_host': socket.getfqdn(),
                'module_name': module,
                'operation': SKV_TOOLS_MANUAL_COMPACTION_OPERATION,
                'table_name': table
            },
        )
        server.init_context()
        server.execute_one_by_one()
    else:
        server = BaseServer(
            hosts=[socket.getfqdn()],
            name=SKV_TOOLS_STEPWORKER_NAME,
            support_rollback=False,
            step_class_path=SKV_TOOLS_MANUAL_COMPACTION_STEPS_PATH,
            logger=logger,
            context_type=ContextProcessType.NE_CREATE_E_ASK,
            context_details={
                'execute_host': socket.getfqdn(),
                'module_name': module,
                'operation': SKV_TOOLS_MANUAL_COMPACTION_OPERATION,
                'table_name': table
            },
        )
        server.init_context()
        server.execute_one_by_one()
