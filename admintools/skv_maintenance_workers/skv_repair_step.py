#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

修复的时候走 stepworker
这个是包装的步骤类
每个实际修复的规则 被这个类加载后 执行 repair()方法
"""
import os
import sys
import traceback


sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from stepworker.base_step import BaseStep
from skv_common import get_context_details
from skv_maintenance_workers.skv_maintenance_main import get_worker_object, PRIORITY_LEVEL_LIST


class SkvRepairStep(BaseStep):
    def __init__(self, name, priority_level):
        self.name = name
        self.priority_level = priority_level

    def update(self):
        try:
            details = get_context_details()
            work_object = get_worker_object(self.name, details['module_name'], self.logger)

            self.print_msg_to_screen('checking %s' % self.name)
            if not work_object.is_state_abnormal():
                self.logger.info('check %s: pass' % self.name)
                return

            self.print_msg_to_screen('repairing %s' % self.name)
            work_object.repair()
        except Exception:
            self.logger.warn('caught exception while doing %s' % self.name)
            self.logger.warn(traceback.format_exc())

        if self.priority_level == PRIORITY_LEVEL_LIST[0]:
            # level A 按照 yml 文件内配置任务的顺序执行，有一个异常就退出
            raise Exception('please fix this and run again!')

    def backup(self):
        pass

    def check(self):
        return True
