#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

mothership 2.0不希望开放启停接口给skv 经过激烈的battle 同意放开 但是需要我们通过mothershipadmin启停
具体参考https://doc.sensorsdata.cn/pages/viewpage.action?pageId=209177537 评论区
"""
import os
import sys


from hyperion_utils.shell_utils import check_call

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_service_controller import BaseServiceController


class _ActionType:
    START = 'start'
    RESTART = 'restart'
    STOP = 'stop'
    STATUS = 'status'


class MothershipServiceController(BaseServiceController):
    def _exec_mothershipadmin(self, action, role=None, node=None):
        """调用mothershipadmin启停
action的取值范围是_ActionType里面的那些
role: 角色meta_server/replica_server
node: FQDN
        """
        cmd = 'mothershipadmin {action} --module "{module_name}"'.format(action=action, module_name=self.module)
        if role:
            cmd += ' --role "%s"' % role
            if node:
                cmd += ' --host "%s"' % node
        check_call(cmd, self.logger.debug)

    def _module_start(self):
        """启动整个模块"""
        self._exec_mothershipadmin(_ActionType.START, self.module)

    def _module_stop(self):
        """停止整个模块"""
        self._exec_mothershipadmin(_ActionType.STOP, self.module)

    def _module_is_alive(self):
        """查看单个模块是否活着"""
        # 法涵还不支持
        # return self._exec_mothershipadmin(_ActionType.STATUS, self.module) == 0
        raise Exception('this method is not supported!')

    def _role_stop(self, role):
        """停止单个角色所有实例"""
        self._exec_mothershipadmin(_ActionType.STOP, role)

    def _role_start(self, role):
        """启动单个角色所有实例"""
        self._exec_mothershipadmin(_ActionType.START, role)

    def _instance_stop(self, role, fqdn):
        """停止单个实例"""
        self._exec_mothershipadmin(_ActionType.STOP, role, fqdn)

    def _instance_start(self, role, fqdn):
        """启动单个实例"""
        self._exec_mothershipadmin(_ActionType.START, role, fqdn)

    def _instance_restart(self, role, fqdn):
        """重启单个实例"""
        self._exec_mothershipadmin(_ActionType.RESTART, role, fqdn)
