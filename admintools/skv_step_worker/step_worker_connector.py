#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

"""

import datetime
import os
import json
import socket
import sys
import yaml

from hyperion_client.directory_info import DirectoryInfo
from hyperion_utils.sailor_utils.sailor_utils import Status
from hyperion_guidance.sailor_worker.sailor_worker_connector import SailorWorkerConnector
from hyperion_utils.template_engine_utils.template_engine import TemplateEngine

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import TEMPLATE_YAML, STEPS_YAML, SKV_REPLICA_SERVER_ROLE_NAME

sys.path.append(os.path.join(os.environ['MOTHERSHIP_HOME'], 'mothership_client'))
from mothership_client import MothershipClient


class STATE():
    DOING = 'DOING'
    PAUSE = 'PAUSE'
    SUCCEED = 'SUCCEED'
    ABORT = 'ABORT'


class StepWorkerConnector(SailorWorkerConnector):
    """
    skv 自用 step worker
    """

    def __init__(self, logger, module, operation_type, context_details, params_dict):
        """
        适用于 skv 运维操作
        """
        super().__init__(logger)
        self.module = module
        # 操作类型：在 skv_common 中声明使用
        self.operation_type = operation_type
        self.my_host = socket.getfqdn()
        # 上下文信息，同一个操作未完成继续进行时传入的上下文需要一致
        self.context_details = context_details
        # 可变参数，允许每次执行时可以变动的参数
        self.params_dict = params_dict
        self.work_path = None
        self.steps_yaml = None
        self.current_step = None

    def generate_yaml_func(self):
        """
        根据 yaml 中的固定参数以及传入的参数生成可 sailor_worker 执行的 yaml 文件
        """
        template_yaml = os.path.join(os.environ['SKV_HOME'], 'admintools/skv_step_worker', self.operation_type, TEMPLATE_YAML)
        real_yaml = os.path.join(self.work_path, STEPS_YAML)

        self.params_dict['work_path'] = self.work_path
        # 替换执行机
        self.params_dict.update(self.context_details)
        self.params_dict['local_host'] = [self.my_host]
        self.params_dict['replica_server_hosts'] = \
            MothershipClient().get_host_list_by_role_name(self.module, SKV_REPLICA_SERVER_ROLE_NAME)

        with open(template_yaml, 'r') as f:
            template_str = f.read()
            content = TemplateEngine().render_template_str(template=template_str, variable_dict=self.params_dict)
            yaml_content = yaml.load(content, Loader=yaml.UnsafeLoader)

        # 写入实际需要使用的 yaml
        with open(real_yaml, 'w+') as f:
            yaml.dump(yaml_content, f, default_flow_style=False)

    def init_context(self):
        """
        context data schema
        {
            "HOST": "execute_host",
            "STATE: "DOING/PAUSE/SUCCEED/ABORT",
            "TYPE": "MIGRATE/..",
            "WORK_DIR": "abs_path",
            "CURRENT_STEP", "step_name"
            "DETAILS": dict()
        }
        """
        last_context = MothershipClient().get_latest_module_context(self.module)
        if len(last_context) == 0 or last_context['STATE'] == STATE.SUCCEED or last_context['STATE'] == STATE.ABORT:
            my_context = dict()
            my_context['HOST'] = self.my_host
            my_context['STATE'] = STATE.DOING
            my_context['TYPE'] = self.operation_type
            self.work_path = os.path.join(
                DirectoryInfo().get_runtime_dir_by_product('skv'),
                '%s_migrate_%s' % (self.module, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
            )
            os.mkdir(self.work_path)
            self.steps_yaml = os.path.join(self.work_path, STEPS_YAML)
            my_context['WORK_DIR'] = self.work_path
            my_context['DETAILS'] = self.context_details
            my_context['CURRENT_STEP'] = ''
            MothershipClient().put_module_context(self.module, json.dumps(my_context))
            # 生成 yaml 文件
            self.generate_yaml_func()
        elif last_context['STATE'] == STATE.DOING:
            raise Exception('Operation %s on %s is doing, wait it finished!' % (last_context['TYPE'], last_context['HOST']))
        else:
            if last_context['TYPE'] != self.operation_type:
                raise Exception("different operation_type! current %s vs last %s" % (self.operation_type, last_context['TYPE']))
            if last_context['HOST'] != self.my_host:
                raise Exception("different execute host! current %s vs last %s" % (self.my_host, last_context['HOST']))
            if last_context['DETAILS'] != self.context_details:
                raise Exception("different details! current %s vs last %s" % (
                    str(self.context_details), str(last_context['DETAILS'])))
            self.work_path = last_context['WORK_DIR']
            self.steps_yaml = os.path.join(self.work_path, STEPS_YAML)
            self.current_step = last_context['CURRENT_STEP']
            last_context['STATE'] = STATE.DOING
            MothershipClient().put_module_context(self.module, json.dumps(last_context))

    def execute_one_by_one(self):
        # 读取完整的 yaml
        with open(self.steps_yaml, 'r') as f:
            jobs_desc = yaml.load(f, Loader=yaml.UnsafeLoader)
        jobs_msg = jobs_desc['spec']['jobs_msg']

        # 根据 'CURRENT_STEP' 选取本次需要执行的 jobs, 并生成 yaml
        if self.current_step:
            begin_step_index = 0
            for msg in jobs_msg:
                if msg['step_name'] != self.current_step:
                    begin_step_index += 1
            jobs_msg = jobs_msg[(begin_step_index - 1):]
        jobs_desc['spec']['jobs_msg'] = jobs_msg

        # 写入当前使用的 yaml
        yaml_file = '%s_%s' % (self.steps_yaml, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))
        with open(yaml_file, 'w') as f:
            yaml.dump(jobs_desc, f, default_flow_style=False)

        # 执行 sailor_worker
        err_code, err_msg = self.execute_jobs_from_yaml_file(yaml_file)
        last_context = MothershipClient().get_latest_module_context(self.module)
        if err_code == Status.SUCCESS:
            last_context['STATE'] = STATE.SUCCEED
            last_context['CURRENT_STEP'] = ''
            MothershipClient().put_module_context(self.module, json.dumps(last_context))
            self.logger.info('Operation %s on %s execute succesfully' % (self.operation_type, self.my_host))
        else:
            last_context['CURRENT_STEP'] = err_msg['step_name']
            last_context['STATE'] = STATE.PAUSE
            MothershipClient().put_module_context(self.module, json.dumps(last_context))
            raise Exception('Operation %s on %s execute failed, message: %s' % (self.operation_type, self.my_host, str(err_msg)))
