#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
@brief

skv 自动检查任务的执行的入口

对于仅检测的任务，使用一个简单的 for 循环遍历所有任务，通过信号做超时管理；
对于修复任务，考虑到可能会对系统造成影响，因此需要用 stepworker 加锁，另外也顺便用 stepworker 做超时管理和进度记录
"""
import enum
import importlib
import os
import re
import signal
import socket
import sys
import traceback
import yaml

from stepworker.server import BaseServer, ContextProcessType
from stepworker.steps_info_utils import StepsInfoGenerator

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_common import SKV_TOOLS_MAINTENANCE_OPERATION, SKV_TOOLS_STEPWORKER_NAME, get_context_details, \
    SKV_PRODUCT_NAME, SKV_MODULE_NAME_LIST, get_installed_skv_modules


SKV_MAINTENANCE_ROOT_DIR = os.path.join(
    os.environ['SKV_HOME'],
    'admintools/skv_maintenance_workers'
)
WORK_MANAGE_YAML_FILE = os.path.join(SKV_MAINTENANCE_ROOT_DIR, 'maintenance_work.yml')
PRIORITY_LEVEL_LIST = ['A', 'B', 'C']


class WorkType(enum.Enum):
    DIAGNOSE_WORK = 1
    REPAIR_WORK = 2


def uncamelize(camelCaps, separator='_'):
    """将驼峰命名的字符串转为下划线拼接的"""
    pattern = re.compile(r'([A-Z]{1})')
    sub = re.sub(pattern, separator + r'\1', camelCaps).lower()
    return sub[1:]


def get_worker_object(work_name, skv_module, logger):
    """根据 work_name 获取该 worker 对象"""
    class_file_name = uncamelize(work_name)
    class_file_dir = os.path.join(SKV_MAINTENANCE_ROOT_DIR, '{name}.py'.format(name=class_file_name))

    spec = importlib.util.spec_from_file_location(class_file_name, class_file_dir)
    pymodule = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pymodule)

    work_object = getattr(pymodule, work_name)
    if not issubclass(work_object, BaseWorker):
        msg = 'from {file} class {class_name} is not a sub class for BaseWorker!'.format(
            file=class_file_dir,
            class_name=work_name
        )
        raise Exception(msg)

    return work_object(skv_module, logger)


def work_within(action, timeout_seconds, logger):
    # 收到信号 SIGALRM 后的回调函数，第一个参数是信号的数字，第二个参数是the interrupted stack frame.
    def handle(signum, frame):
        if signum == signal.SIGALRM:
            raise Exception('timeout!')
        else:
            raise Exception('caught signal %d' % signum)

    # 设置信号和回调函数
    signal.signal(signal.SIGALRM, handle)
    # 设置 num 秒的闹钟
    signal.alarm(timeout_seconds)
    ret = action()
    # 关闭闹钟
    signal.alarm(0)
    return ret


def get_ordered_work_list(lowest_level):
    """获取所有的work信息 过滤后排序"""
    with open(WORK_MANAGE_YAML_FILE, 'r') as f:
        works_detail = yaml.safe_load(f)

    level_candidates = PRIORITY_LEVEL_LIST[:PRIORITY_LEVEL_LIST.index(lowest_level) + 1]
    work_list_filter = filter(lambda x: x['priority_level'] in level_candidates, works_detail)
    return sorted(work_list_filter, key=lambda x: x['priority_level'])


def diagnose(skv_module, logger, lowest_level='B'):
    """执行 maintenance_job.yml 中配置的 lowest_level 以上的 repair 任务"""
    logger.info('start {skv_module} maintenance diagnose.'.format(skv_module=skv_module))
    for work in get_ordered_work_list(lowest_level):
        try:
            work_object = get_worker_object(work['class_name'], skv_module, logger)
            is_abnormal = work_within(work_object.is_state_abnormal, work['check_timeout'], logger)
            if not is_abnormal:
                logger.info('check {work}: pass'.format(work=work['class_name']))
                continue

            if work_object.self_remedy():
                logger.info('check {work} failed, start dignose'.format(work=work['class_name']))
            else:
                logger.warn('check {work} failed, start dignose'.format(work=work['class_name']))

            work_within(work_object.diagnose, work['diagnose_timeout'], logger)

        except Exception:
            logger.warn('caught exception while doing %s' % work['class_name'])
            logger.warn(traceback.format_exc())

        # level A 按照 yml 文件内配置任务的顺序执行，有一个异常就退出
        if work['priority_level'] == PRIORITY_LEVEL_LIST[0]:
            raise Exception('please fix this and run again!')
    logger.info('end {skv_module} maintenance diagnose.'.format(skv_module=skv_module))


class SkvMaintenanceRepairServer(BaseServer):
    def __init__(self, skv_module, logger, lowest_level):
        self.lowest_level = lowest_level
        context_details = {
            'operation': SKV_TOOLS_MAINTENANCE_OPERATION,
            'module_name': skv_module,
            'lowest_level': lowest_level,
        }
        # 构造server
        super().__init__(
            hosts=[socket.getfqdn()],  # 仅在本机执行
            name=SKV_TOOLS_STEPWORKER_NAME,
            step_class_path=os.path.dirname(os.path.abspath(__file__)),
            logger=logger,
            context_details=context_details,
            context_type=ContextProcessType.NE_CREATE_E_EXCEPTION
        )

    def gen_steps_yml(self):
        """重写gen_steps.yml方法 主要修改是增超时"""
        steps_yml = self.get_steps_yml()
        if os.path.isfile(steps_yml):
            self.logger.info("{steps_yml} has been existing".format(steps_yml=steps_yml))
            return
        step_list = []
        for work in get_ordered_work_list(self.lowest_level):
            step_list.append({
                'name': work['class_name'],
                'hosts': 'all',
                'timeout': work['check_timeout'] + work['repair_timeout'],
                'class_name': 'SkvRepairStep',
                'constructor_args': {
                    'name': work['class_name'],
                    'priority_level': work['priority_level']
                }
            })
        StepsInfoGenerator.gen_steps_info_by_step_info_list(
            step_list, save=True, steps_yml_path=steps_yml)


def repair(skv_module, logger, lowest_level='B'):
    """执行 maintenance_job.yml 中配置的 lowest_level 以上的 diagnose 任务"""
    context_details = get_context_details()
    # 之前失败的修复动作 可以忽略
    if context_details and context_details['operation'] == SKV_TOOLS_MAINTENANCE_OPERATION:
        BaseServer.reset_context(SKV_TOOLS_STEPWORKER_NAME, logger, verbose=False)

    try:
        server = SkvMaintenanceRepairServer(skv_module, logger, lowest_level)
        server.init_context()
        server.execute_one_by_one()
    except Exception:
        logger.error('caught exception while trying to repair %s' % skv_module)
        logger.warn(traceback.format_exc())

    context_details = get_context_details()
    # 失败的修复动作 可以忽略
    if context_details and context_details['operation'] == SKV_TOOLS_MAINTENANCE_OPERATION:
        BaseServer.reset_context(SKV_TOOLS_STEPWORKER_NAME, logger, verbose=False)
    return False


if __name__ == '__main__':
    """主函数是 无人值守时 由scheduler调度的自动修复任务"""
    from utils.sa_utils import init_runtime_logger
    file_name, logger = init_runtime_logger(SKV_PRODUCT_NAME, 'skv_maintenance')
    modules = get_installed_skv_modules()
    for module in SKV_MODULE_NAME_LIST:
        if module in modules:
            repair(module, logger, 'C')
            # 为了阅读添加的隔断
            logger.info('============================================================')
