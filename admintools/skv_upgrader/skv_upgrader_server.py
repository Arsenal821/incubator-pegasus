# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os

from hyperion_client.deploy_topo import DeployTopo
from stepworker.server import BaseServer
from stepworker.steps_info_utils import StepsInfoGenerator
from skv_common import SKV_TOOLS_STEPWORKER_NAME
from skv_common import SKV_TOOLS_UPGRADER_STEPS_PATH
from skv_upgrader.skv_upgrader_common import HOT_UPGRADE_TYPE, COLD_UPGRADE_TYPE
from skv_upgrader.skv_upgrader_common import str_is_camel_case
from skv_upgrader.skv_upgrader_common import camel_case_to_snake_case

RUN_ON_MAIN_HOST = 'execute'
RUN_ON_ALL_HOST = 'all'


class SkvUpgraderServer(BaseServer):
    def __init__(self, upgrade_type, replica_server_host_list, meta_server_host_list,
                 logger, context_details, context_type):
        self.skv_upgrade_type = upgrade_type
        self.skv_replica_server_host_list = replica_server_host_list
        self.skv_meta_server_host_list = meta_server_host_list

        self.skv_host_list = list(set(replica_server_host_list) | set(meta_server_host_list))
        self.skv_all_host_list = DeployTopo().get_all_host_list()

        # 构造server
        super().__init__(
            hosts=self.skv_all_host_list,
            name=SKV_TOOLS_STEPWORKER_NAME,
            step_class_path=SKV_TOOLS_UPGRADER_STEPS_PATH,
            logger=logger,
            context_details=context_details,
            context_type=context_type,
        )

    @staticmethod
    def generate_step(class_name, run_hosts, step_host=None, constructor_args=None):
        if not str_is_camel_case(class_name):
            raise Exception("{class_name} is not valid camel-case class name".format(
                class_name=class_name))

        if not class_name.startswith('SkvUpgrader'):
            raise Exception("as a class name, {class_name} is missing 'SkvUpgrader' "
                            "in the prefix".format(class_name=class_name))

        if not class_name.endswith('Step'):
            raise Exception("as a class name, {class_name} is missing 'Step' "
                            "in the postfix".format(class_name=class_name))

        if isinstance(run_hosts, list):
            run_hosts = ','.join(run_hosts)
        elif not isinstance(run_hosts, str):
            raise Exception(
                "{run_hosts} is neither a list nor a string, but a type of "
                "{type}".format(run_hosts=run_hosts, type=type(run_hosts).__name__)
            )

        name = class_name[len('SkvUpgrader'):len(class_name) - len('Step')]
        name = camel_case_to_snake_case(name)
        if step_host:
            name += "_" + step_host

        return {
            'name': name,
            'class_name': class_name,
            'constructor_args': constructor_args,
            'hosts': run_hosts,
        }

    def generate_host_step(self, step_host, **kwargs):
        if not step_host:
            raise Exception("step_host should be specified")

        kwargs['step_host'] = step_host

        return self.generate_step(**kwargs)

    def generate_hot_initial_step_list(self):
        return list(map(
            lambda kwargs: self.generate_step(**kwargs),
            [
                {
                    'class_name': 'SkvUpgraderGenerateNewModuleDirStep',
                    'run_hosts': RUN_ON_ALL_HOST,
                },
            ]
        ))

    def generate_hot_replica_server_step_list(self, host):
        return list(map(
            lambda kwargs: self.generate_host_step(host, **kwargs),
            [
                {
                    'class_name': 'SkvUpgraderStopMetaServerInstanceStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderSafelyStopReplicaServerStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderBackupModuleDirStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderReplaceWithNewModuleDirStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderFixShellConfigStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderStartAndCheckReplicaServerStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderStartMetaServerInstanceStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
            ]
        ))

    def generate_hot_meta_server_step_list(self, host):
        return list(map(
            lambda kwargs: self.generate_host_step(host, **kwargs),
            [
                {
                    'class_name': 'SkvUpgraderStopMetaServerInstanceStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderBackupModuleDirStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderReplaceWithNewModuleDirStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderStartMetaServerInstanceStep',
                    'constructor_args': {'host': host},
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
            ]
        ))

    def generate_hot_other_server_step_list(self, host):
        return list(map(
            lambda kwargs: self.generate_host_step(host, **kwargs),
            [
                {
                    'class_name': 'SkvUpgraderBackupModuleDirStep',
                    'run_hosts': host,
                },
                {
                    'class_name': 'SkvUpgraderReplaceWithNewModuleDirStep',
                    'run_hosts': host,
                },
            ]
        ))

    def generate_hot_final_step_list(self):
        return list(map(
            lambda kwargs: self.generate_step(**kwargs),
            [
                {
                    'class_name': 'SkvUpgraderBalanceAndWaitStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
            ]
        ))

    def generate_hot_step_list(self):
        step_list = self.generate_hot_initial_step_list()

        for replica_server_host in self.skv_replica_server_host_list:
            step_list.extend(
                self.generate_hot_replica_server_step_list(replica_server_host)
            )

        for meta_server_host in self.skv_meta_server_host_list:
            if meta_server_host in self.skv_replica_server_host_list:
                continue

            step_list.extend(
                self.generate_hot_meta_server_step_list(meta_server_host)
            )

        for host in self.skv_all_host_list:
            if host in self.skv_host_list:
                continue

            step_list.extend(
                self.generate_hot_other_server_step_list(host)
            )

        step_list.extend(
            self.generate_hot_final_step_list()
        )

        return step_list

    def generate_cold_step_list(self):
        return list(map(
            lambda kwargs: self.generate_step(**kwargs),
            [
                {
                    'class_name': 'SkvUpgraderGenerateNewModuleDirStep',
                    'run_hosts': RUN_ON_ALL_HOST,
                },
                {
                    'class_name': 'SkvUpgraderStopMetaServerRoleStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderStopReplicaServerRoleStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderBackupModuleDirStep',
                    'run_hosts': RUN_ON_ALL_HOST,
                },
                {
                    'class_name': 'SkvUpgraderReplaceWithNewModuleDirStep',
                    'run_hosts': RUN_ON_ALL_HOST,
                },
                {
                    'class_name': 'SkvUpgraderFixShellConfigStep',
                    'run_hosts': RUN_ON_ALL_HOST,
                },
                {
                    'class_name': 'SkvUpgraderStartReplicaServerRoleStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderStartMetaServerRoleStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
                {
                    'class_name': 'SkvUpgraderWaitAllHealthyStep',
                    'run_hosts': RUN_ON_MAIN_HOST,
                },
            ]
        ))

    def _get_step_list_generator(self):
        generator_map = {
            HOT_UPGRADE_TYPE: self.generate_hot_step_list,
            COLD_UPGRADE_TYPE: self.generate_cold_step_list,
        }

        return generator_map[self.skv_upgrade_type]

    def generate_step_list(self):
        generator = self._get_step_list_generator()
        return generator()

    def gen_steps_yml(self):
        """重写gen_steps.yml方法 主要修改是增加了可以修改部署步骤的方法"""
        steps_yml = self.get_steps_yml()
        if os.path.isfile(steps_yml):
            self.logger.info("{steps_yml} has been existing".format(steps_yml=steps_yml))
            return

        step_list = self.generate_step_list()
        self.logger.info("generated step info list: {step_list}".format(step_list=step_list))

        StepsInfoGenerator.gen_steps_info_by_step_info_list(
            step_list, save=True, steps_yml_path=steps_yml)

    @staticmethod
    def reset_skv_context(logger, verbose=True):
        BaseServer.reset_context(SKV_TOOLS_STEPWORKER_NAME, logger, verbose=verbose)
