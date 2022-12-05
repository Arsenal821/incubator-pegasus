#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

逐一把sp的meta server停了，然后逐一启动skv的meta server
注意停可以用ModuleService，但是启动不可以，需要用local start
"""
import time

from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_client.module_service import ModuleService
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo
from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo
from hyperion_client.deploy_topo import DeployTopo
from hyperion_guidance.ssh_connector import SSHConnector


class BaseReplaceMetaServerStep(BaseInstallerStep):
    skv_module = None  # 需要修改为对应的模块

    def update(self):
        if self.skv_module not in DeployTopo().get_all_module_name_by_product_name('sp'):
            self.logger.info('skip replace meta for %s because not installed' % self.skv_module)
            return
        sp_meta_host_lists = InnerDeployTopo().get_host_list_by_role_name('sp', self.skv_module, 'meta_server')
        sp_replica_host_lists = InnerDeployTopo().get_host_list_by_role_name('sp', self.skv_module, 'replica_server')
        service = ModuleService()
        for host in sp_meta_host_lists:
            # 1. 停止老的 可以用ModuleService
            self.print_msg_to_screen('stopping sp %s meta_server %s' % (self.skv_module, host))
            service.stop('sp', self.skv_module, 'meta_server', host)

            # 2. 当前节点上只有 meta_server 时，备份 sp 下老的代码，升级一半时防止误启动，备份完成后删除
            ssh_connector = SSHConnector.get_instance(host)
            if host not in sp_replica_host_lists:
                sp_runtime_dir = InnerDirectoryInfo.get_instance().get_home_dir_by_product(product_name='sp')
                ret = ssh_connector.call('cd %s && ls %s.bak' % (sp_runtime_dir, self.skv_module))
                if ret == 0:
                    ret = ssh_connector.call('cd %s && ls %s' % (sp_runtime_dir, self.skv_module))
                    if ret == 0:
                        ssh_connector.check_call('cd %s && rm -rf %s' % (sp_runtime_dir, self.skv_module))
                else:
                    ssh_connector.check_call('cd %s && cp -r %s %s.bak' % (sp_runtime_dir, self.skv_module, self.skv_module))
                    ssh_connector.check_call('cd %s && rm -rf %s' % (sp_runtime_dir, self.skv_module))

            # 3. 启动新的 需要用local
            self.print_msg_to_screen('starting skv %s meta_server %s' % (self.skv_module, host))
            ssh_connector.check_call('spadmin local start -p skv -m %s -r meta_server' % self.skv_module, self.logger.debug)
            ssh_connector.close()

            # 避免频繁切主可能带来的问题 sleep 5s
            time.sleep(5)

        # 这里等待 'group check' 的间隔时间
        self.print_msg_to_screen('sleep 100s(group check interval) to wait the cluster to be definitely stable...')
        time.sleep(100)

    def check(self):
        return True
