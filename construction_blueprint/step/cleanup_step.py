#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

把服务都enable了
然后检查一下health
最后 删除sp的模块 然后重启monitor
"""
import os
import sys
import time
import json
import ast

from construction_vehicle.step.base_installer_step import BaseInstallerStep
from hyperion_client.module_service import ModuleService
from hyperion_utils.shell_utils import call, check_call
from hyperion_guidance.arsenal_connector import ArsenalConnector
from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_client.deploy_topo import DeployTopo
from hyperion_client.hyperion_inner_client.inner_deploy_topo import InnerDeployTopo
from hyperion_client.hyperion_inner_client.inner_config_manager import InnerConfigManager

SKV_ADMINTOOLS_ROOT_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'admintools')
if SKV_ADMINTOOLS_ROOT_PATH not in sys.path:
    sys.path.append(SKV_ADMINTOOLS_ROOT_PATH)
from skv_common import SKV_MODULE_NAME_LIST
from recipes import balance_and_wait


class CleanupStep(BaseInstallerStep):

    def update(self):
        # 1. enable 产品线
        service = ModuleService()
        service.enable('skv')
        for module in SKV_MODULE_NAME_LIST:
            if module not in DeployTopo().get_all_module_name_by_product_name('sp'):
                self.logger.info('skip %s because not installed' % module)
                continue

            # 2. 检查 new deploy_topo, server conf, client conf 元数据路径是否存在
            # 实际服务已经起来了已经访问这些元数据了,并且第四步 health 也会获取这些元数据就可以检查, 这里专门加上一个防御性检查吧, 防止瞎操作
            store_connector = ArsenalConnector.get_store().get_instance()
            for subpath in ['', '/meta_server', '/replica_server']:
                deploy_topo_skv_path = store_connector.join_full_path('deploy_topo', 'skv', module + subpath)
                if not store_connector.check_path_exists(deploy_topo_skv_path):
                    raise Exception('skv deploy_topo meta_data[%s] not find!' % deploy_topo_skv_path)
            for subpath in ['server', 'client']:
                conf_sp_path = store_connector.join_full_path('skv', subpath, module)
                if not store_connector.check_path_exists(conf_sp_path):
                    raise Exception('skv %s conf [%s] not find!' % (subpath, conf_sp_path))

            # 3. 集群版 需要 balance一下
            sp_host_lists = InnerDeployTopo().get_host_list_by_role_name('skv', module, 'replica_server')
            if len(sp_host_lists) >= 3:
                balance_and_wait(module, self.logger, print_progress_fun=self.print_msg_to_screen)

            # 4. 跑skvadmin health
            cmd = 'spadmin skv health -m %s --full' % module
            call(cmd, self.logger.debug, 1800)  # 检查个30分钟吧 仅记录结果用

            # 5. 文件备份元数据, 删除sp下的元数据
            # 高危操作, 需要先备份才能删除, 与房东雨 李宁讨论后采用文件备份的方式, 备份路径在 replica 的配置 data_dir 下
            new_server_conf = InnerConfigManager().get_server_conf('skv', module)
            # 记录旧的元数据
            backup_skv_conf = {}
            # 记录旧的元数据路径
            sp_path = {}
            store_connector = ArsenalConnector.get_store().get_instance()
            # (etcd/zk) /sensors_analytics/deploy_topo/sp/skv_offline
            sp_path['deploy_topo'] = store_connector.join_full_path('deploy_topo', 'sp', module)
            # (etcd/zk) /sensors_analytics/sp/server/skv_offline
            sp_path['server_conf'] = store_connector.join_full_path('sp', 'server', module)
            if store_connector.check_path_exists(sp_path['deploy_topo']) and store_connector.check_path_exists(sp_path['server_conf']):
                for item in ['server_conf', 'deploy_topo']:
                    backup_skv_conf[item] = store_connector.dump_node_recursive(sp_path[item], ignore_temporarily_node=True)
                self.logger.info('backup old skv meta data:%s' % backup_skv_conf)
                for host in sp_host_lists:
                    backup_skv_conf_file = os.path.join(self._get_data_dir_by_host(new_server_conf, host), 'old_skv_meta_data.backup')
                    connector = SSHConnector.get_instance(host)
                    cmd = 'echo %s > %s' % (json.dumps(backup_skv_conf), backup_skv_conf_file)
                    connector.check_call(cmd, self.logger.debug)
                    connector.close()
                # 所有的元数据均已经备份完成后, 删除 sp 下的旧元数据
                for item in ['server_conf', 'deploy_topo']:
                    self.logger.info('rm old %s: %s\n%s' % (item, sp_path[item], backup_skv_conf[item]))
                    store_connector.delete_by_path(sp_path[item], recursive=True)
            else:
                # 能够进入此 case 说明是重入执行，并且旧的元数据已经删除了，需检查备份文件是否存在以及内容非空
                for host in sp_host_lists:
                    backup_skv_conf_file = os.path.join(self._get_data_dir_by_host(new_server_conf, host), 'old_skv_meta_data.backup')
                    cmd = "test -f %s && wc -c %s | awk '{print $1}'" % (backup_skv_conf_file, backup_skv_conf_file)
                    cmd_result = connector.run_cmd(cmd, self.logger.debug)
                    cmd_stdout = cmd_result['stdout'].strip()
                    if cmd_result['ret'] != 0 or cmd_stdout is None:
                        raise Exception('host:%s, run[%s] failed, please check!' % (host, cmd))
                    if int(cmd_stdout) < 10:
                        raise Exception('host:%s, backup old skv meta data[%s] failed, please check!' % (host, backup_skv_conf_file))
                    connector.close()

            # 6. 修改 client conf 中的major_version 为 2.0
            # skv/client/skv_offline
            client_conf_sp_path = store_connector.join_full_path('skv', 'client', module)
            client_conf = store_connector.dump_node_recursive(client_conf_sp_path, ignore_temporarily_node=True)
            client_conf_data = ast.literal_eval(client_conf['data'])
            client_conf_data['major_version'] = '2.0'
            client_conf['data'] = json.dumps(client_conf_data)
            self.logger.info('update new server conf: %s\n%s' % (client_conf_sp_path, client_conf))
            store_connector.load_node_recursive(client_conf_sp_path, client_conf)

        # 7. 重启monitor
        cmd = 'spadmin monitor restart'
        check_call(cmd, self.logger.debug, 1800)  # 检查个30分钟吧 避免节点数过多

        # 8. 和姚聪商量了 由于上面这个操作比较骚 需要重启monitor/captain之后等一会 姚聪说10s就够了
        time.sleep(10)

    def check(self):
        return True

    def _get_data_dir_by_host(self, server_conf, hostname):
        group = server_conf['host_group']['replica_server']['hosts_to_groups'].get(hostname)
        return server_conf['host_group']['replica_server']['group_config'][group]['core'].get('data_dir')
