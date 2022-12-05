# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import importlib.machinery
import os
import sys
import types

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))

from skv_upgrader.skv_upgrader_common import (
    BEFORE_UPGRADE_PY_FILE_NAME,
    BEFORE_UPGRADE_ENTRY_NAME,
)
from skv_upgrader.skv_upgrader_package import SkvUpgraderPackage
from skv_upgrader.skv_upgrader_step import SkvUpgraderStep

from hyperion_guidance.ssh_connector import SSHConnector
from hyperion_utils.shell_utils import check_call


class SkvUpgraderGenerateNewModuleDirStep(SkvUpgraderStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def _decompress_new_package(self):
        cmd = "rm -rf {path}".format(path=self.skv_new_dir)
        check_call(cmd, self.logger.debug)

        cmd = "mkdir -p {path}".format(path=self.skv_new_dir)
        check_call(cmd, self.logger.debug)

        local_pack_name = os.path.basename(self.skv_remote_pack_path)
        local_pack_path = os.path.join(self.skv_new_dir, local_pack_name)

        ssh_client = SSHConnector.get_instance(hostname=self.skv_main_host)
        ssh_client.copy_from_remote(
            remote_file=self.skv_remote_pack_path,
            local_file=local_pack_path,
            print_fun=self.logger.debug,
        )

        upgrader_package = SkvUpgraderPackage(
            local_pack_path, self.skv_module_name, self.logger,
        )
        upgrader_package.extract_module_dir(self.skv_new_dir)

        if not os.path.isdir(self.skv_new_module_dir):
            raise Exception("skv module dir is not included in the tar package "
                            "for the upgrade")

    def _exec_before_upgrade(self):
        upgrade_dir = os.path.join(self.skv_new_module_dir, 'upgrade')
        before_upgrade_py_file_path = os.path.join(
            upgrade_dir, BEFORE_UPGRADE_PY_FILE_NAME)
        if not os.path.isfile(before_upgrade_py_file_path):
            self.logger.warn("{path} for before-upgrade does not exist, just ignore".format(
                path=before_upgrade_py_file_path)
            )
            return

        loader = importlib.machinery.SourceFileLoader(
            '_before_upgrade', before_upgrade_py_file_path)
        mod = types.ModuleType(loader.name)
        loader.exec_module(mod)
        entry_func = getattr(mod, BEFORE_UPGRADE_ENTRY_NAME)
        entry_func()

    def update(self):
        self._decompress_new_package()
        self._exec_before_upgrade()

    def check(self):
        return True

    def rollback(self):
        pass
