# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import tarfile
import os
import yaml

from hyperion_helper.os_info_helper import OsInfoHelper


class SkvUpgraderPackage(object):
    def __init__(self, pack_path, module_name, logger):
        self.pack_path = pack_path
        self.module_name = module_name
        self.logger = logger

    def extract_module_dir(self, target_dir):
        with tarfile.open(self.pack_path, 'r:*') as tar:
            tar.extractall(target_dir)

    def get_version_from_file(self, version_file_name, version_parser):
        version_file_path = os.path.join(self.module_name, version_file_name)
        with tarfile.open(self.pack_path, 'r:*') as tar:
            for member in tar.getmembers():
                if version_file_path not in member.name:
                    continue

                f = tar.extractfile(member)
                return version_parser(
                    f, self.pack_path, self.module_name, version_file_name,
                )

        raise Exception("{version_file_path} not found".format(
            version_file_path=version_file_path)
        )

    def check_module_and_os_version_from_package(self):
        with tarfile.open(self.pack_path, 'r:*') as tar:
            module_name_in_tar = os.path.basename(tar.getnames()[1])

            dragon_pkg_path = os.path.join('.', module_name_in_tar, 'dragon_pkg.yml')
            yaml_dict = yaml.safe_load(tar.extractfile(dragon_pkg_path))
            if yaml_dict['dragon_pkg']['module_name'] != self.module_name:
                raise Exception(
                    "resolve module in {pack_path} is {module_name_in_tar},"
                    "different from args --module {module_name}".format(
                        pack_path=self.pack_path,
                        module_name_in_tar=yaml_dict['dragon_pkg']['module_name'],
                        module_name=self.module_name)
                )
            os_version = OsInfoHelper.read_distributor_version()
            if (os_version == "centos7" and "el7" not in yaml_dict['dragon_pkg']['os_distro']) or \
                    (os_version == "centos6" and "el6" not in yaml_dict['dragon_pkg']['os_distro']):
                raise Exception("os version in package {package_name} is different from {os_version}!".format(
                    package_name=os.path.basename(self.pack_path), os_version=os_version)
                )
