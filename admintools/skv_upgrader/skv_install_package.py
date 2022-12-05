#!/bin/env python
# -*- coding: UTF-8 -*-

import json
import logging
import os
import requests

from hyperion_helper.os_info_helper import OsInfoHelper
from hyperion_utils.shell_utils import ShellClient


class SkvInstallPackage():

    def __init__(self, module, major_version, package_type, logger=None, api_version='v1'):
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
        self.module = module
        self.major_version = major_version
        self.package_type = package_type

        self.dragon_info_url = 'http://dragon.sensorsdata.cn/api/%s/dragon/latest' % api_version
        if OsInfoHelper.read_distributor_version() == "centos7":
            self.os_version = 'el7'
        elif OsInfoHelper.read_distributor_version() == "centos6":
            self.os_version = 'el6'
        else:
            raise Exception("Can not solve os_version by [cat /etc/redhat-release]!")

    def _get_download_info(self):
        """根据参数获取 skv 的模块信息"""
        data = {
            'module_name': self.module,
            'level': self.package_type,
            "prod_comp": "skv",
            "compat_with_os_distro": self.os_version,
            "version": self.major_version
        }
        headers = {'content-type': 'application/json'}
        response = requests.post(self.dragon_info_url, data=json.dumps(data), headers=headers)
        response.raise_for_status()
        if len(response.json()['results']) == 0:
            raise Exception('MAJOR_VERSION is wrong, can not get skv package!')
        download_url = response.json()['results'][0]['jfrog_download_url']
        package_name = response.json()['results'][0]['name']
        return download_url, package_name

    def download_package_and_return_path(self, path):
        """根据path路径下载所需的包"""
        down_load_url, package_name = self._get_download_info()
        self.logger.info('get newest %s major_version %s package is %s' % (self.module, self.major_version, package_name))
        self.logger.info('install package %s to %s' % (package_name, os.path.join(path, package_name)))
        cmd = 'wget -c %s -O %s' % (
            down_load_url,
            os.path.join(path, package_name))
        ShellClient.check_call(cmd, print_fun=self.logger.debug)
        return os.path.join(path, package_name)
