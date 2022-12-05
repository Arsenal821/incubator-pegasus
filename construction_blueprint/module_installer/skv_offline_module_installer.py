# -*- coding: UTF-8 -*-

"""
Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
if sys.modules.get('skv_module_installer'):
    del sys.modules['skv_module_installer']
from skv_module_installer import SkvModuleInstaller


class SkvOfflineModuleInstaller(SkvModuleInstaller):
    """
    skv_offline的部署
    """
    pass
