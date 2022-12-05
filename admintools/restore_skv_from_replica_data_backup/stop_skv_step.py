#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys

from hyperion_client.module_service import ModuleService

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from restore_skv_from_replica_data_backup.base_restore_step import BaseRestoreStep
from skv_common import SKV_PRODUCT_NAME


class StopSkvStep(BaseRestoreStep):

    def do_update(self):
        ModuleService().stop(SKV_PRODUCT_NAME, self.module_name)
