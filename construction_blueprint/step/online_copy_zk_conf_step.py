#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys

CONSTRUCTION_BLUEPRINT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if CONSTRUCTION_BLUEPRINT_PATH not in sys.path:
    sys.path.append(CONSTRUCTION_BLUEPRINT_PATH)
from step.base_copy_zk_conf_step import BaseCopyZkConfStep


class OnlineCopyZkConfStep(BaseCopyZkConfStep):
    skv_module = 'skv_online'
