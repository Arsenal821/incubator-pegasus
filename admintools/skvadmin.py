#!/bin/env python3
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2018 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

skvadmin是所有skv的工具的集合
这个脚本只是指向了真正使用的脚本
"""
import sys
import os
from bridge_console.admin import main

current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append(current_dir)

sys.exit(main('skv', current_dir))
