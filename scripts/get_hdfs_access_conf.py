#! /usr/bin/env python3
# coding=utf-8

# 用于获取HDFS的连接信息，即 core-site.xml 和 hdfs-site.xml 的路径

import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'pynest'))
from pyguidance.hadoop_config.GuidanceHadoopConfig import get_access_conf

# 对应为 skv_ecosystem 库中 construction_blueprint/blueprint_2_1/declarative_desc/platform_resources.yaml 申请的资源
resource_owner = 'skv'
resource_name = 'skv_backup'
os.environ['SSDT_SYS_PRODUCT_COMPONENT_NAME'] = 'skv'
os.environ['SSDT_SYS_MODULE_NAME'] = 'skv_offline'

# 返回HDFS的连接信息
print(get_access_conf(resource_owner, resource_name, 'hdfs_dir')['access_conf']['connection_info']['conf_path'])
