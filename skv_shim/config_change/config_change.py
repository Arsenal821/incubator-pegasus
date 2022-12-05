#!/bin/env python
# -*- coding: UTF-8 -*-

"""
配置类型及说明见文档 : https://doc.sensorsdata.cn/pages/viewpage.action?pageId=224175436 "配置管理"
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))
from skv_role_base import SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME, SKV_ONLINE_MODULE_NAME, MODULE_NAME_ROLE_NAME_TO_DATA_DIR_NAME


def module_config_change(params):
    ret = {}
    return ret


def role_config_group_change(module_name, role_name, params):
    hardware_params = params["hardware_configs"]
    if SKV_META_SERVER_ROLE_NAME == role_name:
        d = __role_config_group_meta_server_change(module_name, hardware_params)
    elif SKV_REPLICA_SERVER_ROLE_NAME == role_name:
        d = __role_config_group_replica_server_change(module_name, hardware_params)
    else:
        d = {}

    return d


def __role_config_group_replica_server_change(module_name, hardware_params):
    one_gb = 1024 * 1024 * 1024
    mem_gb = int(hardware_params["mem"])
    multiple = int(mem_gb / 28)

    if multiple >= 1:
        if module_name == SKV_ONLINE_MODULE_NAME:
            multiple *= 2
        rocksdb_block_cache_capacity = multiple * one_gb
        rocksdb_total_size_across_write_buffer = int(rocksdb_block_cache_capacity * 0.5)
        rocksdb_max_open_files = min(multiple * 128, 512)
    else:
        # default config, in the past time
        rocksdb_block_cache_capacity = 629145600
        rocksdb_total_size_across_write_buffer = 503316480
        rocksdb_max_open_files = 128
        if module_name == SKV_ONLINE_MODULE_NAME:
            rocksdb_block_cache_capacity = 2147483648
            rocksdb_total_size_across_write_buffer = 1073741824
            rocksdb_max_open_files = 256

    data_dirs = hardware_params[MODULE_NAME_ROLE_NAME_TO_DATA_DIR_NAME[module_name][SKV_REPLICA_SERVER_ROLE_NAME]]

    return {
        'replica_server.ini': {
            'pegasus.server|rocksdb_block_cache_capacity': rocksdb_block_cache_capacity,
            'pegasus.server|rocksdb_total_size_across_write_buffer': rocksdb_total_size_across_write_buffer,
            'pegasus.server|rocksdb_max_open_files': rocksdb_max_open_files,
            "core|data_dir": data_dirs[0],
            "replication|slog_dir": data_dirs[0],
            "replication|data_dirs": ','.join(['data%d:%s' % (i, d) for i, d in enumerate(data_dirs)]),
        },
        'role_group_hardware_config': {
            'mem': mem_gb
        }
    }


def __role_config_group_meta_server_change(module_name, hardware_params):
    data_dirs = hardware_params[MODULE_NAME_ROLE_NAME_TO_DATA_DIR_NAME[module_name][SKV_META_SERVER_ROLE_NAME]]

    return {
        'meta_server.ini': {
            'core|data_dir': data_dirs[0],
        }
    }
