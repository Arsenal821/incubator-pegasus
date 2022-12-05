#!/bin/env python
# -*- coding: UTF-8 -*-

import socket
from enum import Enum

from hyperion_guidance.ssh_connector import SSHConnector


class MemoryUnit(Enum):
    BYTE = 1
    KB = 2
    MB = 3
    GB = 4


FREE_CMD_DICT = {
    MemoryUnit.BYTE: "b",
    MemoryUnit.KB: "k",
    MemoryUnit.MB: "m",
    MemoryUnit.GB: "g"
}


def get_memory_info_from_ip(ip, unit, logger):
    return get_memory_info_from_host(socket.getfqdn(ip), unit, logger)


def get_memory_info_from_host(host, unit, logger):
    """
                  total        used        free      shared  buff/cache   available
Mem:          31900       17721        4571        1666        9606       12039
Swap:             0           0           0
    """
    free_cmd = "free -" + FREE_CMD_DICT[unit]
    connector = SSHConnector.get_instance(host)
    free_cmd_result = connector.check_output(free_cmd, logger.debug)

    cmd_result_lines = free_cmd_result.split("\n")
    mem_num_line = cmd_result_lines[1]
    memory_nums = mem_num_line.split()[1:]

    memory_info_dict = dict()
    memory_info_dict['total'] = int(memory_nums[0])
    memory_info_dict['used'] = int(memory_nums[1])
    memory_info_dict['free'] = int(memory_nums[2])
    memory_info_dict['shared'] = int(memory_nums[3])
    memory_info_dict['buff/cache'] = int(memory_nums[4])
    memory_info_dict['available'] = int(memory_nums[5])

    return memory_info_dict


def get_disk_info_from_host_by_dirs(host, data_dirs, logger):
    # 返回机器上指定目录所有磁盘的空间信息
    # return {"data0": {'Filesystem': '/dev/vdb', 'Size': 50268, 'Available': 55039, 'Use%': 54}}
    df_cmd = "df -m"
    dir_list = data_dirs.split(',')
    for dir in dir_list:
        df_cmd = df_cmd + " " + dir.split(':')[1]
    connector = SSHConnector.get_instance(host)
    free_cmd_result = connector.check_output(df_cmd, logger.debug)
    disk_info_lines = free_cmd_result.splitlines()[1:]
    disk_used_info = dict()
    for i in range(len(dir_list)):
        line = disk_info_lines[i].split()
        # key = dir_list[i].split(':')[0]
        key = dir_list[i]
        disk_used_info[key] = dict()
        disk_used_info[key]['Filesystem'] = line[0]
        disk_used_info[key]['Size'] = int(line[1])
        disk_used_info[key]['Used'] = int(line[2])
        disk_used_info[key]['Available'] = int(line[3])
        disk_used_info[key]['Use%'] = line[4]
    return disk_used_info


if __name__ == '__main__':
    pass
