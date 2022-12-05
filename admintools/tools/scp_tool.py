#!/bin/env python3
# -*- coding: UTF-8 -*-
"""
Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
@author zhangzichu@sernsorsdata.cn
@brief skv scp tool
"""

import argparse
import yaml

from hyperion_utils.shell_utils import run_cmd
from hyperion_client.config_manager import ConfigManager

TIMEOUT = 600


def do(args):
    file = open(args.log, 'w')
    ip_list = get_ip_list(args)

    timeout = args.timeout if args.timeout is not None else TIMEOUT

    for ip in ip_list:
        cmd_arg = args.cmd
        scp_cmd = "ssh %s '%s'" % (ip, cmd_arg)
        result = run_cmd(cmd=scp_cmd, timeout=timeout)
        if result['ret'] == 0:
            print("%s 执行成功." % ip)
        else:
            print("%s 执行失败, 请看日志文件 %s" % (ip, args.log))
            file.write(ip + '\n')


def get_ip_list(args):
    if args.ips_file is not None:
        file = open(args.ips_file, 'r')
        lines = file.readlines()
        return list(map(lambda x: x.strip(), lines))
    elif args.migration_conf_file is not None:
        with open(args.migration_conf_file) as f:
            d = yaml.safe_load(f)
            ip_mapping_list = d['ip_mapping']
            return list(map(lambda x: x['old_ip'], ip_mapping_list))
    else:
        conf = ConfigManager().get_server_conf("sp", "skv_offline")
        meta_server_list = conf["replica_server_list"]
        return list(map(lambda x: x.split(":")[0], meta_server_list))


def init_parser(parser):
    subparsers = parser.add_subparsers(dest='scp')
    subparsers.required = True
    # alter
    alter_subparsers = subparsers.add_parser('scp', help='执行 scp 命令')
    alter_subparsers.add_argument('-c', '--cmd', required=True, help='指定执行的 cmd 指令')
    alter_subparsers.add_argument('-l', '--log', required=True, help='指定日志的输出目录, 记录执行失败的ip')
    alter_subparsers.add_argument('-t', '--timeout', required=False, help='执行 cmd 指令的超时时间')
    alter_subparsers.add_argument('-f', '--ips_file', required=False, help='指定执行的ip')
    alter_subparsers.add_argument('-m', '--migration_conf_file', required=False, help='指定 skv 迁移的配置文件')


def main():
    # 定义 parser
    argparse.ArgumentParser(description="skv scp 工具")
    parser = argparse.ArgumentParser()
    init_parser(parser)
    args = parser.parse_args()
    return do(args)


if __name__ == '__main__':
    main()
