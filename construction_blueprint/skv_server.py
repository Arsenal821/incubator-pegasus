# -*- coding: UTF-8 -*-
# 启动skv的脚本 从之前的sp/tools/optools拷贝而来
# skv 2.0 如果不是被mothership 2.0管理，则会使用这部分代码启动

import argparse
from datetime import datetime
import os
import logging
import sys

from hyperion_client.hyperion_inner_client.inner_directory_info import InnerDirectoryInfo
from hyperion_utils.shell_utils import check_call
from hyperion_client.directory_info import DirectoryInfo
from hyperion_client.config_manager import ConfigManager

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from skv_config_manager import generate_std_config

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_common import get_config_file_path, SKV_META_SERVER_ROLE_NAME, \
    SKV_REPLICA_SERVER_ROLE_NAME, SKV_PRODUCT_NAME


def output_header(output_path):
    check_call(
        "echo \"START_TIME: $(date +\"%Y-%m-%d %H:%M:%S\")\" >> {output_path}".format(
            output_path=output_path))


APP_NAME_MAP = {
    SKV_META_SERVER_ROLE_NAME: 'meta',
    SKV_REPLICA_SERVER_ROLE_NAME: 'replica',
}


def get_app_name(role_name):
    return APP_NAME_MAP[role_name] if role_name in APP_NAME_MAP else role_name


def generate_output(module_name, role_name):
    skv_log_dir = InnerDirectoryInfo.get_instance().get_log_dir_by_product(SKV_PRODUCT_NAME)
    log_dir = os.path.join(skv_log_dir, module_name, role_name)

    check_call("mkdir -p {log_dir}".format(log_dir=log_dir))

    symlink_name = "{app_name}.output.ERROR".format(
        app_name=get_app_name(role_name),
    )

    current_datetime = datetime.now()
    timestamp_postfix = current_datetime.strftime('%Y%m%d_%H%M%S_%f')[:-3]
    file_name = "{symlink_name}.{timestamp_postfix}".format(
        symlink_name=symlink_name,
        timestamp_postfix=timestamp_postfix,
    )

    output_path = os.path.join(log_dir, file_name)
    output_header(output_path)

    symlink_path = os.path.join(log_dir, symlink_name)
    try:
        os.unlink(symlink_path)
    except FileNotFoundError:
        pass
    os.symlink(file_name, symlink_path)

    return output_path


def start(args):
    module_name = args.module
    role_name = args.role
    is_install = args.install

    module_home_dir = os.path.join(os.environ['SKV_HOME'], module_name)
    root_dir = os.path.join(os.environ['SKV_HOME'], 'skv_offline')
    root_dir = module_home_dir

    if is_install:
        # 安装时不重新生成配置 直接返回
        config_file = get_config_file_path(module_name, role_name)
    else:
        server_conf = ConfigManager().get_server_conf(SKV_PRODUCT_NAME, module_name)
        config_file = generate_std_config(module_name, role_name, server_conf, 'loose', logging)

    start_script_path = os.path.join(root_dir, 'bin', 'start_server.sh')
    app_name = get_app_name(role_name)

    # 把stdout/stderr重定向到log_dir里面 TODO 日志优化
    print('using config file %s' % config_file)
    output_path = generate_output(module_name, role_name)
    # 修改当前目录为runtime 避免core打满程序目录
    runtime_dir = os.path.join(DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME), 'skv_%s_cores' % app_name)
    os.makedirs(runtime_dir, exist_ok=True)
    os.chdir(runtime_dir)
    start_command = "nohup {start_script_path} {conf_file_path} {app_name} &>> {output_path} & ".format(
        start_script_path=start_script_path,
        conf_file_path=config_file,
        app_name=app_name,
        output_path=output_path,
    )

    check_call(start_command)


def parse():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(help="sub-command help", dest='sub_command')
    subparsers.required = True

    start_parser = subparsers.add_parser(start.__name__, help='start skv server')
    start_parser.add_argument(
        '-m', '--module', required=True,
        help='skv module name: skv_online or skv_offline',
    )
    start_parser.add_argument(
        '-r', '--role', required=True,
        help='skv role name: meta_server, replica_server or collector',
    )
    start_parser.add_argument(
        '-i', '--install', required=False, default=False, action="store_true",
        help=':is_install, yes/no, yes use the local generated config just now, coz zk meta has not persistent',
    )
    start_parser.set_defaults(func=start)

    args = parser.parse_known_args()
    return args[0]


def main():
    args = parse()
    args.func(args)


if __name__ == '__main__':
    main()
