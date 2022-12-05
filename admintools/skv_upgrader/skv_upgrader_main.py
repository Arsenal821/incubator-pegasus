# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author wangdan(wangdan@sensorsdata.cn)
"""

import argparse
from functools import wraps
import os
import re
import socket
import sys
import traceback

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from base_tool import BaseTool

from hyperion_client.directory_info import DirectoryInfo
from stepworker.context.context_manager import ContextProcessType
from skv_common import SKV_OFFLINE_MODULE_NAME, SKV_META_SERVER_ROLE_NAME, SKV_REPLICA_SERVER_ROLE_NAME, \
    SKV_MODULE_NAME_LIST, SKV_ONLINE_MODULE_NAME, SKV_PRODUCT_NAME, SKV_TOOLS_STEPWORKER_NAME, SKV_TOOLS_UPGRADE_OPERATION, \
    check_exists_module, fix_shell_config, get_context_details, get_operation_history, get_installed_skv_modules

from recipes import (
    balance_and_wait,
    start_skv_cluster,
    get_skv_config_manager,
    check_health,
)
from skv_admin_api import SkvAdminApi
from skv_upgrader.skv_install_package import SkvInstallPackage
from skv_upgrader.skv_upgrader_common import (
    HOT_UPGRADE_TYPE, COLD_UPGRADE_TYPE, UPGRADE_TYPE_SET,
    get_version_from_module_package,
    get_current_version,
    get_version_from_files,
)
from skv_upgrader.skv_upgrader_package import SkvUpgraderPackage
from skv_upgrader.skv_upgrader_server import SkvUpgraderServer
from utils.sa_utils import get_os_version


class SkvUpgraderMain(object):
    @staticmethod
    def _extract_version_prefix(ver):
        match = re.match(r'^(\d+\.\d+\.\d+).*', ver)
        if not match:
            raise Exception("invalid skv version: {version}".format(version=ver))
        return match.group(1)

    @classmethod
    def _match_version_prefix(cls, from_version, to_version):
        from_prefix = cls._extract_version_prefix(from_version)
        to_prefix = cls._extract_version_prefix(to_version)
        if from_prefix != to_prefix:
            raise Exception(
                "current version {from_version} and target version {to_version} "
                "do not have a common version prefix".format(
                    from_version=from_version, to_version=to_version,
                )
            )

    def __init__(
        self, module_name, upgrade_type, remote_pack_path, major_version, package_type, logger,
        ignore_support_upgrade_type=False,
    ):
        self.module_name = module_name
        self.upgrade_type = upgrade_type
        self.skv_remote_pack_path = remote_pack_path
        self.major_version = major_version
        self.package_type = package_type
        self.logger = logger
        self.ignore_support_upgrade_type = ignore_support_upgrade_type

        self.meta_server_host_list = get_skv_config_manager(module_name, SKV_META_SERVER_ROLE_NAME, self.logger).get_host_list()
        if len(self.meta_server_host_list) <= 0:
            raise Exception("{module_name} has no meta server".format(
                module_name=self.module_name))

        self.replica_server_host_list = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger).get_host_list()
        if len(self.replica_server_host_list) <= 0:
            raise Exception("{module_name} has no replica server".format(
                module_name=self.module_name))

        self.step_class_path = os.path.join(
            os.path.abspath(os.path.dirname(os.path.abspath(__file__))),
            self.upgrade_type,
        )

        self.skv_admin_api = SkvAdminApi(self.logger, self.module_name)
        self.skv_module_dir = os.path.join(os.environ['SKV_HOME'], self.module_name)
        self.other_meta_server_nodes_map = {}
        for module in get_installed_skv_modules():
            self.other_meta_server_nodes_map[module] = SkvAdminApi(self.logger, module).meta_server_endpoint.split(',')
        meta_server_nodes_map = {
            name: node_list
            for name, node_list in self.other_meta_server_nodes_map.items()
        }
        fix_shell_config(SKV_MODULE_NAME_LIST, meta_server_nodes_map)

    def check_ensure_no_context(self):
        context_details = get_context_details()
        if context_details:
            self.logger.error("context details {context_details}".format(context_details=context_details))
            raise Exception("You have a uncompleted skv upgrader worker, please continue execute it!")

    def check_support_hot_upgrade(self):
        if len(self.replica_server_host_list) < 3:
            raise Exception("hot upgrade is unsupported, since the number "
                            "of replica servers is less than 3")

    def check_support_cold_upgrade(self):
        pass

    def check_support_upgrade_type(self):
        if self.ignore_support_upgrade_type:
            return

        checker_map = {
            HOT_UPGRADE_TYPE: self.check_support_hot_upgrade,
            COLD_UPGRADE_TYPE: self.check_support_cold_upgrade,
        }
        checker_map[self.upgrade_type]()

    def check_ops(self):
        if self.upgrade_type == HOT_UPGRADE_TYPE:
            return

        tables = self.skv_admin_api.get_all_tables_has_op()
        tables = [t for t in tables if t not in {'temp', 'impala_historical_profile'}]
        if tables:
            raise Exception(
                "cold upgrade cannot be continued since some app is being written or read: "
                "app_name={app_name}".format(app_name=tables)
            )

    class BeforeUpgrade:
        @staticmethod
        def check(module_updater):
            # 升级前检查

            @wraps(module_updater)
            def wrapper(self, *args, **kwargs):
                # 检查确保升级上下文是不存在的
                self.check_ensure_no_context()

                # 检查是否支持当前升级类型
                self.check_support_upgrade_type()

                # 检查冷升级是否仍然有表在读写数据(除temp等可以忽略读写流量的表外)
                self.check_ops()

                if not check_health(self.logger, self.module_name):
                    raise Exception('cluster not healthy!')

                return module_updater(self, *args, **kwargs)

            return wrapper

    def init_context_dict(self):
        from_version = get_current_version(
            self.skv_module_dir, self.module_name, self.logger, self.skv_admin_api)
        to_version = get_version_from_files(
            self.skv_remote_pack_path, self.module_name, get_version_from_module_package, self.logger,
        )
        self._match_version_prefix(from_version, to_version)

        return {
            'module_name': self.module_name,
            'operation': SKV_TOOLS_UPGRADE_OPERATION,
            'upgrade_type': self.upgrade_type,
            'package_type': self.package_type,
            'main_host': socket.getfqdn(),
            'skv_remote_pack_path': self.skv_remote_pack_path,
            'from_version': from_version,
            'to_version': to_version,
        }

    def _build_request_context_dict(self):
        return {
            'module_name': self.module_name,
            'operation': SKV_TOOLS_UPGRADE_OPERATION,
            'upgrade_type': self.upgrade_type,
            'package_type': self.package_type,
            'skv_remote_pack_path': self.skv_remote_pack_path,
            'major_version': self.major_version,
        }

    @BeforeUpgrade.check
    def update_module(self, ask_multiple=False):
        # 升级逻辑

        context_type = (ContextProcessType.NE_CREATE_E_ASK_MULTIPLE if ask_multiple
                        else ContextProcessType.NE_CREATE_E_EXCEPTION)

        # 如果传参 major_version 需要下载包并赋值给 skv_remote_pack_path
        if self.major_version:
            self.skv_remote_pack_path = SkvInstallPackage(
                self.module_name,
                self.major_version,
                self.package_type,
                self.logger).download_package_and_return_path(DirectoryInfo().get_runtime_dir_by_product(SKV_PRODUCT_NAME))

        # 如果存在上下文就抛异常，没有上下文就创建
        step_worker = SkvUpgraderServer(
            upgrade_type=self.upgrade_type,
            replica_server_host_list=self.replica_server_host_list,
            meta_server_host_list=self.meta_server_host_list,
            logger=self.logger,
            context_details=self.init_context_dict(),
            context_type=context_type,
        )

        # 初始化上下文
        step_worker.init_context()

        # 执行升级步骤
        step_worker.execute_one_by_one()

    @staticmethod
    def _raise_context_details_mismatch(current_context_details, request_context_details,
                                        current_context_name, request_context_name=None):
        if not request_context_name:
            request_context_name = current_context_name

        error_info = ("mismatched context found:\n"
                      "mismatched_current_context_item={current_context_item},\n"
                      "mismatched_request_context_item={request_context_item},\n"
                      "current_context_details={current_context_details},\n"
                      "request_context_details={request_context_details}").format(
            current_context_item={current_context_name: current_context_details[current_context_name]},
            request_context_item={request_context_name: request_context_details[request_context_name]},
            current_context_details=current_context_details,
            request_context_details=request_context_details,
        )
        raise Exception(error_info)

    @staticmethod
    def _major_version_match(current_to_version, request_major_version):
        """
        Examples:
            current_to_version: 1.12.3-0.5.0 (f2e254ee7fcb764d8bd0036c72bd9267b3acf30a)
            request_major_version: 0.6.0
        """

        begin = current_to_version.find('-')
        if begin < 0:
            begin = 0
        else:
            begin += len('-')

        end = current_to_version.find('(', begin)
        if end < 0:
            end = len(current_to_version)

        current_major_version = current_to_version[begin:end].strip()
        if not current_major_version:
            raise Exception(
                "cannot extract major version from {current_to_version}".format(
                    current_to_version=current_to_version,
                )
            )

        return current_major_version == request_major_version

    @staticmethod
    def _check_version_or_pack_path(current_context_details, request_context_details):
        if request_context_details['major_version']:
            if not SkvUpgraderMain._major_version_match(
                current_context_details['to_version'],
                request_context_details['major_version']
            ):
                SkvUpgraderMain._raise_context_details_mismatch(
                    current_context_details, request_context_details,
                    'to_version', 'major_version',
                )
            return

        if current_context_details['skv_remote_pack_path'] != request_context_details['skv_remote_pack_path']:
            SkvUpgraderMain._raise_context_details_mismatch(
                current_context_details, request_context_details, 'skv_remote_pack_path',
            )

    @staticmethod
    def _check_context_details(current_context_details, request_context_details):
        for context_name in ['module_name', 'operation', 'upgrade_type', 'package_type']:
            if current_context_details[context_name] == request_context_details[context_name]:
                continue

            SkvUpgraderMain._raise_context_details_mismatch(
                current_context_details, request_context_details, context_name,
            )

        SkvUpgraderMain._check_version_or_pack_path(current_context_details, request_context_details)

    def _check_context(self):
        current_context_details = get_context_details()
        if not current_context_details:
            # 上下文为空就抛异常
            raise Exception(
                "context for '{context_name}' cannot be found !".format(
                    context_name=SKV_TOOLS_STEPWORKER_NAME,
                )
            )

        request_context_details = self._build_request_context_dict()

        self.logger.debug("current_context_details={current_context_details}".format(
            current_context_details=current_context_details))
        self.logger.debug("request_context_details={request_context_details}".format(
            request_context_details=request_context_details))

        self._check_context_details(current_context_details, request_context_details)

    def upgrader_continue(self):
        # 继续逻辑

        # 检查上下文
        self._check_context()

        # 如果存在上下文就按上下文进度继续，没有上下文就抛异常
        step_worker = SkvUpgraderServer(
            upgrade_type=self.upgrade_type,
            replica_server_host_list=self.replica_server_host_list,
            meta_server_host_list=self.meta_server_host_list,
            logger=self.logger,
            context_details=None,
            context_type=ContextProcessType.NE_EXCEPTION_E_CONTINUE,
        )

        # 初始化上下文
        step_worker.init_context()

        # 执行升级步骤
        step_worker.execute_one_by_one()

    def upgrader_rollback(self):
        # 回滚逻辑

        # 检查上下文
        self._check_context()

        # 如果存在上下文就按上下文进度继续，没有上下文就抛异常
        step_worker = SkvUpgraderServer(
            upgrade_type=self.upgrade_type,
            replica_server_host_list=self.replica_server_host_list,
            meta_server_host_list=self.meta_server_host_list,
            logger=self.logger,
            context_details=None,
            context_type=ContextProcessType.NE_EXCEPTION_E_CONTINUE,
        )

        # 初始化上下文
        step_worker.init_context()

        # 执行升级步骤
        step_worker.rollback_current_step()

    @staticmethod
    def upgrader_reset_context(logger):
        context_details = get_context_details()
        if not context_details:
            # 上下文为空就抛异常
            raise Exception(
                "context for '{context_name}' cannot be found !".format(
                    context_name=SKV_TOOLS_STEPWORKER_NAME,
                )
            )

        operation = context_details['operation']
        if operation != SKV_TOOLS_UPGRADE_OPERATION:
            raise Exception(
                "since operation is not '{operation}', {context_name}'s context cannot be reset".format(
                    operation=operation, context_name=SKV_TOOLS_STEPWORKER_NAME,
                )
            )

        logger.warn(
            "context for '{context_name}' will be reset, please enter 'yes' to confirm:".format(
                context_name=SKV_TOOLS_STEPWORKER_NAME,
            )
        )
        answer = input()
        if answer.strip() != 'yes':
            raise Exception("confirm failed. do nothing and quit")

        module_name = context_details['module_name']
        upgrade_type = context_details['upgrade_type']

        try:
            start_skv_cluster(module_name, logger)
        except Exception:
            logger.warn('start skv failed! will try to continue..')
            logger.warn(traceback.format_exc())

        try:
            api = SkvAdminApi(logger, module_name)
            api.set_add_secondary_max_count_for_one_node('DEFAULT')
            if upgrade_type == HOT_UPGRADE_TYPE:
                balance_and_wait(module_name, logger)
        except Exception:
            logger.warn('reset add secondary & balance failed! will try to continue..')
            logger.warn(traceback.format_exc())

        SkvUpgraderServer.reset_skv_context(logger, verbose=False)

    @staticmethod
    def read_history(module, logger):
        # 读取历史升级数据

        module_map = {}
        for row in get_operation_history(logger):
            if row['operation'] != SKV_TOOLS_UPGRADE_OPERATION:
                continue

            context_details = row['details']
            row['from_version'] = context_details['from_version']
            row['to_version'] = context_details['to_version']
            row['upgrade_type'] = context_details['upgrade_type']

            module_name = context_details['module_name']
            if module_name not in module_map:
                module_map[module_name] = []
            module_map[module_name].append(row)

        for _module, row_list in module_map.items():
            if module != _module:
                continue
            logger.info("{module} recent {count} upgrade history:".format(
                module=_module, count=len(module_map[_module]))
            )
            for fields in row_list:
                logger.info(
                    "[{id}] {start_time}-{end_time} {from_version} -> {to_version} "
                    "({upgrade_type}) {status} {server_host}[{tmp_work_path}]".format(**fields)
                )


class UpgraderTool(BaseTool):
    example_doc = '''
    # online hot upgrade to 2.0.3
    skvadmin upgrader -t hot -m skv_offline -v 2.0.3
    # offline cold upgrade
    skvadmin upgrader -t cold -m skv_offline -p /sensorsdata/main/runtime/skv/skv_offline-2.0.2.1-el7.tar
    '''

    def init_parser(self, upgrader_parser):
        upgrader_parser.add_argument(
            '-m', '--module', required=False, default=None,
            choices=[SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME],
            help="skv module name: skv_online or skv_offline",
        )
        upgrader_parser.add_argument(
            '-t', '--type', required=False, default=None,
            choices=UPGRADE_TYPE_SET,
            help="upgrade type: hot or cold",
        )
        # 下面两个二选一必选
        path_group = upgrader_parser.add_mutually_exclusive_group(required=False)
        path_group.add_argument(
            '-p', '--package', dest='pack_path',
            help="the path of the upgrade pack",
        )
        path_group.add_argument(
            '-v', '--version', dest='major_version',
            help="the specified skv version of the upgrade pack",
        )
        upgrader_parser.add_argument(
            '-l', '--level', type=str, required=False,
            default='release', dest='package_type',
            choices=['release', 'test', 'develop'],
            help='package level: release or test or develop, work only set -v',
        )
        upgrader_exclusive_group = upgrader_parser.add_mutually_exclusive_group(required=False)
        upgrader_exclusive_group.add_argument(
            '-c', '--continue', dest='cont', required=False, action='store_true',
            help="continue current upgrade",
        )
        upgrader_exclusive_group.add_argument(
            '-r', '--rollback', required=False, action='store_true',
            help="rollback current upgrade",
        )
        upgrader_exclusive_group.add_argument(
            '-s', '--reset_context', required=False, action='store_true', help=argparse.SUPPRESS,
        )
        upgrader_parser.set_defaults(func=self.upgrader)

        upgrader_subparsers = upgrader_parser.add_subparsers(dest='upgrader_subparser_name')

        history_parser = upgrader_subparsers.add_parser(
            'history', help="skv upgrader history")
        history_parser.required = True
        history_parser.add_argument(
            '-m', '--module', required=True, default=SKV_OFFLINE_MODULE_NAME,
            choices=[SKV_OFFLINE_MODULE_NAME, SKV_ONLINE_MODULE_NAME],
            help="skv module name: skv_online or skv_offline",
        )
        history_parser.set_defaults(func=self.history)

    def do(self, args):
        args.func(args)

    def upgrader(self, args):
        if args.reset_context:
            SkvUpgraderMain.upgrader_reset_context(self.logger)
            return

        if not args.module:
            raise Exception("module name should be specified")
        if not args.type:
            raise Exception("upgrade type should be specified")

        if (not args.major_version) and (not args.pack_path):
            raise Exception("either major_version or pack_path should be specified")

        check_exists_module(args.module)

        if args.pack_path:
            args.pack_path = os.path.abspath(args.pack_path)
            if not os.path.isfile(args.pack_path):
                raise Exception("{pack_path} does not exist".format(pack_path=args.pack_path))
            # 检查包内模块名和参数模块名是否一致
            SkvUpgraderPackage(args.pack_path, args.module, self.logger).check_module_and_os_version_from_package()
            # 如果包名包含 os 版本，检查包名 os 版本与系统 os 是否一致
            os_version = get_os_version()
            if ("el7" in os.path.basename(args.pack_path) and os_version != "centos7") or \
                    ("el6" in os.path.basename(args.pack_path) and os_version != "centos6"):
                raise Exception("os version in package {package_name} is different from {os_version}!".format(
                    package_name=os.path.basename(args.pack_path), os_version=os_version)
                )

        if args.major_version:
            if len(args.major_version.split('.')) != 3:
                raise Exception("skv major version(%s) format is wrong!!!" % args.major_version)
            for s in args.major_version.split('.'):
                if not s.isdigit():
                    raise Exception("skv major version(%s) format is wrong!!!" % args.major_version)

        upgrader_main = SkvUpgraderMain(
            module_name=args.module,
            upgrade_type=args.type,
            remote_pack_path=args.pack_path,
            major_version=args.major_version,
            package_type=args.package_type,
            logger=self.logger,
            ignore_support_upgrade_type=False,
        )

        if args.cont:
            upgrader_main.upgrader_continue()
        elif args.rollback:
            upgrader_main.upgrader_rollback()
        else:
            upgrader_main.update_module(ask_multiple=False)

    def history(self, args):
        SkvUpgraderMain.read_history(args.module, self.logger)
