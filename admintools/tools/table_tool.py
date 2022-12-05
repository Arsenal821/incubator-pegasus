#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import os
import sys
import argparse

from stepworker.server import ContextProcessType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_admin_api import SkvAdminApi
from skv_common import get_context_details, SKV_TOOLS_PARTITION_SPLIT_OPERATION, assert_context_consistent, \
    SKV_MODULE_NAME_LIST, SKV_REPLICA_SERVER_ROLE_NAME
from partition_split.partition_split_server import PartitionSplitServer
from skv_maintenance_workers.skv_check_shreded_sst_files_worker import SkvCheckShrededSstFilesWorker
from manual_compaction.main import skv_manual_compaction
from recipes import get_skv_config_manager


class TableTool(BaseTool):
    example_doc = '''
skvadmin table count_data -m skv_online -t temp # count temp table
skvadmin table disk_usage -m skv_online -t temp # show disk usage
skvadmin table partition_split -t sdf_id_mapping -m skv_offline -p 32 --dry_run # split sdf_id_mapping to 32 partitons
'''

    def init_parser(self, subparser):
        # count_data 统计条数
        subparsers = subparser.add_subparsers(dest='table_op')
        count_data_suparser = subparsers.add_parser('count_data', help='count table data rows')
        count_data_suparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')
        count_data_suparser.add_argument('-t', '--table', required=True, help='table name')

        # disk_usage 统计所占的磁盘空间
        disk_usage_suparser = subparsers.add_parser('disk_usage', help='count table used capacity in disk')
        disk_usage_suparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')
        disk_usage_suparser.add_argument('-t', '--table', required=True, help='table name')

        # partition_split 扩分片
        partition_split_subparser = subparsers.add_parser('partition_split', help='change table paritition number')
        partition_split_subparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')
        partition_split_subparser.add_argument('-t', '--table', required=True, help='table name')
        partition_split_subparser.add_argument('-p', '--partition_count', required=False, type=int, help='new partittion number')
        partition_split_subparser.add_argument('--dry_run', default=False, action='store_true', help='if set, will only calculate expect action time')
        partition_split_subparser.add_argument('--multi_set', default=False, action='store_true', help='if set, will use multi_set copy')
        # manual compaction
        manual_compaction_suparser = subparsers.add_parser('manual_compaction', help='run manual compaction for the table')
        manual_compaction_suparser.add_argument(
            '-m', '--module', required=True, choices=SKV_MODULE_NAME_LIST, help='module name, skv_offline/skv_online')
        manual_compaction_suparser.add_argument('-t', '--table', required=True, help='table name')
        manual_compaction_suparser.add_argument('-y', '--assume_yes', default=False, action='store_true', help=argparse.SUPPRESS)
        self.parser = subparser

    def do(self, args):
        if args.table_op == 'disk_usage':
            self.logger.info('Table %s use %.2f MB disk capacity.(Including duplicate data)' % (
                args.table, SkvAdminApi(self.logger, args.module).app_disk_used(args.table)))
        elif args.table_op == 'count_data':
            self.logger.info('counting data ...')
            self.logger.info('Table %s have %s rows data' % (
                args.table, SkvAdminApi(self.logger, args.module).count_table(args.table)))
        elif args.table_op == 'partition_split':
            self._table_partition_split(args.module, args.table, args.partition_count, args.dry_run, args.multi_set)
        elif args.table_op == 'manual_compaction':
            self._table_manual_compaction(args.module, args.table, args.assume_yes)
        else:
            self.parser.print_help()

    def _table_partition_split(self, module_name, table, partition_count, dry_run, multi_set):
        """扩分片 把分片数调大 目前的版本只能是停止写入->拷贝->rename"""
        self.logger.info('start change %s %s table partition to %s, dry_run=%s' % (
            module_name, table, partition_count, dry_run))
        api = SkvAdminApi(self.logger, module_name)
        if not partition_count:
            partition_count = api.get_big_table_default_partition_count()
            self.logger.info('default partition_count = %d' % partition_count)

        # 参数检查和前置检查，当前置检查需要执行一些操作时ret=0,，不需要ret=1
        ret = 1
        if not get_context_details() or dry_run:
            if table not in api.get_all_avaliable_table_name():
                raise Exception('cannot find table %s in %s!' % (table, module_name))

            # 检查partition是不是2的指数
            # https://stackoverflow.com/questions/57025836/how-to-check-if-a-given-number-is-a-power-of-two
            def check_is_power_of_two(n):
                return (n & (n - 1) == 0) and n != 0
            if not check_is_power_of_two(partition_count):
                raise Exception('invalid partition count %d: should be power of two!' % partition_count)

            context_type = ContextProcessType.NE_CREATE_E_ASK_MULTIPLE
            # 不真正执行扩分片，估算扩分片所需要的时间
            if dry_run:
                server = PartitionSplitServer(self.logger, module_name, table, partition_count, context_type)
                scan_old_table_time, scan_new_table_time = server.estamite_old_new_scan_time()
                estimate_time_seconds = scan_old_table_time + scan_new_table_time * 2
                self.logger.info('table %s total estimate %d seconds' % (table, estimate_time_seconds))
                self.logger.info('dry_run is true, do nothing and exit.')

            # 执行扩分片前置检查
            ret = self._pre_check_partition_split(module_name, table)
        if dry_run or 1 != ret:
            return 0

        # 当存在上下文
        old_context = get_context_details()
        if old_context:
            # 确认上下文是一致的
            new_context = {'table': table, 'operation': SKV_TOOLS_PARTITION_SPLIT_OPERATION, 'partition_count': partition_count, 'module_name': module_name, 'multi_set': multi_set}
            assert_context_consistent(self.logger, old_context, new_context)
            self.logger.error('you can choose to continue partition split: enter [yes], or rollback to old table: enter [rollback_to_old]')
            resp = input()
            # 回滚到上一步，然后继续执行扩分片
            if resp == 'yes':
                context_type = ContextProcessType.NE_CREATE_E_CONTINUE
                server = PartitionSplitServer(self.logger, module_name, table, partition_count, context_type, multi_set)
                server.init_context()
                server.rollback_current_step()
                server.execute_one_by_one()
            # 执行回滚到最开始
            elif resp == 'rollback_to_old':
                context_type = ContextProcessType.NE_EXCEPTION_E_CONTINUE
                server = PartitionSplitServer(self.logger, module_name, table, partition_count, context_type, multi_set)
                server.init_context()
                server.rollback_one_by_one()
                self.logger.info('rollback is complete, you can re-execute partition split!')
            else:
                raise Exception('invalid response!')
        # 当不存在上下文时，执行扩分片操作
        else:
            if table not in api.get_all_avaliable_table_name():
                raise Exception('table %s not exist or is unavailable!' % table)
            self.logger.error('THIS OPERATION WILL DROP CURRENT TABLE AND COPY DATA TO NEW TABLE, ARE YOU SURE?')
            self.logger.info('please enter [yes] to confirm')
            resp = input()
            if resp != 'yes':
                raise Exception('invalid response!')
            server = PartitionSplitServer(self.logger, module_name, table, partition_count, context_type, multi_set)
            server.init_context()
            server.execute_one_by_one()

    def _pre_check_partition_split(self, module_name, table):
        # 扩分前置检查，在dry_run和扩分片时都需要执行
        self.logger.info("pre-check before partition split...")
        # 确保配置项 '[pegasus.server]rocksdb_filter_type' 在 'server_conf.replica_server' 里被设置，且值为 'common';
        # 如果不满足条件 则提示手动设置此配置为 'common'
        section, name, value = 'pegasus.server', 'rocksdb_filter_type', 'common'
        skv_config_manager = get_skv_config_manager(module_name, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        if not skv_config_manager.check_final_config_value(section, name, value):
            cmd = 'skvadmin config set -m %s -r %s -s %s -n %s -v %s' % (
                module_name, SKV_REPLICA_SERVER_ROLE_NAME, section, name, value)
            self.logger.error("'rocksdb_filter_type' not set in the replica_server config"
                              " or its config value is not 'common' please execute [{cmd}] to set it"
                              " and then restart the skv module".format(cmd=cmd))
            return 0

        # 检查此表是否有"sst碎文件"问题
        one_skv_check_shreded_file_worker = SkvCheckShrededSstFilesWorker(module_name=module_name, logger=self.logger)
        has_shreded_sst_file = one_skv_check_shreded_file_worker.check_one_table_shreded_sst_file(table, True)
        if has_shreded_sst_file:
            self.logger.warn("please enter [ignore_compaction] to continue the process anyway or stop otherwise...")
            resp = input()
            if resp != 'ignore_compaction':
                self.logger.info("please execute[skvadmin table manual_compction -m {module} -t {table}]".format(module=module_name, table=table))
                return 0
        self.logger.info("pre-check result: success!")
        return 1

    def _table_manual_compaction(self, module, table, assume_yes):
        api = SkvAdminApi(self.logger, module)
        if table not in api.get_all_avaliable_table_name():
            raise Exception('cannot find table [%s] in %s!' % (table, module))
        self.logger.error('THIS OPERATION WILL MANUAL COMPACTION TABLE [{table}], ARE YOU SURE?'.format(table=table))
        if not assume_yes:
            self.logger.info('please enter [yes] to confirm')
            resp = input()
            if 'yes' != resp:
                raise Exception('invalid response!')
        skv_manual_compaction(self.logger, module, table)
