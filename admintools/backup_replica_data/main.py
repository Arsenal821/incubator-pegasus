#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
import argparse
import os
import socket
import sys

from hyperion_guidance.ssh_connector import SSHConnector
from stepworker.server import BaseServer, ContextProcessType

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool

from skv_admin_api import SkvAdminApi
from skv_common import SKV_MODULE_NAME_LIST, SKV_TOOLS_STEPWORKER_NAME, \
    SKV_TOOLS_BACUP_REPLICA_OPERATION, assert_context_consistent, SKV_REPLICA_SERVER_ROLE_NAME, get_context_details
from recipes import get_skv_config_manager


class BackUpReplicaDataTool(BaseTool):
    example_doc = '''
# to simply backup & restore
skvadmin backup_replica_data -m skv_offline \\
    --backup_path_on_each_host /sensorsdata/main/packages/backup_tmp \\
# to migrate
skvadmin backup_replica_data -m skv_offline \\
    --backup_path_on_each_host /sensorsdata/main/packages/backup_tmp \\
    --restore_from_backup_local_dir /sensorsdata/main/runtime/migrate_dir \\
    --new_meta_server_list 10.1.1.1:8170,10.1.1.2:8170,10.1.1.3:8170
'''

    def init_parser(self, subparser):
        subparser.add_argument(
            '-m', '--module',
            required=True,
            choices=SKV_MODULE_NAME_LIST,
            help='module name, skv_offline/skv_online')
        subparser.add_argument(
            '--backup_path_on_each_host',
            required=True,
            help='replica will make tar and store in this path on EACH REPLICA SERVER HOST.')
        subparser.add_argument(
            '--remote_migration_dir',
            required=False,
            help='remote directory(abspath) for saving migration data.')
        subparser.add_argument(
            '--new_meta_server_list',
            required=False,
            help='remote meta_server list, comma-separated.')
        subparser.add_argument(
            '--port',
            required=False,
            default=22,
            type=int,
            help='原集群机器的 ssh 连接端口，默认 22\n'
        )
        subparser.add_argument(
            '--password',
            required=False,
            default='',
            help='新集群 meta_server_list 第一台机器的 sa_cluster 连接 ssh 密码\n'
        )
        subparser.add_argument(
            '--only_make_snapshot',
            required=False,
            action='store_true',
            help='if set, will only make snapshot and then quit. you can coninue by rerun without this option.')
        # 隐藏接口
        subparser.add_argument(
            '--all_yes',
            help=argparse.SUPPRESS,
            default=False,
            action='store_true')

    def do(self, args):
        """停服务 把每个replica server的数据目录打tar包然后拷贝走(可以到每台机器的同一个目录 也可以最终拷贝到执行机上)
        高危操作 所以名字比较长 避免有人意会随便执行
        """
        # 检查迁移参数
        if (args.new_meta_server_list and not args.remote_migration_dir) or (not args.new_meta_server_list and args.remote_migration_dir):
            raise Exception('new_meta_server_list and remote_migration_dir need both None or not!')

        new_replica_server_list = None
        # 迁移逻辑
        if args.new_meta_server_list:
            if '' == args.password:
                raise Exception('You need input password for new cluster machine!')
            cmd = 'spadmin config get server -m skv_offline -p skv -n replica_server_list -c | grep "^ " | sed \'s/"//g;s/ *$//g\' | awk \'{print $1}\' | xargs | sed \'s/ //g\''
            ssh_client = SSHConnector.get_instance(
                hostname=args.new_meta_server_list.split(',')[0].split(':')[0],
                user='sa_cluster',
                password=args.password,
                ssh_port=args.port,
            )
            new_replica_server_list = ssh_client.check_output(cmd).strip()

            current_replica_server_count = SkvAdminApi(self.logger, args.module).get_replica_server_num()
            # 判断新旧环境的集群部署一致
            if not (current_replica_server_count == 1 and len(new_replica_server_list.split(',')) == 1) and not (current_replica_server_count >= 3 and len(new_replica_server_list.split(',')) >= 3):
                raise Exception('invalid new_replica_server_list %s; The count of remote replica server is not match current!' % new_replica_server_list)
        args.backup_path_on_each_host = os.path.abspath(args.backup_path_on_each_host)

        context_details = {
            'execute_host': socket.getfqdn(),
            'backup_path_on_each_host': args.backup_path_on_each_host,
            'module': args.module,
            'operation': SKV_TOOLS_BACUP_REPLICA_OPERATION,
            'new_replica_server_list': new_replica_server_list,
            'remote_migration_dir': args.remote_migration_dir,
        }
        old_context_details = get_context_details()
        if old_context_details:
            # 确认上下文是一致的
            assert_context_consistent(self.logger, old_context_details, context_details)
        # 必须有高危警告
        self.logger.error('!!!THIS COMMAND WILL BACKUP %s DATA!!!' % args.module.upper())
        if not args.all_yes:
            self.logger.warn('please enter[y] to confirm!!!')
            response = input()
            if response != 'y':
                self.logger.info('response[%s] != y. Goodbye!!!' % response)
                return 1
        # 执行的机器包括本机+所有replica server所在的机器
        skv_config_manager = get_skv_config_manager(args.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        hosts = list(set([socket.getfqdn()] + skv_config_manager.get_host_list()))
        step_class_path = os.path.join(os.environ['SKV_HOME'], 'admintools/backup_replica_data')
        context_type = ContextProcessType.NE_CREATE_E_CONTINUE if args.all_yes else ContextProcessType.NE_CREATE_E_ASK_MULTIPLE
        # 如果只制作snapshot 只要跑到snapshot_done即可返回 否则跑到最后一步
        # 注意这个现在继续会 *重复* 跑snapshot_done 不过没关系 这个步骤啥也没干
        to_step = 'snapshot_done' if args.only_make_snapshot else 'cleanup'
        server = BaseServer(
            hosts=hosts,
            name=SKV_TOOLS_STEPWORKER_NAME,
            support_rollback=False,
            step_class_path=step_class_path,
            logger=self.logger,
            context_type=context_type,
            context_details=context_details,
            to_step=to_step)
        server.init_context()
        ret = server.execute_one_by_one()
        if args.only_make_snapshot:  # 只创建snapshot 需要提醒还需要继续 注意如果to_step!=最后一步没法区分是不是结束了 因此都要打印。。。
            self.logger.info('make snapshot success; you can start read/write operation.')
            self.logger.warn('BACKUP NOT DONE YET! please remove --only_make_snapshot and rerun to pack all the snapshots!')
        elif ret != 0:  # 执行失败直接返回 不打印后面的提示
            return ret
        elif args.new_meta_server_list:
            self.logger.info('please run %s/scp_metadata.sh on local!' % args.backup_path_on_each_host)
            self.logger.info('please run %s/scp_data.sh on each host manually!' % args.backup_path_on_each_host)
        return 0
