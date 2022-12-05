# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
"""

import configparser
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import traceback
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from migrate.steps.skv_migrate_step import SkvMigrateStep

ROCKSDB_TARGET_FILE_MB = 8


class SkvPreCopyDataStep(SkvMigrateStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        self.generate_config_file()
        self.try_connect()
        self.check_cluster_health()
        self.check_table_in_cluster()
        self.generate_copy_manage_file()
        self.check_src_rocksdb_file_capacity()
        self.check_src_cluster_table_qps()

    def check(self):
        return True

    def rollback(self):
        pass

    def try_connect(self):
        """测试新老集群的连通性"""
        self.change_config_file_to_source_cluster()
        try:
            self.src_cluster_api.get_version()
        except Exception as exc:
            self.logger.debug(traceback.format_exc())
            raise Exception('connect to {cluster_list} failed, {exc}'.format(cluster_list=self.src_meta_server_list, exc=exc))

    def generate_config_file(self):
        """给执行机的 skv tool config.ini 补充 src&dest meta_server_list"""
        shutil.copyfile(self.shell_config_path, self.src_config_path)
        shutil.copyfile(self.shell_config_path, self.dest_config_path)

        config_parser = configparser.ConfigParser()
        # we should not convert all keys to lowercase,
        # since we have "@CLUSTER_NAME@ = @CLUSTER_ADDRESS@"
        config_parser.optionxform = lambda option: option

        # 补充 src.ini
        config_parser.read(self.src_config_path)
        config_parser.set("pegasus.clusters", self.module, self.src_meta_server_list)
        config_parser.set("pegasus.clusters", 'target_cluster', self.dest_meta_server_list)
        with open(self.src_config_path, "w") as f:
            config_parser.write(f)

        # 补充 dest.ini
        config_parser.read(self.dest_config_path)
        config_parser.set("pegasus.clusters", self.module, self.dest_meta_server_list)
        with open(self.dest_config_path, "w") as f:
            config_parser.write(f)

    def check_table_in_cluster(self):
        """检查该表是否在目标集群以及原集群存在"""
        self.change_config_file_to_destination_cluster()
        dest_cluster_table_name = self.dest_cluster_api.get_all_avaliable_table_name()

        self.change_config_file_to_source_cluster()
        src_cluster_table_name = self.src_cluster_api.get_all_avaliable_table_name()

        if self.assign_table_names:
            diff_table_from_src_cluster = set(self.assign_table_names).difference(set(src_cluster_table_name))
            if len(diff_table_from_src_cluster) != 0:
                raise Exception('table {tables} not in source cluster!'.format(tables=diff_table_from_src_cluster))
            different_tables = set(self.assign_table_names).difference(set(dest_cluster_table_name))
        else:
            dest_cluster_table_name.extend(self.skip_table_names)
            different_tables = set(src_cluster_table_name).difference(set(dest_cluster_table_name))

        if len(different_tables) != 0:
            raise Exception('cluster {meta_server_list} not have table {tables}.'.format(
                meta_server_list=self.dest_meta_server_list,
                tables=str(different_tables),
            ))

    def generate_copy_manage_file(self):
        """生成 copy_manage_file.yml 管理拷贝进度"""
        copy_data_detail = {}
        self.change_config_file_to_source_cluster()
        if self.assign_table_names is not None:
            for table_name in self.assign_table_names:
                copy_data_detail[table_name] = {}
        else:
            for table_name in self.src_cluster_api.get_all_avaliable_table_name():
                if table_name not in self.skip_table_names:
                    copy_data_detail[table_name] = {}

        if len(copy_data_detail) == 0:
            raise Exception('you cannot only migrate system table!')

        # 检查 dest_cluster 中该表是否有数据
        self.change_config_file_to_destination_cluster()
        for table_name in copy_data_detail.keys():
            cmd = '{skv_tool_run_script} shell --cluster {meta_server_list}'.format(
                skv_tool_run_script=self.skv_tool_run_script,
                meta_server_list=self.dest_meta_server_list,
            )
            proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                                    stderr=subprocess.PIPE, start_new_session=True)
            try:
                _, err = proc.communicate('use {table}\ncount_data -c'.format(table=table_name).encode('utf-8'), timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                _, err = proc.communicate()
            if 'Count done, total 0 rows.' not in str(err, encoding="utf-8"):
                msg = 'table {table_name} in cluster {meta_server_list} count_data not empty!'.format(
                    table_name=table_name, meta_server_list=self.dest_meta_server_list
                )
                raise Exception(msg)

        with open(self.copy_manage_file, 'w') as f:
            yaml.dump(copy_data_detail, f, default_flow_style=False)

    def check_src_rocksdb_file_capacity(self):
        """检查原集群 rocksdb 文件的大小是否合规以及配置是否合规"""
        self.change_config_file_to_source_cluster()
        table_list = self.src_cluster_api.get_big_capacity_table_list()
        src_cluster_ssh_client = self.get_src_cluster_ssh_client()

        rocksdb_filter_type_msg = str()
        compaction_msg = str()
        rocksdb_sst_size_msg = str()

        # 设置原集群 conf，避免 copy 异常
        cmd = "spadmin skv config get -m {module} -r replica_server -P".format(module=self.module)
        ret = src_cluster_ssh_client.run_cmd(cmd, self.logger.debug)
        conf = json.loads(ret['stdout'])
        if not conf.get('pegasus.server'):
            rocksdb_filter_type_msg = "on source cluster format json and execute [spadmin skv config set -m {module} " \
                                      "-r replica_server -s pegasus.server -v '{{\"rocksdb_filter_type\": " \
                                      "\"common\"}}']\n".format(module=self.module)
        else:
            if not conf['pegasus.server'].get('rocksdb_filter_type')\
                    or conf['pegasus.server']['rocksdb_filter_type'] != 'common':
                rocksdb_filter_type_msg = "on source cluster execute [spadmin skv config set -m {module} -r " \
                                          "replica_server -s 'pegasus.server' -n rocksdb_filter_type -v " \
                                          "common]\n".format(module=self.module)

        for table in table_list:
            if table in self.skip_table_names or self.assign_table_names is not None and table not in self.assign_table_names:
                continue
            file_mb, file_num = self.src_cluster_api.get_table_file_mb_and_num(table)
            if file_mb / file_num < ROCKSDB_TARGET_FILE_MB * 0.9:
                compaction_msg = compaction_msg + 'on source cluster execute [skvadmin table manual_compaction -m {module} -t {table}]'.format(
                    module=self.module, table=table)
        if compaction_msg != '':
            compaction_msg = "Please check the disk space of the old cluster before manual compaction\n" + compaction_msg
            # 老集群修改 sst 文件大小配置
            cmd = "spadmin skv config get -m {module} -r replica_server -s 'pegasus.server' " \
                  "-n rocksdb_target_file_size_base -P".format(module=self.module)
            ret1 = src_cluster_ssh_client.run_cmd(cmd, self.logger.debug)['stdout']
            cmd = "spadmin skv config get -m {module} -r replica_server -s 'pegasus.server' " \
                  "-n rocksdb_max_bytes_for_level_base -P".format(module=self.module)
            ret2 = src_cluster_ssh_client.run_cmd(cmd, self.logger.debug)['stdout']
            if '33554432\n' != ret1 or '335544320\n' != ret2:
                msg1 = "on source cluster execute [spadmin skv config set -m {module} -r replica_server -s " \
                       "'pegasus.server' -n rocksdb_target_file_size_base -v 33554432]\n".format(module=self.module)
                msg2 = "on source cluster execute [spadmin skv config set -m {module} -r replica_server -s " \
                       "'pegasus.server' -n rocksdb_max_bytes_for_level_base -v 335544320]\n".format(module=self.module)
                rocksdb_sst_size_msg = msg1 + msg2

        if rocksdb_filter_type_msg != '' or compaction_msg != '':
            self.change_config_file_to_destination_cluster()
            msg = 'please execute commands followed in order on source cluster {meta_server_list}.\n'.format(
                meta_server_list=self.src_meta_server_list
            )
            if rocksdb_filter_type_msg != '' or rocksdb_sst_size_msg != '':
                restart_msg = "on source cluster execute [spadmin restart -m {module} -p sp/skv]\n".format(
                    module=self.module
                )
                msg = msg + rocksdb_filter_type_msg + rocksdb_sst_size_msg + restart_msg
            msg = msg + compaction_msg

            raise Exception(msg)

    def check_src_cluster_table_qps(self):
        """确定原集群需要迁移的表没有 qps"""
        with open(self.copy_manage_file, 'r') as f:
            copy_data_detail = yaml.load(f)

        self.change_config_file_to_source_cluster()
        for table_name in copy_data_detail.keys():
            if self.src_cluster_api.check_table_has_ops(table_name):
                msg = 'source cluster {meta_server} table {table_name} has qps, check it!'.format(
                    meta_server=self.src_meta_server_list,
                    table_name=table_name
                )
                raise Exception(msg)
