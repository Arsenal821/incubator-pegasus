# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author liguohao(liguohao@sensorsdata.cn)
"""

import datetime
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
import yaml

from enum import Enum

from hyperion_utils.shell_utils import check_call

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from migrate.steps.skv_migrate_step import SkvMigrateStep


class CopyState(Enum):
    RUNNING = 1
    FINISHED = 2
    INTERRUPTED = 3
    FAILED = 4


class SkvCopyDataStep(SkvMigrateStep):
    def __init__(self):
        super().__init__()

    def backup(self):
        pass

    def update(self):
        self.check_cluster_health()
        table_list = self.generate_in_copy_table_list()
        self.wait_for_copy_data_done(table_list)

    def check(self):
        return True

    def rollback(self):
        pass

    def try_execute_copy_data_return_pid(self, table_name):
        """执行 copy_data & 返回 pid"""
        self.change_config_file_to_destination_cluster()
        cmd = '{skv_tool_run_script} shell --cluster {meta_server_list}'.format(
            skv_tool_run_script=self.skv_tool_run_script,
            meta_server_list=self.dest_meta_server_list,
        )

        # 检查 dest_cluster 中该表是否有数据
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
            self.logger.warn(msg)

        # bulk_load 模式通常用于灌数据，但是在灌数据过程中因为消耗大量的CPU和IO，对读性能会产生较大影响造成读延迟陡增、超时率升高等
        self.dest_cluster_api.set_table_env(table_name, 'rocksdb.usage_scenario', 'bulk_load')

        copy_output_file = os.path.join(self.manage_root_dir, 'copy_{table}.out'.format(table=table_name))
        copy_err_file = os.path.join(self.manage_root_dir, 'copy_{table}.err'.format(table=table_name))
        copy_execute_file = os.path.join(self.manage_root_dir, 'copy_{table}.sh'.format(table=table_name))
        program_id_file = os.path.join(self.manage_root_dir, 'copy_{table}.pid'.format(table=table_name))

        self.change_config_file_to_source_cluster()
        cmd = r'echo -e "use {table_name}\ncopy_data -c target_cluster -a {table_name} -t {timeout_ms} -b {max_batch_count}" | ' \
              '{skv_tool_run_script} shell --cluster {meta_server_list} ' \
              '1>{output_file} 2>{err_file}'.format(table_name=table_name,
                                                    timeout_ms=self.timeout_ms,
                                                    max_batch_count=self.max_batch_count,
                                                    skv_tool_run_script=self.skv_tool_run_script,
                                                    meta_server_list=self.src_meta_server_list,
                                                    output_file=copy_output_file,
                                                    err_file=copy_err_file)

        with open(copy_execute_file, 'w') as f:
            f.write('echo $$\n')
            f.write(cmd)
            f.write('\n')

        check_call('sh {execute_file} &> {program_id_file} &'.format(
            execute_file=copy_execute_file, program_id_file=program_id_file))
        # 太快会读不到输出的 pid
        time.sleep(1)
        with open(program_id_file, 'r') as f:
            pid = f.read().split()[0]
        return pid

    def generate_in_copy_table_list(self):
        """读取 copy_manage_file.yml 生成正在执行 copy_data 的表的 list"""
        if not os.path.exists(self.copy_manage_file):
            raise Exception('copy data manage file {copy_manage_file} not exist!'.format(
                copy_manage_file=self.copy_manage_file))

        in_copy_table_list = []

        # 读取 copy_manage_file.yml
        with open(self.copy_manage_file, 'r') as f:
            copy_data_detail = yaml.load(f)

        for table_name, detail in copy_data_detail.items():
            # 当 copy_manage_file.yml 中该表的信息为空说明第一次执行 copy_data，直接执行并将 pid 写入 copy_manage_file.yml
            if table_name in self.skip_table_names:
                self.logger.info('skip copy table {table_name}'.format(table_name=table_name))
                continue
            if len(detail) == 0:
                self.logger.info('start to copy data for table {table_name}'.format(table_name=table_name))
                detail['pid'] = self.try_execute_copy_data_return_pid(table_name)
                in_copy_table_list.append(table_name)
            # 不为空，说明之前有执行过，现在从 detail 判断执行信息
            else:
                self.logger.info('table {table_name} has been executed copy.'.format(table_name=table_name))
                state = self.get_copy_table_status(table_name, detail['pid'])
                if state == CopyState.FINISHED:
                    continue
                elif state == CopyState.FAILED or state == CopyState.INTERRUPTED:
                    self.logger.warn('last copy_data on table {table_name} failed or interrupted.'.format(
                        table_name=table_name))
                    copy_output_file = os.path.join(self.manage_root_dir, 'copy_{table}.out'.format(table=table_name))
                    copy_err_file = os.path.join(self.manage_root_dir, 'copy_{table}.err'.format(table=table_name))

                    time_stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

                    shutil.copyfile(copy_err_file, '{name}_{timestamp}'.format(
                        name=copy_err_file, timestamp=time_stamp))
                    shutil.copyfile(copy_output_file, '{name}_{timestamp}'.format(
                        name=copy_output_file, timestamp=time_stamp))

                    self.logger.warn('You can see last copy log in {file_err}, {file_out}'.format(
                        file_err='{name}_{timestamp}'.format(name=copy_err_file, timestamp=time_stamp),
                        file_out='{name}_{timestamp}'.format(name=copy_output_file, timestamp=time_stamp),
                    ))
                    self.logger.warn('try again copy for {table_name}'.format(table_name=table_name))
                    detail['pid'] = self.try_execute_copy_data_return_pid(table_name)
                # 除了 state == finished 的表，其它表都需要重新被监听 copy 进度
                in_copy_table_list.append(table_name)

        # dump copy_data manage 信息
            with open(self.copy_manage_file, 'w') as f:
                yaml.dump(copy_data_detail, f, default_flow_style=False)

        return in_copy_table_list

    def wait_for_copy_data_done(self, in_copy_table_list):
        """轮询等待 copy_data 的结果"""
        if len(in_copy_table_list) == 0:
            self.logger.info('No table need be copyed.')
            return

        self.change_config_file_to_source_cluster()
        total_data_mb = self.src_cluster_api.get_cluster_all_table_file_mb()
        # 这里认为 copy_data 的最慢速度为 0.5MB/s
        max_wait_sec = 2 * (total_data_mb / 1 + 1)
        begin_time = time.time()
        all_copy_table_count = len(in_copy_table_list)

        self.change_config_file_to_destination_cluster()
        while time.time() - begin_time < max_wait_sec:
            # 每次轮询间隔时间为 5s
            self.print_msg_to_screen('copy data for {all_copy_table_count} table, now wait for table {tables}'.format(
                all_copy_table_count=all_copy_table_count,
                tables=str(in_copy_table_list),
            ))
            time.sleep(5)
            finished_copy_table_list = []

            for table_name in in_copy_table_list:
                state = self.get_copy_table_status(table_name)
                if state == CopyState.FINISHED:
                    finished_copy_table_list.append(table_name)
                    self.dest_cluster_api.clear_table_envs(table_name)
                elif state != CopyState.RUNNING:
                    msg = 'Copy_data on table {table_name} failed, check log in {file}'.format(
                        table_name=table_name,
                        file=os.path.join(self.manage_root_dir, 'copy_{table}.err'.format(table=table_name))
                    )
                    raise Exception(msg)
            # 在等待列表删除本轮 copy 结束的 table_mame
            for table_name in finished_copy_table_list:
                in_copy_table_list.remove(table_name)
            if len(in_copy_table_list) == 0:
                self.logger.info('copy_data finished.')
                return

        raise Exception('Timeout for copy_data in {tables}.copy_data program will be continue.'
                        'Please check or try again.'.format(tables=str(in_copy_table_list)))

    def get_copy_table_status(self, table_name, program_pid=None):
        copy_output_file = os.path.join(self.manage_root_dir, 'copy_{table}.out'.format(table=table_name))
        copy_err_file = os.path.join(self.manage_root_dir, 'copy_{table}.err'.format(table=table_name))

        with open(copy_output_file, 'r') as f:
            output_last_line = f.readlines()[-1]
        # stdout 输出 'dsn exit with code' 说明 pegasus shell 程序已结束
        if 'dsn exit with code' in output_last_line:
            self.logger.info('table {table_name} copy program has been stoped.'.format(table_name=table_name))
            with open(copy_err_file, 'r') as f:
                err_last_line = f.readlines()[-1]
            # copy_data finished
            if 'Copy done, total' in err_last_line:
                self.logger.info('table {table_name} has been copyed done.'.format(table_name=table_name))
                return CopyState.FINISHED
            # copy_data 结束，但是未成功
            else:
                self.logger.warn('last copy_data on table {table_name} failed, message: {msg}. '
                                 'Now try again.'.format(table_name=table_name, msg=err_last_line))
                return CopyState.FAILED
        # pegasus_shell 进程未中断
        else:
            self.logger.info('table {table_name} copy program not stoped.'.format(table_name=table_name))
            # copy_data 异常中断
            if (program_pid is not None and program_pid not in os.listdir('/proc')) or \
                    time.time() - os.stat(copy_err_file).st_mtime > 3:
                self.logger.warn('last copy_data in table {table_name} interrupt, unknown reason.'.format(
                    table_name=table_name))
                return CopyState.INTERRUPTED
            else:
                self.logger.info(
                    'table {table_name} copy program is running now.'.format(table_name=table_name))
                return CopyState.RUNNING
