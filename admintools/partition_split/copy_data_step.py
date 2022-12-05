#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

从老表拷贝到新表

这部分代码选择性拷贝了sp-admintools/skv_tools/migrate/steps/skv_copy_data_step.py的代码
主要考虑是 后续热扩分片将弃用这部分逻辑
可能这个代码的生存期限只有一年
那就没有必要再抽象一层 不然还要各种回归
"""
import datetime
import os
import re
import shutil
import sys
import time
import yaml

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from partition_split.base_partition_split_step import BasePartitionSplitStep
from hyperion_utils.shell_utils import check_call


# operation timeout是200s
COPY_DATA_TIMEOUT = 200 * 1000
# 超过3天stderr没有更新则认为是失败
COPY_DATA_WAIT_STDERR_DAYS = 3
# 输出正则匹配
COPY_DATA_LINE_PATTERN = re.compile(r'.*processed for ([0-9]+) seconds.*total ([0-9]+) rows.*')


class CopyState():
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    FINISHED = "FINISHED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


class CopyDataStep(BasePartitionSplitStep):
    def do_update(self):
        # 获取总数
        with open(self.static_yml_file) as f:
            static_data = yaml.safe_load(f)
        self.table_count_num = static_data['table_count_num']

        self.new_table_name = '%s_%s' % (self.table_name, self.time_str)

        self.copy_output_file = os.path.join(self.get_stepworker_work_dir(), 'copy_%s.out' % self.table_name)
        self.copy_err_file = os.path.join(self.get_stepworker_work_dir(), 'copy_%s.err' % self.table_name)
        self.program_id_file = os.path.join(self.get_stepworker_work_dir(), 'copy_%s.pid' % self.table_name)

        # 启动任务
        status = self._get_table_status()
        if status == CopyState.FAILED:
            self._clean_up_failed_job()
            self._try_execute_copy_data()
        elif status == CopyState.INTERRUPTED:
            raise Exception('please make sure process status! pid file %s' % self.program_id_file)
        elif status == CopyState.NOT_STARTED:
            self._try_execute_copy_data()

        # 不断检查是否结束
        while True:
            time.sleep(1)

            status = self._get_table_status()
            if status == CopyState.FINISHED:
                self.logger.info('copy table done!')
                self.api.clear_table_envs(self.new_table_name)
                return
            elif status != CopyState.RUNNING:
                raise Exception('failed to copy table! see %s, %s for details' % (self.copy_output_file, self.copy_err_file))

    def _try_execute_copy_data(self):
        """执行 copy_data"""
        # bulk_load 模式通常用于灌数据，但是在灌数据过程中因为消耗大量的CPU和IO，对读性能会产生较大影响造成读延迟陡增、超时率升高等
        self.api.set_table_env(self.new_table_name, 'rocksdb.usage_scenario', 'bulk_load')

        # 写入启动脚本
        copy_execute_file = os.path.join(self.get_stepworker_work_dir(), 'copy_{table}.sh'.format(table=self.table_name))
        multi_set = '-u -m 5 -b 100' if self.use_multi_set else ''
        with open(copy_execute_file, 'w') as f:
            f.write('''set -x -e
date
echo $$ > {pid_file} # 写入pid
cmd="use {table_name}\ncopy_data -c {cluster_name} -a {new_table_name} -t {timeout_ms} {multi_set}"
echo -e "$cmd" | {skv_tool_run_script} shell --cluster {meta_server_endpoint}
'''.format(
                cluster_name=self.module_name,
                table_name=self.table_name,
                new_table_name=self.new_table_name,
                timeout_ms=200000,
                multi_set=multi_set,
                skv_tool_run_script=self.api.skv_tool_run_script,
                meta_server_endpoint=self.api.meta_server_endpoint,
                pid_file=self.program_id_file))

        check_call('nohup sh {execute_file} >{output_file} 2>{err_file} </dev/null &'.format(
            execute_file=copy_execute_file, output_file=self.copy_output_file, err_file=self.copy_err_file))

    def _get_table_status(self):
        """返回拷贝表的状态"""
        if not os.path.isfile(self.copy_output_file):
            return CopyState.NOT_STARTED

        with open(self.copy_output_file, 'r') as f:
            output_last_line = f.readlines()[-1].strip()
        with open(self.copy_err_file, 'r') as f:
            err_last_line = f.readlines()[-1].strip()
        self.logger.debug('last line: %s' % output_last_line)
        # stdout 输出 'dsn exit with code' 说明 pegasus shell 程序已结束
        if 'dsn exit with code' in output_last_line:
            self.logger.info('table copy program has finished.')
            # copy_data finished
            if 'Copy done, total' in err_last_line:
                self.logger.info('table copyed done.')
                return CopyState.FINISHED
            # copy_data 结束，但是未成功
            else:
                self.logger.warn('last copy_data on table failed!')
                return CopyState.FAILED
        # pegasus_shell 进程未中断
        else:
            with open(self.program_id_file, 'r') as f:
                program_pid = int(f.read())
            self.logger.info('table copy program is not finished, program pid %s' % program_pid)
            # copy_data 异常中断
            if program_pid is not None and not os.path.exists('/proc/%d' % program_pid):
                self.logger.warn('/proc/%d not exists' % program_pid)
            elif time.time() - os.stat(self.copy_err_file).st_mtime > COPY_DATA_WAIT_STDERR_DAYS:
                self.logger.warn('%s has not updated for %d days.' % (self.copy_err_file, COPY_DATA_WAIT_STDERR_DAYS))
                return CopyState.INTERRUPTED
            else:
                self.logger.info('table copy program is running now.')
                g = COPY_DATA_LINE_PATTERN.match(err_last_line)
                if g:
                    running_seconds = int(g.group(1))
                    done_lines = int(g.group(2))
                    # 计算剩余时间
                    avg_speed = done_lines / running_seconds
                    if 0 == avg_speed:
                        remaining_seconds = 'unknown'
                    else:
                        remaining_seconds = int((self.table_count_num - done_lines) / avg_speed)
                    self.print_msg_to_screen('%s/%s done, avg %.2f/s, remaining %s seconds' % (done_lines, self.table_count_num, avg_speed, remaining_seconds))
                else:
                    self.print_msg_to_screen(err_last_line)
                return CopyState.RUNNING

    def _clean_up_failed_job(self):
        """备份日志等清理工作"""
        self.logger.warn('last copy_data on table {table_name} failed or interrupted.'.format(table_name=self.table_name))

        time_stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        for f in [self.copy_err_file, self.copy_output_file, self.program_id_file]:
            shutil.copyfile(f, '{name}_{timestamp}'.format(name=f, timestamp=time_stamp))

    def do_rollback(self):
        pass
