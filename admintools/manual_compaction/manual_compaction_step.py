#!/bin/env python
# -*- coding: UTF-8 -*-

import datetime
import os
import shutil
import sys
import time

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from manual_compaction.base_manual_compaction_step import BaseManualCompactionStep

from hyperion_utils.shell_utils import check_call

# 超过10s copaction_output_file 没有更新则认为是失败
MANUAL_COMPACTION_WAIT_SEC = 10


class CompactionState():
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    FINISHED = "FINISHED"


class ManualCompactionStep(BaseManualCompactionStep):
    def do_update(self):
        self.compaction_output_file = os.path.join(self.get_stepworker_work_dir(), 'compaction_%s.out' % self.table_name)
        self.compaction_err_file = os.path.join(self.get_stepworker_work_dir(), 'compaction_%s.err' % self.table_name)

        # 启动任务
        status = self._get_table_status()
        if status == CompactionState.FAILED:
            self._clean_up_failed_job()
        elif status == CompactionState.NOT_STARTED:
            self._try_execute_manual_compaction_data()

        # 不断检查是否结束
        while True:
            time.sleep(1)
            status = self._get_table_status()
            if status == CompactionState.FINISHED:
                self.logger.info('manual compaction done!')
                self._clear_table_manual_compaction_envs()
                return
            elif status != CompactionState.RUNNING:
                raise Exception('failed to manual compaction! see %s, %s for details' % (self.compaction_output_file, self.compaction_err_file))
            with open(self.compaction_output_file, 'r') as f:
                output_last_line = f.readlines()[-1].strip()
            self.print_msg_to_screen(output_last_line)

    def _try_execute_manual_compaction_data(self):
        """执行 manual compaction"""
        # 写入启动脚本
        compaction_execute_file = os.path.join(self.get_stepworker_work_dir(), 'manual_compaction_{table}.sh'.format(table=self.table_name))
        script_cmd = os.path.join(os.environ['SKV_HOME'], self.module_name, 'tools/scripts/pegasus_manual_compact.sh')
        with open(compaction_execute_file, 'w') as f:
            f.write('''set -x -e
date
sh {script_cmd} -c {meta_server_list} -a {table_name} --bottommost_level_compaction force
'''.format(
                script_cmd=script_cmd,
                meta_server_list=self.meta_server_list,
                table_name=self.table_name))

        check_call('nohup sh {execute_file} >{output_file} 2>{err_file} </dev/null &'.format(
            execute_file=compaction_execute_file, output_file=self.compaction_output_file, err_file=self.compaction_err_file))

    def _get_table_status(self):
        """返回manual compaction的状态"""
        if not os.path.isfile(self.compaction_output_file):
            return CompactionState.NOT_STARTED
        with open(self.compaction_output_file, 'r') as f:
            output_last_line = f.readlines()[-1].strip()
        self.logger.debug('last line: %s' % output_last_line)
        # stdout 输出 'Manual compact done' 说明manual compaction程序已执行完成
        if 'Manual compact done' in output_last_line:
            self.logger.info('table %s manual compaction has finished.' % self.table_name)
            return CompactionState.FINISHED
        else:
            # manual compaction 异常中断
            with open(self.compaction_output_file, 'r') as f:
                program_pid = int(f.readlines()[2].split()[1])
            if program_pid is not None and not os.path.exists('/proc/%d' % program_pid):
                self.logger.warn('/proc/%d not exists' % program_pid)
                return CompactionState.FAILED
            elif time.time() - os.stat(self.compaction_output_file).st_mtime > MANUAL_COMPACTION_WAIT_SEC:
                self.logger.warn('%s has not updated for %ds.' % (self.compaction_err_file, MANUAL_COMPACTION_WAIT_SEC))
                return CompactionState.FAILED
            else:
                self.logger.info('table [%s] manual compaction is running now.' % self.table_name)
                return CompactionState.RUNNING

    def _clean_up_failed_job(self):
        """备份日志等清理工作"""
        time_stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        for f in [self.compaction_err_file, self.compaction_output_file]:
            shutil.copyfile(f, '{name}_{timestamp}'.format(name=f, timestamp=time_stamp))
        self._clear_table_manual_compaction_envs()
        raise Exception('last manual compaction on table {table_name} failed.'.format(table_name=self.table_name))

    def _clear_table_manual_compaction_envs(self):
        keys = ['manual_compact.once.bottommost_level_compaction', 'manual_compact.once.target_level', 'manual_compact.once.trigger_time']
        for key in keys:
            self.api.del_table_envs(self.table_name, key)

    def rollback(self):
        pass
