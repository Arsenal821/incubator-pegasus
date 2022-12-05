#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

异步操作封装
skv中有不少异步操作 用这个统一封装等待接口

可以先调用start()然后在外部调用check()
也可以调用run()阻塞返回
"""
import time
import traceback


class AsyncWaitRunner:
    def __init__(self, check_interval_seconds=1, check_timeout_seconds=None, retry_times=1):
        self.check_interval_seconds = check_interval_seconds
        self.check_timeout_seconds = check_timeout_seconds
        self.retry_times = retry_times

    def async_do(self):
        """异步操作"""
        raise Exception('please implement this function')

    def execute_check(self):
        """执行一次检查"""
        raise Exception('please implement this function')

    def check_wait_done(self, result):
        """检查结果 是否可以结束等待 result是execute_check()的结果 返回True/False"""
        raise Exception('please implement this function')

    def print_progress(self, result):
        """打印进度 result是execute_check()的结果"""
        raise Exception('please implement this function')

    def start(self):
        """执行后返回 不等待"""
        # 1. 前置检查
        last_result = self.execute_check()
        # 已完成 则直接打印进度 返回
        if self.check_wait_done(last_result):
            self.print_progress(last_result)
            return

        # 2. 执行命令
        self.async_do()

    def check(self):
        """调用start后检查进度 返回true/false"""
        result = self.execute_check()
        self.print_progress(result)
        return self.check_wait_done(result)

    def run(self):
        """执行并等待"""
        # 1. 前置检查
        last_result = self.execute_check()
        # 已完成 则直接打印进度 返回
        if self.check_wait_done(last_result):
            self.on_finish(last_result)
            return

        # kill_partition等操作需要多次重试..
        for _ in range(self.retry_times):
            # 2. 执行命令
            self.async_do()

            # 3. 等待
            start_time = time.time()
            while self.check_timeout_seconds is None or time.time() - start_time < self.check_timeout_seconds:
                try:
                    # 1. 每轮计算结果
                    result = self.execute_check()
                    # 2. 如果计算结果有变化 则输出
                    if last_result is None or result != last_result:
                        self.print_progress(result)
                        last_result = result
                    # 3. 检查计算结果 是否可以终止循环
                    if self.check_wait_done(result):
                        self.on_finish(result)
                        return
                except Exception:
                    self.logger.warn('caught exception in waiting..')
                    self.logger.warn(traceback.format_exc())
                time.sleep(self.check_interval_seconds)
        else:
            self.on_timeout()

    def on_timeout(self):
        # 默认超时抛异常
        raise Exception('Failed to wait! timeout %d seconds! retry times %d!' % (self.check_timeout_seconds, self.retry_times))

    def on_finish(self, result):
        self.print_progress(result)
