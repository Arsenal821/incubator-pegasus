#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : shell_utils.py
# @Author: liguohao
# @Date  : 2022/8/24
# @Desc  : 搬运原先shell_wrapper, 提供shell调用工具
import sys
import subprocess


# 默认打印到stderr
def default_print_fun(x):
    print(x, file=sys.stderr)


# 不打印
def none_print_fun(x):
    pass


def run_cmd(cmd, print_fun=default_print_fun, timeout=600):
    """执行命令 返回{'ret': <ret>, 'stdout': <stdout>, 'stderr': <stderr}

    Args:
        cmd (str): shell cmd
        print_fun (function): print function
        timeout (int): timeout in seconds

    Returns:
        dict: {'ret': <ret>, 'stdout': <stdout>, 'stderr': <stderr}
    """
    print_fun('running cmd: [%s]' % cmd)
    p = subprocess.Popen(
        cmd,
        shell=True,
        universal_newlines=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE)
    try:
        (stdout, stderr) = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        print_fun('timeout!')
        p.kill()
        (stdout, stderr) = p.communicate()
    ret = p.returncode

    print_fun("======")
    print_fun("cmd:\n%s\n\nret:%d\n\nstdout:\n%s\n\nstderr:\n%s\n\n" % (cmd, ret, stdout, stderr))

    return {'ret': ret, 'stdout': stdout, 'stderr': stderr}


def __assert_ret(ret, cmd, action, run_cmd_result=None):
    """需要打印stderr可传入run_cmd_result

    Args:
        ret (int): 返回码
        cmd (str): 命令字符串
        action (str): 描述该命令行为的说明 会体现在报错信息里 例如 "执行 xxx_action 失败"
        run_cmd_result (dict): 运行shell命令结果 {'ret': ret, 'stdout': stdout, 'stderr': stderr}
    """
    if ret != 0:
        # type str
        stderr_info = None if run_cmd_result is None else run_cmd_result['stderr']
        # 需要打印stderr
        if stderr_info:
            if action:
                raise Exception('failed to %s! ret=%d\nstderr:\n%s' % (action, ret, stderr_info))
            else:
                raise Exception('failed to run[%s]! ret=%d\nstderr:\n%s' % (cmd, ret, stderr_info))
        else:
            if action:
                raise Exception('failed to %s! ret=%d' % (action, ret))
            else:
                raise Exception('failed to run[%s]! ret=%d' % (cmd, ret))


def check_output(cmd, print_fun=default_print_fun, timeout=600, action=None):
    """
    执行命令 返回output 如果ret非0抛异常
    Args:
        cmd (str): shell cmd
        print_fun (function): print function
        timeout (int): timeout in seconds
        action (str): 描述该命令行为的说明 会体现在报错信息里 例如 "执行 xxx_action 失败"

    Returns:
        str: stdout输出
    """
    result = run_cmd(cmd, print_fun, timeout)
    __assert_ret(result['ret'], cmd, action, run_cmd_result=result)
    return result['stdout']


def call(cmd, print_fun=default_print_fun, timeout=600):
    """执行命令 返回返回码
    Args:
        cmd (str): shell cmd
        print_fun (function): print function
        timeout (int): timeout in seconds

    Returns:
        int: shell命令运行完返回的状态码
    """
    return run_cmd(cmd, print_fun, timeout)['ret']


def check_call(cmd, print_fun=default_print_fun, timeout=600, action=None):
    """执行命令，检查是否成功，不成功抛异常. 和check_output一样区别是无返回值
    Args:
        cmd (str): shell cmd
        print_fun (function): print function
        timeout (int): timeout in seconds
        action (str): 描述该操作的字符串 会体现在报错信息里 例如 "执行 xxx_action 失败"
    """

    result = run_cmd(cmd, print_fun, timeout)
    __assert_ret(result['ret'], cmd, action, run_cmd_result=result)
