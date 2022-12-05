#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

配置管理工具
"""
import configparser
import json
import os
import sys
from prettytable import PrettyTable

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools', 'tools'))
from base_tool import BaseTool
from skv_common import SKV_MODULE_NAME_LIST, SKV_ROLE_NAME_LIST, is_skv_in_mothership
from recipes import get_skv_config_manager


class ConfigTool(BaseTool):
    example_doc = '''
skvadmin config show -m skv_offline -r replica_server # show replica configs
skvadmin config group show -m skv_offline # show skv offline group info
'''

    def init_parser(self, subparser):
        # 以下定义一些通用的参数
        MODULE_ARG = ('module', {'choices': SKV_MODULE_NAME_LIST, 'help': 'module name, %s' % SKV_MODULE_NAME_LIST})
        ROLE_ARG = ('role', {'choices': SKV_ROLE_NAME_LIST, 'help': 'role name, %s' % SKV_ROLE_NAME_LIST})
        GROUP_ARG = ('group', {'help': 'group name'})
        SECTION_ARG = ('section', {'help': 'section name'})
        NAME_ARG = ('name', {'help': 'config name'})
        YES_ARG = ('yes', {'help': 'if set, skip input yes', 'action': 'store_true', 'default': False})

        def add_argument_to_parser(parser, requires, optionals):
            for name, argument_args in requires:
                parser.add_argument('-%s' % name[0], '--%s' % name, required=True, **argument_args)
            for name, argument_args in optionals:
                parser.add_argument('-%s' % name[0], '--%s' % name, required=False, **argument_args)

        subparsers = subparser.add_subparsers(dest='config_type')

        # 查看配置
        show_subparser = subparsers.add_parser(name='show', help='show config in human readable format')
        show_subparser.required = True
        add_argument_to_parser(show_subparser, [MODULE_ARG], [ROLE_ARG, GROUP_ARG, YES_ARG])

        # 输出配置
        get_subparser = subparsers.add_parser(name='get', help='get skv config')
        get_subparser.required = True
        add_argument_to_parser(get_subparser, [MODULE_ARG, ROLE_ARG], [GROUP_ARG, SECTION_ARG, NAME_ARG])
        # 输出可选 默认json 也可以-P输出stdout 也可以-f输出文件
        get_subparser.add_argument('-P', '--print_to_stdout', help='if set, print to stdout', action='store_true', default=False)
        get_subparser.add_argument('-f', '--filename', help='if set, print to file')

        # 修改配置
        set_subparser = subparsers.add_parser(name='set', help='set skv config')
        set_subparser.required = True
        add_argument_to_parser(set_subparser, [MODULE_ARG, ROLE_ARG], [GROUP_ARG, SECTION_ARG, NAME_ARG, YES_ARG])
        # 输入配置 二选一
        value_group = set_subparser.add_mutually_exclusive_group(required=True)
        value_group.add_argument('-f', '--filename', help="if set, read value from file")
        value_group.add_argument('-v', '--value', help="set value")

        # 删除配置
        delete_subparser = subparsers.add_parser(name='delete', help='delete skv config')
        delete_subparser.required = True
        add_argument_to_parser(delete_subparser, [MODULE_ARG, ROLE_ARG, SECTION_ARG], [GROUP_ARG, NAME_ARG, YES_ARG])

        # 配置组
        group_subparser = subparsers.add_parser(name='group', help='config group relative operations')
        group_subparser.required = True
        group_subparsers = group_subparser.add_subparsers(dest='group_type')

        # 查看配置组
        show_subparser = group_subparsers.add_parser(name='show', help='show config group in human readable format')
        show_subparser.required = True
        add_argument_to_parser(show_subparser, [MODULE_ARG], [ROLE_ARG, GROUP_ARG])

        # 配置组修改
        alter_subparser = group_subparsers.add_parser(name='alter', help='alter host config group')
        alter_subparser.required = True
        add_argument_to_parser(alter_subparser, [MODULE_ARG, ROLE_ARG, GROUP_ARG], [YES_ARG])
        alter_subparser.add_argument('--host', '-H', required=True, help='host fqdn to be altered')

        # 配置组新增
        add_subparser = group_subparsers.add_parser(name='add', help='add new config group')
        add_subparser.required = True
        add_argument_to_parser(add_subparser, [MODULE_ARG, ROLE_ARG], [YES_ARG])
        add_subparser.add_argument('--host_list', '-l', help='comma seperated fqdn list, if set, will be transfer to this group', default=None)
        add_subparser.add_argument('--config_copy_group', help='if set, copy config from this group')

        # 配置组删除
        delete_subparser = group_subparsers.add_parser(name='delete', help='delete new config group')
        delete_subparser.required = True
        add_argument_to_parser(delete_subparser, [MODULE_ARG, ROLE_ARG, GROUP_ARG], [YES_ARG])
        delete_subparser.add_argument('--transfer_host_group', help='if set, transfer current hosts to this new group')

        self.parser = subparser

    def do(self, args):
        """展示所有skv的操作记录 重大操作会使用stepworker记录上下文 实用history命令可以看到记录"""
        if args.config_type == 'show':
            return self._show_config(args.module, args.role, args.group, args.yes)
        elif args.config_type == 'get':
            return self._get_config(args.module, args.role, args.group, args.section, args.name, args.print_to_stdout, args.filename)
        elif args.config_type == 'set':
            return self._set_config(args.module, args.role, args.group, args.section, args.name, args.yes, args.filename, args.value)
        elif args.config_type == 'delete':
            return self._delete_config(args.module, args.role, args.group, args.section, args.name, args.yes)
        elif args.config_type == 'group':
            if args.group_type == 'show':
                return self._show_config_group(args.module, args.role, args.group)
            elif args.group_type == 'alter':
                return self._alter_config_group(args.module, args.role, args.group, args.host, args.yes)
            elif args.group_type == 'add':
                return self._add_config_group(args.module, args.role, args.config_copy_group, args.host_list, args.yes)
            elif args.group_type == 'delete':
                return self._delete_config_group(args.module, args.role, args.group, args.transfer_host_group, args.yes)
            else:
                self.parser.print_help()
        else:
            self.parser.print_help()

    def _show_config(self, module, role, group, yes):
        """打印当前配置"""
        self.logger.debug('show_config: module[%s] role[%s] group[%s] yes[%s]' % (module, role, group, yes))
        roles = [role] if role else SKV_ROLE_NAME_LIST
        for r in roles:
            skv_config_manager = get_skv_config_manager(module, r, self.logger, verbose=False)
            if not group:
                # 打印角色的配置
                self.logger.info('=' * 80)
                self.logger.info('%s configs:\n%s' % (r, self._config_to_ini_str(skv_config_manager.get_config_section_to_kv())))

            groups = [group] if group else skv_config_manager.get_config_groups()
            for g in groups:
                hosts = skv_config_manager.get_config_group_hosts(g)
                self.logger.info('=' * 80)
                self.logger.info('host group %s has %d hosts:%s' % (g, len(hosts), hosts))
                self.logger.info('configs:\n%s' % self._config_to_ini_str(skv_config_manager.get_config_section_to_kv(g)))

    def _get_config(self, module, role, group, section, name, print_to_stdout, filename):
        """输出特定配置"""
        skv_config_manager = get_skv_config_manager(module, role, self.logger, verbose=not print_to_stdout)
        if not section:
            conf = skv_config_manager.get_config_section_to_kv(group)
            conf = json.dumps(conf, indent=4, sort_keys=True)
        elif not name:
            conf = skv_config_manager._get_config_kv(section, group)
            conf = json.dumps(conf, indent=4, sort_keys=True)
        else:
            conf = skv_config_manager.get_config_value(section, name, group)
        if print_to_stdout or is_skv_in_mothership(module):
            print(conf)
        if filename:
            with open(filename, 'w+') as f:
                f.write(conf)
            self.logger.info('dumped config to %s' % filename)

    def _set_config(self, module, role, group, section, name, yes, filename, value):
        """修改配置"""
        # 读取新的值
        if filename:
            with open(filename) as f:
                new_value = f.read()
        else:
            new_value = value
        skv_config_manager = get_skv_config_manager(module, role, self.logger)
        if not section:
            skv_config_manager._set_config_section_to_kv(json.loads(new_value), group, yes)
        elif not name:
            skv_config_manager._set_config_kv(json.loads(new_value), section, group, yes)
        else:
            skv_config_manager.set_config_value(section, name, new_value, group, yes)

    def _delete_config(self, module, role, group, section, name, yes):
        """删除配置"""
        skv_config_manager = get_skv_config_manager(module, role, self.logger)
        if not name:
            skv_config_manager._delete_config_section(section, group, yes)
        else:
            skv_config_manager._delete_config(section, name, group, False, yes)

    def _show_config_group(self, module, role, group):
        """配置组查看"""
        roles = [role] if role else SKV_ROLE_NAME_LIST
        for r in roles:
            skv_config_manager = get_skv_config_manager(module, r, self.logger)
            table = PrettyTable(['group(num hosts)', 'hosts'])
            groups = skv_config_manager.get_config_groups()
            show_group = group.strip().split(',') if group else groups
            print_group = []
            for g in show_group:
                g = g.strip()
                if not g or g not in groups:
                    self.logger.error("group[%s] not in %s group %s" % (g, r, groups))
                    continue
                print_group.append(g)
                hosts = skv_config_manager.get_config_group_hosts(g)
                table.add_row(['%s(%d)' % (g, len(hosts)), '\n'.join(hosts)])
            self.logger.info('show %s %s config group %s:\n%s' % (module, r, print_group, table.get_string()))

    def _alter_config_group(self, module, role, group, host, yes):
        """配置组修改"""
        skv_config_manager = get_skv_config_manager(module, role, self.logger)
        skv_config_manager._alter_config_group(group, host, yes)

    def _add_config_group(self, module, role, config_copy_group, host_list, yes):
        """增加配置组"""
        skv_config_manager = get_skv_config_manager(module, role, self.logger)
        hosts = host_list.split(',') if host_list else []
        skv_config_manager._add_config_group(config_copy_group, hosts, yes)

    def _delete_config_group(self, module, role, group, transfer_host_group, yes):
        """删除配置组"""
        skv_config_manager = get_skv_config_manager(module, role, self.logger)
        skv_config_manager._delete_config_group(group, transfer_host_group, yes)

    def _config_to_ini_str(self, config):
        """把dict转换城ini string"""
        strbuf = []

        class FakeFd:
            def write(self, x):
                strbuf.append(x)
        parser = configparser.ConfigParser()
        parser.read_dict(config)
        parser.write(FakeFd())
        return ''.join(strbuf)

    def _confirm_yes(self):
        """交互式输入确认"""
        self.logger.info('please enter [yes] to confirm')
        resp = input()
        if resp != 'yes':
            self.logger.error('invalid response: %s' % resp)
            raise Exception('failed to set skv config!')
