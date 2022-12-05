#!/bin/env python
# -*- coding: UTF-8 -*-

"""
   skv 升级分片写坏工具修复
skv 升级是实例依次重启，可能存在在升级某个replica server时失败，所以需要手动进行修复，列举了以下case:
            CASE                          SOLUTION
   1: primary->new secondary->new      继续升级，全部升级完再补副本
   2: primary->new secondary->old      主从切换
   3: primary->old secondary->random   补副本
"""

import os
import sys
import logging
import datetime
import getopt
import random

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_admin_api import SkvAdminApi


class repairBadPartitionTool:

    def __init__(self, old_version, new_version, module_name):
        self.module_name = module_name
        self.api = SkvAdminApi(logging, self.module_name)
        logging.basicConfig(
            level=logging.WARN,
            format='[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger()
        self.old_version = old_version
        self.new_version = new_version

    def do_repair(self):
        """
        通过解析 app {table_name} -d -j获取所有分片的所在节点的 version 和 status 信息
        用于对 unhealth 的分片进行修复
        """
        # 0. 获取所有 alive 的 replica server版本信息
        alive_replica_server = self.api.get_alive_node_list()
        if not alive_replica_server:
            raise Exception(
                'failed to get replica server list in {cluster}'.format(
                    cluster=self.module_name)
            )
        replica_addr_to_version = {'old': [], 'new': []}
        for addr in alive_replica_server:
            version = self.api.get_server_version(addr).split('(')[0].strip()
            if version == self.old_version:
                replica_addr_to_version['old'].append(addr)
            elif version == self.new_version:
                replica_addr_to_version['new'].append(addr)
            else:
                raise Exception(
                    "{addr} version is {version}, old_version is {old_version},"
                    "new version is {new_version}, please check it!".format(
                        addr=addr,
                        version=version,
                        old_version=self.old_version,
                        new_version=self.new_version,
                    )
                )
        # 1. 获取所有 available 表
        tables = self.api.get_all_avaliable_table_name()
        check_report = ''
        for table in tables:
            # 2. 获取单个表的节点信息
            replica_count = self.api.get_table_replica_count(table)
            # 只考虑三副本表
            if replica_count != 3:
                raise Exception('table [%s] replica count is not 3, please check it!' % table)
            app_id = self.api.get_app_id_by_table(table)
            partition_num = self.api.get_table_partition_count(table)
            for i in range(partition_num):
                gpid = str(app_id) + '.' + str(i)
                primary, secondaries = self.api.get_gpid_to_addr(table, gpid)
                self.logger.warning('begin to check gpid %s:' % gpid)
                # 3. 依次对每个分片的节点进行检查, 并生成检查报告
                check_report += self.generate_repair_code(replica_addr_to_version, table, gpid, primary, secondaries)
                self.logger.warning('check gpid %s done!' % gpid)
        time_stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        check_report_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'skv_replica_repair_report_' + str(time_stamp) + '.txt')
        with open(check_report_file, 'w+', encoding='utf-8') as f:
            f.write(check_report)
        self.logger.warning(
            'all table checked, please follow the repair report(%s) to fix bad gpid in pegasus shell!' % check_report_file)

    def generate_repair_code(self, replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr):
        """对单个分片进行版本检查，并打印相应的处理log
        args:
            replica_addr_to_version: 新老版本对应的replica addr
            table: 表名
            bad_gpid: gpid
            primary_replica_addr: gpid primary 对应的 ip:port
            secondaries_replica_addr gpid secondary 对应的 ip:port 列表
        """
        if not primary_replica_addr:
            raise Exception('gpid %s has no primary, please check!' % bad_gpid)
        # 一主无备
        if not secondaries_replica_addr:
            raise Exception('gpid %s only has a primaty replica[%s], please check!' % (bad_gpid, primary_replica_addr))
        # 一主两备
        if len(secondaries_replica_addr) == 2:
            msg = "# check gpid %s PASS!\n\n" % bad_gpid
            return msg
        # 一主一备
        # replica_server_list 为所有的 alive replica list, 去掉已存在副本的节点，剩余的节点可用来添加副本
        replica_server_list = set(replica_addr_to_version['old']).union(replica_addr_to_version['new'])
        replica_server_list.remove(primary_replica_addr)
        add_sec_addr_list = list(replica_server_list.difference(secondaries_replica_addr))
        # primary is old version, add replica to random addr => 在存活的节点上补分片
        if primary_replica_addr in replica_addr_to_version['old']:
            # 当存在其他 replica 是 alive 时，在其上补副本
            if add_sec_addr_list:
                # 随机选择一个可用节点来添加备份
                add_sec_addr = random.choice(add_sec_addr_list)
                new_secondaries_replica_addr = [secondaries_replica_addr[0], add_sec_addr]
                return """# Check gpid({gpid}) status, should be primary({primary}) secondaries({secondaries})
app {table} -d
# add {addr} to secondary
propose -f -g {gpid} -p ADD_SECONDARY -t {primary} -n {addr}
# After executing the above command, please check gpid({gpid}) status many times until
# primary({primary}) secondaries({new_secondaries})
app {table} -d\n\n""".format(
                    gpid=bad_gpid,
                    table=table,
                    primary=primary_replica_addr,
                    addr=add_sec_addr,
                    secondaries=secondaries_replica_addr,
                    new_secondaries=new_secondaries_replica_addr)
            else:
                raise Exception(
                    'gpid %s only has two replica, primary(%s) is old version, no node available to add secondary!' % (bad_gpid, primary_replica_addr)
                )

        # primary is new version, secondary is new verison => 提示升级完后再补分片
        if secondaries_replica_addr[0] in replica_addr_to_version['new']:
            return "# gpid(%s) both replica are new versions, add the third replica after all upgrades are completed!\n\n" % bad_gpid
        # primary is new version, secondary is old version => 切主,然后补副本
        # 当无可用的副本时
        if not add_sec_addr_list:
            raise Exception(
                'gpid %s only has two replica, secondary(%s) is old version, no node available to add secondary!' % (bad_gpid, secondaries_replica_addr[0]))
        tmp_secondaries = [primary_replica_addr, secondaries_replica_addr[0]]
        msg = """# Check gpid({gpid}) status, should be primary({primary_addr}) seconaries({old_secondaries})
app {table} -d
# downgrade primary({primary_addr}) to secondary
propose -f -g {gpid} -p DOWNGRADE_TO_SECONDARY -t {primary_addr} -n {primary_addr}
# After executing the above command, please check gpid({gpid}) status many times until
# primary(None) seconaries({secondaries_addr})
app {table} -d
app {table} -d
# After gpid({gpid}) status is primary(None) seconaries({secondaries_addr}), upgrade secondary({primary}) to primary
propose -f -g {gpid} -p UPGRADE_TO_PRIMARY -t {primary} -n {primary}
# After executing the above command, please check gpid({gpid}) status many times until
# primary({primary}) seconaries({secondaries})
app {table} -d\n""".format(
            gpid=bad_gpid,
            table=table,
            old_secondaries=secondaries_replica_addr,
            primary=secondaries_replica_addr[0],
            primary_addr=primary_replica_addr,
            secondaries=[primary_replica_addr],
            secondaries_addr=tmp_secondaries,)
        # 补副本, 随机选择一个可用节点来添加备份
        add_sec_addr = random.choice(add_sec_addr_list)
        add_sec_msg = """# there are alive nodes, add {add_sec_addr} to secondary
propose -f -g {gpid} -p ADD_SECONDARY -t {primary} -n {add_sec_addr}
# After executing the above command, please check gpid({gpid}) status many times until
# primary({primary}) secondaries({secondaries})
app {table} -d\n\n""".format(
            add_sec_addr=add_sec_addr,
            gpid=bad_gpid,
            primary=secondaries_replica_addr[0],
            secondaries=[primary_replica_addr, add_sec_addr],
            table=table,)
        return msg + add_sec_msg


def main(argv):
    old_version = ''
    new_version = ''
    module = ''
    try:
        opts, args = getopt.getopt(argv, "o:n:m:", ["old_version=", "new_version=", "skv_module="])
    except getopt.GetoptError:
        print('usage: python3 repair_bad_partition_tool.py -o <old_version> -n <new_version> -m <skv_module>')
        sys.exit(1)
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print('usage: python3 repair_bad_partition_tool.py -o <old_version> -n <new_version> -m <skv_module>')
            sys.exit(1)
        elif opt in ("-o", "--old_version"):
            old_version = arg
        elif opt in ("-n", "--new_version"):
            new_version = arg
        elif opt in ("-m", "-skv_module"):
            module = arg
        else:
            print('usage: python3 repair_bad_partition_tool.py -o <old_version> -n <new_version> -m <skv_module>')
            sys.exit(1)
    if not old_version or not new_version or not module:
        print('usage: python3 repair_bad_partition_tool.py -o <old_version> -n <new_version> -m <skv_module>')
        sys.exit(1)
    tool = repairBadPartitionTool(old_version, new_version, module)
    tool.do_repair()


if __name__ == '__main__':

    main(sys.argv[1:])
