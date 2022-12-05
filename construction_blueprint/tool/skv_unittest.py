#!/bin/env python
# -*- coding: UTF-8 -*-

import unittest
import re

from repair_bad_partition_tool import repairBadPartitionTool


# skv 副本修复工具进行 ut 测试
class TestSkvGenerateRepairCodeMethods(unittest.TestCase):

    maxDiff = None

    # 一主无备, 抛出异常
    def test_p1_s0(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171'], 'new': ['10.129.132.240:8171']}
        primary_replica_addr = '10.129.132.239:8171'
        secondaries_replica_addr = []
        with self.assertRaises(Exception):
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)

    # 一主一备, primary 为 old version, 并且无其他 alive 节点
    def test_p1_s1_primary_is_old_no_other_alive_node(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171'], 'new': ['10.129.132.240:8171']}
        primary_replica_addr = '10.129.132.239:8171'
        secondaries_replica_addr = ['10.129.132.240:8171']
        with self.assertRaises(Exception):
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)

    # 一主一备, primary 为 old version, 当存在一个其他 alive 的 replica 时, 选择其补副本
    def test_p1_s1_primary_is_old_has_other_alive_node_1(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171', '10.129.132.241:8171'], 'new': ['10.129.132.240:8171']}
        primary_replica_addr = '10.129.132.239:8171'
        secondaries_replica_addr = ['10.129.132.240:8171']
        right_msg = '''# Check gpid(24.0) status, should be primary(10.129.132.239:8171) secondaries(['10.129.132.240:8171'])
app xxx -d
# add 10.129.132.241:8171 to secondary
propose -f -g 24.0 -p ADD_SECONDARY -t 10.129.132.239:8171 -n 10.129.132.241:8171
# After executing the above command, please check gpid(24.0) status many times until
# primary(10.129.132.239:8171) secondaries(['10.129.132.240:8171', '10.129.132.241:8171'])
app xxx -d\n\n'''
        self.assertEqual(
            right_msg,
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)
        )

    # 一主一备, primary 为 old version, 当存在多个的其他 alive 的 replica 时, 随机选择一个补副本
    def test_p1_s1_primary_is_old_has_other_alive_node_2(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171', '10.129.132.241:8171', '10.129.132.242:8171'], 'new': ['10.129.132.240:8171', '10.129.132.243:8171']}
        primary_replica_addr = '10.129.132.239:8171'
        secondaries_replica_addr = ['10.129.132.240:8171']
        add_sec_addr_list = ['10.129.132.241:8171', '10.129.132.242:8171', '10.129.132.243:8171']
        generate_msg = tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)
        addr_pat = re.compile(r'# add (\S*) to secondary', re.DOTALL)
        add_sec_addr = addr_pat.findall(generate_msg)[0]
        self.assertTrue((add_sec_addr in add_sec_addr_list))
        right_msg = '''# Check gpid(24.0) status, should be primary(10.129.132.239:8171) secondaries(['10.129.132.240:8171'])
app xxx -d
# add {addr} to secondary
propose -f -g 24.0 -p ADD_SECONDARY -t 10.129.132.239:8171 -n {addr}
# After executing the above command, please check gpid(24.0) status many times until
# primary(10.129.132.239:8171) secondaries(['10.129.132.240:8171', '{addr}'])
app xxx -d\n\n'''.format(addr=add_sec_addr)
        self.assertEqual(right_msg, generate_msg)

    # 一主一备, primary 为 new version, seconaries 中有老版本的 replica, 无其他存活节点
    def test_p1_s1_primary_is_new_secondaries_has_old_no_other_alive_node(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171'], 'new': ['10.129.132.240:8171']}
        primary_replica_addr = '10.129.132.240:8171'
        secondaries_replica_addr = ['10.129.132.239:8171']
        with self.assertRaises(Exception):
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)

    # 一主一备, primary 为 new version, seconaries 中有老版本的 replica， 有其他存活节点
    def test_p1_s1_primary_is_new_secondaries_has_old_has_other_alive_node(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171', '10.129.132.241:8171'], 'new': ['10.129.132.240:8171', '10.129.132.242:8171', '10.129.132.243:8171']}
        primary_replica_addr = '10.129.132.240:8171'
        secondaries_replica_addr = ['10.129.132.239:8171']
        add_sec_addr_list = ['10.129.132.241:8171', '10.129.132.242:8171', '10.129.132.243:8171']
        generate_msg = tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)
        addr_pat = re.compile(r'add (\S*) to secondary', re.DOTALL)
        add_sec_addr = addr_pat.findall(generate_msg)[0]
        self.assertTrue((add_sec_addr in add_sec_addr_list))
        right_msg = """# Check gpid(24.0) status, should be primary(10.129.132.240:8171) seconaries(['10.129.132.239:8171'])
app xxx -d
# downgrade primary(10.129.132.240:8171) to secondary
propose -f -g 24.0 -p DOWNGRADE_TO_SECONDARY -t 10.129.132.240:8171 -n 10.129.132.240:8171
# After executing the above command, please check gpid(24.0) status many times until
# primary(None) seconaries(['10.129.132.240:8171', '10.129.132.239:8171'])
app xxx -d
app xxx -d
# After gpid(24.0) status is primary(None) seconaries(['10.129.132.240:8171', '10.129.132.239:8171']), upgrade secondary(10.129.132.239:8171) to primary
propose -f -g 24.0 -p UPGRADE_TO_PRIMARY -t 10.129.132.239:8171 -n 10.129.132.239:8171
# After executing the above command, please check gpid(24.0) status many times until
# primary(10.129.132.239:8171) seconaries(['10.129.132.240:8171'])
app xxx -d
# there are alive nodes, add {addr} to secondary
propose -f -g 24.0 -p ADD_SECONDARY -t 10.129.132.239:8171 -n {addr}
# After executing the above command, please check gpid(24.0) status many times until
# primary(10.129.132.239:8171) secondaries(['10.129.132.240:8171', '{addr}'])
app xxx -d\n\n""".format(addr=add_sec_addr)
        self.assertEqual(right_msg, generate_msg)

    # 一主一备, primary 为 new version, seconaries 中没有老版本的 replica
    def test_p1_s1_primary_is_new_secondaries_not_has_old(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': [], 'new': ['10.129.132.239:8171', '10.129.132.240:8171']}
        primary_replica_addr = '10.129.132.240:8171'
        secondaries_replica_addr = ['10.129.132.239:8171']
        right_msg = '# gpid(24.0) both replica are new versions, add the third replica after all upgrades are completed!\n\n'
        self.assertEqual(
            right_msg,
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)
        )

    # 一主两备
    def test_p1_s2(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171'], 'new': ['10.129.132.240:8171', '10.129.132.241:8171']}
        primary_replica_addr = '10.129.132.239:8171'
        secondaries_replica_addr = ['10.129.132.239:8171', '10.129.132.240:8171']
        right_msg = '# check gpid 24.0 PASS!\n\n'
        self.assertEqual(
            right_msg,
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)
        )

    # 无主, 抛异常
    def test_p0(self):
        tool = repairBadPartitionTool('1.12.3-0.5.0', '1.12.3-0.6.0', 'skv_offline')
        table = 'xxx'
        bad_gpid = '24.0'
        replica_addr_to_version = {'old': ['10.129.132.239:8171'], 'new': ['10.129.132.240:8171']}
        primary_replica_addr = ''
        secondaries_replica_addr = ['10.129.132.239:8171', '10.129.132.240:8171']
        with self.assertRaises(Exception):
            tool.generate_repair_code(replica_addr_to_version, table, bad_gpid, primary_replica_addr, secondaries_replica_addr)


if __name__ == '__main__':
    unittest.main()
