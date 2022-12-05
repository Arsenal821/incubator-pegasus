#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

生成脚本按照分片scan


用法：
1. 先运行 python3 ${SKV_HOME}/admintools/skv_tools/scan_by_partition.py 生成样例yml
2. 填写yml(count_data是用来按照分片count的，copy_local是按照分片将当前集群的a表迁移到b表里面，copy_remote是将当前集群的a表迁移到远端集群的a表)
3. 运行 python3 ${SKV_HOME}/admintools/skv_tools/scan_by_partition.py xx.yml 生成脚本
4. cd到临时目录
5. 分批执行run_1.sh，check_1.sh检查进度，check_all.sh计算总数
"""
import datetime
import math
import os
import sys
import yaml


def help():
    with open('/tmp/count_data.yml', 'w+') as f:
        f.write('''
action: count
meta_server_addrs: 10.1.1.1:8170,10.1.1.2:8170,10.1.1.3:8170
table_name: sdf_id_mapping
batch_size: 30
# set partition_count or partition_id_list
partition_count: 128
# partition_id_list: 1,2,14,127
''')
    with open('/tmp/copy_data_local.yml', 'w+') as f:
        f.write('''
action: copy_local
meta_server_addrs: 10.1.1.1:8170,10.1.1.2:8170,10.1.1.3:8170
src_table: sdf_id_mapping
dst_table: sdf_id_mapping_2
cluster_name: skv_offline
batch_size: 30
# set partition_count or partition_id_list
partition_count: 128
# partition_id_list: 1,2,14,127
''')
    with open('/tmp/copy_data_remote.yml', 'w+') as f:
        f.write('''
action: copy_remote
local_meta_server_addrs: 10.1.1.1:8170,10.1.1.2:8170,10.1.1.3:8170
remote_cluster_name: target_cluster
table_name: sdf_id_mapping
batch_size: 30
# set partition_count or partition_id_list
partition_count: 128
# partition_id_list: 1,2,14,127
''')
    print('yaml example in /tmp/count_data.yml, /tmp/copy_data_local.yml, /tmp/copy_data_remote.yml')
    print('usage: %s <ymal>' % sys.argv[0])


def do(config_file):
    with open(config_file) as f:
        d = yaml.safe_load(f)
    if 'action' not in d or d['action'] not in ['count', 'copy_local', 'copy_remote']:
        help()
        raise Exception('invalid config %s!' % config_file)
    tmp_dir = os.path.join('/tmp', datetime.datetime.now().strftime('scan_by_partition_%Y%m%d_%H%M%S'))
    os.makedirs(tmp_dir)

    if 'partition_count' in d:
        partition_ids = [x for x in range(d['partition_count'])]
    elif 'partition_id_list' in d:
        partition_ids = d['partition_id_list'].split(',')
    else:
        raise Exception('cannont find partition info!')

    if d['action'] == 'count':
        cmd = 'count_data -t 120000'
        genereate_script(cmd, d['meta_server_addrs'], d['table_name'], tmp_dir, partition_ids, d['batch_size'])
    elif d['action'] == 'copy_local':
        cmd = 'copy_data -c %s -a %s -t 120000' % (d['cluster_name'], d['dst_table'])
        genereate_script(cmd, d['meta_server_addrs'], d['src_table'], tmp_dir, partition_ids, d['batch_size'])
    else:
        cmd = 'copy_data -c %s -a %s -t 120000' % (d['remote_cluster_name'], d['table_name'])
        genereate_script(cmd, d['meta_server_addrs'], d['table_name'], tmp_dir, partition_ids, d['batch_size'])


def genereate_script(cmd, meta_server_addrs, table, dst_dir, all_partition_ids, batch_size):
    # 按照batch_size分组
    batches = []
    for i in range(math.ceil(len(all_partition_ids) / batch_size)):
        start_index = i * batch_size
        end_index = min(len(all_partition_ids), (i + 1) * batch_size)
        batches.append(all_partition_ids[start_index: end_index])

    # 生成每个batch的脚本
    for i, partition_ids in enumerate(batches):
        root_dir = os.path.join(dst_dir, 'batch_%d' % (i + 1))
        os.makedirs(root_dir, exist_ok=True)
        # 启动任务的脚本
        with open(os.path.join(dst_dir, 'run_%d.sh' % (i + 1)), 'w+') as f:
            f.write('''#!/bin/sh
set -x -e
cd ${SKV_HOME}/skv_offline/tools
''')
            for partition_id in partition_ids:
                f.write("echo -e 'use {table}\\n{cmd} -p {partition_id}' | sh ./run.sh shell --cluster {meta_server_addrs} > {log_dir}/{partition_id}.out 2>{log_dir}/{partition_id}.err &\n".format(
                    table=table, cmd=cmd, meta_server_addrs=meta_server_addrs, log_dir=root_dir, partition_id=partition_id))

        # 检查进度的脚本
        with open(os.path.join(dst_dir, 'check_%d.sh' % (i + 1)), 'w+') as f:
            f.write('tail -n 1 %s/*.err\n' % root_dir)

    # 整体检查脚本
    with open(os.path.join(dst_dir, 'check_all.sh'), 'w+') as f:
        f.write('#!/bin/sh\n')
        for i, partition_ids in enumerate(batches):
            batch_name = 'batch_%d' % (i + 1)
            f.write('''
cd %s
running_count=`ls *.err | wc -l`
finished_count=`tail -n 1 *.err | grep "done, total"  | wc -l`
echo "%s ${finished_count}/${running_count} finished"

''' % (os.path.join(dst_dir, batch_name), batch_name))
        f.write('''
cd %s
total_cnt=`tail */*.err | grep 'done, total' | awk '{sum+=$4}END{print sum}'`
echo "total ${total_cnt} lines done" ''' % dst_dir)
    print('please cd to %s' % dst_dir)
    print('execute run_1.sh to start the first batch, then run check_1.sh to check progress')
    print('after all first batch done, execute run_2.sh to start the second batch.')
    print('you can also run check_all.sh to check total progress')


if __name__ == '__main__':
    if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
        help()
        sys.exit(1)
    do(sys.argv[1])
