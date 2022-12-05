#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief

检查集群副本数是否都为3
"""
import os
import sys

sys.path.append(os.path.join(os.environ['SKV_HOME'], 'admintools'))
from skv_maintenance_workers.base_worker import BaseWorker
from skv_admin_api import SkvAdminApi
from skv_common import SKV_REPLICA_SERVER_ROLE_NAME
from recipes import get_skv_config_manager


CLUSTER_EXPECT_REPLICA_COUNT = 3


class CheckTableReplicaCountWorker(BaseWorker):
    def has_bad_table(self, verbose):
        skv_config_manager = get_skv_config_manager(self.module, SKV_REPLICA_SERVER_ROLE_NAME, self.logger)
        replica_server_count = len(skv_config_manager.get_host_list())
        expect_replica_count = min(replica_server_count, CLUSTER_EXPECT_REPLICA_COUNT)
        if verbose:
            self.logger.info('this cluster has %d replica servers, expect replica count is %s' % (replica_server_count, expect_replica_count))
        api = SkvAdminApi(self.logger, self.module)
        for table in api.get_all_avaliable_table_name():
            replica_count = api.get_table_replica_count(table)
            if replica_count != expect_replica_count:
                if verbose:
                    self.logger.error('%s containse replica count %s != %s' % (table, replica_count, expect_replica_count))
                else:
                    return True
        return False

    def is_state_abnormal(self):
        return self.has_bad_table(verbose=False)

    def diagnose(self):
        self.has_bad_table(verbose=True)
        self.logger.error('!!!THIS MAY CAUSE DATA LOSS!!!Please consult RD for further infomation')

    def repair(self):
        self.has_bad_table(verbose=True)
        self.logger.error('!!!THIS MAY CAUSE DATA LOSS!!!Please consult RD for further infomation')
