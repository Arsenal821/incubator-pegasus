#!/bin/env python
# -*- coding: UTF-8 -*-

"""
Copyright (c) 2021 SensorsData, Inc. All Rights Reserved
@author padme(jinsilan@sensorsdata.cn)
@brief


"""
from stepworker.base_step import BaseStep


class SnapshotDoneStep(BaseStep):
    def update(self):
        pass

    def backup(self):
        pass

    def check(self):
        return True
