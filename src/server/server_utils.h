// Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
// @author liguohao(liguohao@sensorsdata.cn)

#include <string>

#define STR_I(var) #var
#define STR(var) STR_I(var)
#ifndef DSN_BUILD_TYPE
#define PEGASUS_BUILD_TYPE ""
#else
#define PEGASUS_BUILD_TYPE STR(DSN_BUILD_TYPE)
#endif

void dsn_app_registration_commands(const std::string &);

std::string get_pegasus_server_rcsid(const std::string &);
