// Copyright (c) 2022 SensorsData, Inc. All Rights Reserved
// @author liguohao(liguohao@sensorsdata.cn)

#include "brief_stat.h"
#include "server_utils.h"

#include <dsn/tool_api.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/utils/time_utils.h>

#include <pegasus/git_commit.h>
#include <pegasus/version.h>

void dsn_app_registration_commands(const std::string &app_name)
{
    dsn::command_manager::instance().register_command(
        {"server-info"},
        "server-info - query server information",
        "server-info",
        [&app_name](const std::vector<std::string> &args) {
            char str[100];
            ::dsn::utils::time_ms_to_date_time(dsn::utils::process_start_millis(), str, 100);
            std::ostringstream oss;
            oss << app_name << " server " << PEGASUS_VERSION << " (" << PEGASUS_GIT_COMMIT << ") "
                << PEGASUS_BUILD_TYPE << ", Started at " << str;
            return oss.str();
        });
    dsn::command_manager::instance().register_command(
        {"server-stat"},
        "server-stat - query selected perf counters",
        "server-stat",
        [](const std::vector<std::string> &args) { return pegasus::get_brief_stat(); });
}

std::string get_pegasus_server_rcsid(const std::string &app_name)
{
    char const rcsid[] =
        "$Version: " STR(app_name) " Server " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
                                   " " STR(DSN_BUILD_TYPE)
#endif
                                       ", built by gcc " STR(__GNUC__) "." STR(
                                           __GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
                                           ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                                               ", built at " __DATE__ " " __TIME__ " $";
    return static_cast<std::string>(rcsid);
}
