// Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
// @author wangdan(wangdan@sensorsdata.cn)

#include "args.h"
#include "pegasus_server_impl.h"
#include "pegasus_service_app.h"
#include "server_utils.h"

#include <pegasus/version.h>
#include <pegasus/git_commit.h>

#include <dsn/dist/replication/replication_service_app.h>

#include <cstdio>
#include <cstring>
#include <chrono>

#include <sys/types.h>
#include <unistd.h>


using namespace dsn;
using namespace dsn::replication;

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "-version") == 0 ||
            strcmp(argv[i], "--version") == 0) {
            printf("Replica Server %s (%s) %s\n",
                   PEGASUS_VERSION,
                   PEGASUS_GIT_COMMIT,
                   PEGASUS_BUILD_TYPE);
            dsn_exit(0);
        }
    }
    ddebug("replica server starting, pid(%d), version(%s)", (int)getpid(), get_pegasus_server_rcsid("Replica").c_str());

    service_app::register_factory<pegasus::server::pegasus_replication_service_app>("replica");
    pegasus::server::pegasus_server_impl::register_service();

    dsn_app_registration_commands("Replica");

    std::unique_ptr<pegasus::server::Args> new_args;
    pegasus::server::sanitize_args("replica", argc, argv, &new_args);
    char **new_argv = const_cast<char **>(new_args->get_argv());
    int new_argc = new_args->get_argc();

    dsn_run(argc, argv, true);

    return 0;
}
