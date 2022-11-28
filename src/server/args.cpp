// Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
// @author wangdan(wangdan@sensorsdata.cn)

#include "args.h"

#include <cstring>
#include <dsn/c/app_model.h>
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
namespace server {

Args::Args(std::vector<std::string> &&arg_list)
    : _arg_list(std::move(arg_list)), _argv(dsn::make_unique<const char *[]>(_arg_list.size()))
{
    for (std::vector<std::string>::size_type i = 0; i < _arg_list.size(); ++i) {
        _argv[i] = _arg_list[i].c_str();
    }
}

void sanitize_args(const std::string &app_name,
                   const int argc,
                   const char *const *argv,
                   std::unique_ptr<Args> *new_args)
{
    bool has_app_list = false;
    std::vector<std::string> arg_list;
    for (int i = 0; i < argc; ++i) {
        arg_list.push_back(argv[i]);

        if (::strcmp(argv[i], "-app_list") != 0) {
            continue;
        }

        if (i + 1 >= argc) {
            printf("argument for '-app_list' is required\n");
            dsn_exit(1);
        }

        if (app_name.compare(argv[i + 1]) != 0) {
            printf("invalid argument(=%s) for '-app_list' "
                   "since target app_name is %s\n",
                   argv[i + 1],
                   app_name.c_str());
            dsn_exit(1);
        }

        has_app_list = true;
    }

    if (!has_app_list) {
        arg_list.push_back("-app_list");
        arg_list.push_back(app_name);
    }

    *new_args = dsn::make_unique<Args>(std::move(arg_list));
}

} // namespace server
} // namespace pegasus
