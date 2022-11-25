// Copyright (c) 2020 SensorsData, Inc. All Rights Reserved
// @author wangdan(wangdan@sensorsdata.cn)

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace pegasus {
namespace server {

class Args {
public:
    explicit Args(std::vector<std::string> &&);

    Args(const Args &) = delete;

    Args &operator=(const Args &) = delete;

    virtual ~Args() = default;

    const char* const* get_argv() const {
        return _argv.get();
    }

    int get_argc() const {
        return static_cast<int>(_arg_list.size());
    }

private:
    std::vector<std::string> _arg_list;
    std::unique_ptr<const char* []> _argv;
};

void sanitize_args(
        const std::string &app_name,
        const int argc,
        const char * const *argv,
        std::unique_ptr<Args> *new_args
);

} // namespace server
} // namespace pegasus
