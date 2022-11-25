/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <dsn/utility/glob_posix.h>

#include <cstdio>
#include <errno.h>
#include <glob.h>
#include <dsn/utility/defer.h>
#include <dsn/utility/safe_strerror_posix.h>

namespace dsn {
namespace utils {

void glob(const std::string &path_pattern, std::vector<std::string> &path_list)
{
    glob_t result;
    auto cleanup = dsn::defer([&] { globfree(&result); });

    errno = 0;
    int ret = ::glob(path_pattern.c_str(), GLOB_TILDE | GLOB_ERR , NULL, &result);
    switch (ret) {
        case 0:
            break;

        case GLOB_NOMATCH:
            return;

        case GLOB_NOSPACE:
            fprintf(stdout, "glob out of memory");
            return;

        default:
            std::string error(errno == 0 ? "unknown error" : safe_strerror(errno));
            fprintf(stdout, "glob failed for %s: %s\n",
                    path_pattern.c_str(), error.c_str());
            return;
    }

    for (size_t i = 0; i < result.gl_pathc; ++i) {
        path_list.emplace_back(result.gl_pathv[i]);
    }
}

} // namespace utils
} // namespace dsn
