/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     Unit-test for logger.
 *
 * Revision history:
 *     Nov., 2015, @shengofsun (Weijie Sun), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "utils/simple_logger.h"
#include <boost/regex.hpp>
#include <gtest/gtest.h>
#include <dsn/utility/filesystem.h>

using namespace dsn;
using namespace dsn::tools;

static const int simple_logger_gc_gap = 20;

static void get_log_files(std::set<std::string> &files)
{
    std::vector<std::string> sub_list;
    ASSERT_TRUE(utils::filesystem::get_subfiles("./", sub_list, false));

    files.clear();
    boost::regex pattern(R"(skv\.log\.[0-9]{8}_[0-9]{6}_[0-9]{3})");
    for (const auto &path : sub_list) {
        std::string name(utils::filesystem::get_file_name(path));
        if (boost::regex_match(name, pattern)) {
            auto ret = files.insert(name);
            ASSERT_TRUE(ret.second);
        }
    }
}

static void compare_log_files(const std::set<std::string> &before_files,
                              const std::set<std::string> &after_files)
{
    ASSERT_FALSE(after_files.empty());

    if (after_files.size() == before_files.size() + 1) {
        for (auto it1 = before_files.begin(), it2 = after_files.begin(); it1 != before_files.end();
             ++it1, ++it2) {
            ASSERT_EQ(*it1, *it2);
        }
    } else if (after_files.size() == before_files.size()) {
        auto it1 = before_files.begin();
        auto it2 = after_files.begin();
        ASSERT_NE(*it1, *it2);

        for (++it1; it1 != before_files.end(); ++it1, ++it2) {
            ASSERT_EQ(*it1, *it2);
        }
    } else {
        ASSERT_TRUE(false) << "Invalid number of log files, before=" << before_files.size()
                           << ", after=" << after_files.size();
    }
}

static void clear_files(const std::vector<std::string> &file_name_list)
{
    for (const auto &name : file_name_list) {
        EXPECT_TRUE(dsn::utils::filesystem::remove_path(name));
    }
}

static void prepare_test_dir()
{
    const char *dir = "./test";
    std::string dr(dir);
    dsn::utils::filesystem::create_directory(dr);
    chdir(dir);
}

static void finish_test_dir()
{
    const char *dir = "./test";
    chdir("..");
    rmdir(dir);
}

void log_print(logging_provider *logger, const char *fmt, ...)
{
    va_list vl;
    va_start(vl, fmt);
    logger->dsn_logv(__FILE__, __FUNCTION__, __LINE__, LOG_LEVEL_INFORMATION, fmt, vl);
    va_end(vl);
}

TEST(tools_common, simple_logger)
{
    // cases for print_header
    screen_logger *logger = new screen_logger("./", "skv");

    log_print(logger, "%s", "test_print");
    std::thread t([](screen_logger *lg) { log_print(lg, "%s", "test_print"); }, logger);
    t.join();

    logger->flush();
    delete logger;

    prepare_test_dir();
    // create multiple files
    for (unsigned int i = 0; i < simple_logger_gc_gap + 10; ++i) {
        std::set<std::string> before_files;
        get_log_files(before_files);

        simple_logger *logger = new simple_logger("./", "skv");

        // in this case stdout is useless
        for (unsigned int i = 0; i != 1000; ++i)
            log_print(logger, "%s", "test_print");
        logger->flush();

        delete logger;

        std::set<std::string> after_files;
        get_log_files(after_files);

        compare_log_files(before_files, after_files);

        ::usleep(2000);
    }

    std::set<std::string> files;
    get_log_files(files);
    EXPECT_TRUE(!files.empty());
    ASSERT_EQ(simple_logger_gc_gap, files.size());

    std::vector<std::string> file_name_list(files.begin(), files.end());
    clear_files(file_name_list);
    finish_test_dir();
}
