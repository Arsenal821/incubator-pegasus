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

#pragma once

#include <dsn/tool_api.h>
#include <thread>
#include <cstdio>

namespace dsn {
namespace tools {

/*
 * screen_logger provides a logger which writes to terminal.
 */
class screen_logger : public logging_provider
{
public:
    screen_logger(bool short_header);
    screen_logger(const char *log_dir, const char *role_name);
    virtual ~screen_logger(void);

    virtual void dsn_logv(const char *file,
                          const char *function,
                          const int line,
                          dsn_log_level_t log_level,
                          const char *fmt,
                          va_list args);

    virtual void dsn_log(const char *file,
                         const char *function,
                         const int line,
                         dsn_log_level_t log_level,
                         const char *str){};

    virtual void flush();

private:
    ::dsn::utils::ex_lock_nr _lock;
    bool _short_header;
};

/*
 * simple_logger provides a logger which writes to file.
 * The max number of lines in a logger file is 200000.
 */
class simple_logger : public logging_provider
{
public:
    simple_logger(const char *log_dir, const char *role_name);
    virtual ~simple_logger(void);

    virtual void dsn_logv(const char *file,
                          const char *function,
                          const int line,
                          dsn_log_level_t log_level,
                          const char *fmt,
                          va_list args);

    virtual void dsn_log(const char *file,
                         const char *function,
                         const int line,
                         dsn_log_level_t log_level,
                         const char *str);

    virtual void flush();

private:
    void create_log_file();

    void remove_redundant_files();

    inline void add_bytes_if_valid(int bytes)
    {
        if (dsn_likely(bytes > 0)) {
            _file_bytes += static_cast<uint64_t>(bytes);
        }
    }

    inline void write_header(dsn_log_level_t log_level)
    {
        int bytes = print_header(_log, log_level);
        add_bytes_if_valid(bytes);
    }

    inline void write_logv(const char *fmt, va_list args)
    {
        int bytes = vfprintf(_log, fmt, args);
        add_bytes_if_valid(bytes);
    }

    inline void write_log(const char *fmt, ...)
    {
        va_list args;
        va_start(args, fmt);
        write_logv(fmt, args);
        va_end(args);
    }

private:
    std::string _log_dir;
    std::string _role_name;
    std::string _symlink_path;
    std::string _file_name_prefix;
    std::string _file_path_pattern;

    ::dsn::utils::ex_lock _lock; // use recursive lock to avoid dead lock when flush() is called
                                 // in signal handler if cored for bad logging format reason.
    FILE *_log;
    uint64_t _file_bytes;

    bool _short_header;
    bool _fast_flush;

    dsn_log_level_t _stderr_start_level;
};
}
}
