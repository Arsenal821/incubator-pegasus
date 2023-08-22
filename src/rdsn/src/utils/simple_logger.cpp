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

#include "simple_logger.h"
#include <dirent.h>
#include <errno.h>
#include <queue>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/glob_posix.h>
#include <dsn/utility/safe_strerror_posix.h>
#include <dsn/utility/flags.h>
#include <dsn/utils/time_utils.h>
#include <fmt/format.h>

namespace dsn {
namespace tools {

DSN_DEFINE_uint64("tools.simple_logger",
                  max_log_file_bytes,
                  64 * 1024 * 1024,
                  "max bytes of a log file");

DSN_DEFINE_bool("tools.simple_logger", fast_flush, false, "whether to flush immediately");

DSN_DEFINE_bool("tools.simple_logger",
                short_header,
                true,
                "whether to use short header (excluding file/function etc.)");

DSN_DEFINE_uint64("tools.simple_logger",
                  max_number_of_log_files_on_disk,
                  20,
                  "max number of log files reserved on disk, older logs are auto deleted");

DSN_DEFINE_string("tools.simple_logger",
                  stderr_start_level,
                  "LOG_LEVEL_WARNING",
                  "copy log messages at or above this level to stderr in addition to logfiles");
DSN_DEFINE_validator(stderr_start_level, [](const char *level) -> bool {
    return strcmp(level, "LOG_LEVEL_INVALID") != 0;
});

screen_logger::screen_logger(bool short_header) : logging_provider("./", "")
{
    _short_header = short_header;
}

screen_logger::screen_logger(const char *log_dir, const char *role_name)
    : logging_provider(log_dir, role_name)
{
    _short_header =
        dsn_config_get_value_bool("tools.screen_logger",
                                  "short_header",
                                  true,
                                  "whether to use short header (excluding file/function etc.)");
}

screen_logger::~screen_logger(void) {}

void screen_logger::dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             va_list args)
{
    utils::auto_lock<::dsn::utils::ex_lock_nr> l(_lock);

    print_header(stdout, log_level);
    if (!_short_header) {
        printf("%s:%d:%s(): ", file, line, function);
    }
    vprintf(fmt, args);
    printf("\n");

    if (dsn_unlikely(log_level >= LOG_LEVEL_FATAL)) {
        dsn_coredump();
    }
}

void screen_logger::flush() { ::fflush(stdout); }

simple_logger::simple_logger(const char *log_dir, const char *role_name)
    : logging_provider(log_dir, role_name)
{
    _log_dir = std::string(log_dir);
    _role_name = std::string(role_name);
    if (_role_name.empty()) {
        _role_name = dsn_config_get_value_string(
            "tools.simple_logger", "base_name", "skv", "default base name for log file");
    }

    std::string symlink_name(_role_name);
    symlink_name += ".log";
    _symlink_path = utils::filesystem::path_combine(_log_dir, symlink_name);

    _file_name_prefix = symlink_name;
    _file_name_prefix += ".";
    _file_path_pattern = utils::filesystem::path_combine(_log_dir, _file_name_prefix);
    _file_path_pattern += "*";

    // we assume all valid entries are positive
    _file_bytes = 0;
    _log = nullptr;
    _stderr_start_level = enum_from_string(FLAGS_stderr_start_level, LOG_LEVEL_INVALID);

    dassert(FLAGS_max_log_file_bytes > 0,
            "invalid [tools.simple_logger] max_log_file_bytes specified "
            "which should be > 0");

    dassert(FLAGS_max_number_of_log_files_on_disk > 0,
            "invalid [tools.simple_logger] max_number_of_log_files_on_disk specified "
            "which should be > 0");

    create_log_file();
}

void simple_logger::create_log_file()
{
    if (_log != nullptr)
        ::fclose(_log);

    _file_bytes = 0;

    uint64_t ts = dsn::utils::get_current_physical_time_ns();
    char time_str[32];
    ::dsn::utils::time_ms_to_string_for_log_file_name(ts / 1000000, time_str);

    std::string file_name(_file_name_prefix);
    file_name += time_str;

    std::string path(utils::filesystem::path_combine(_log_dir, file_name));
    _log = ::fopen(path.c_str(), "w+");
    if (_log == nullptr) {
        std::string error(dsn::utils::safe_strerror(errno));
        dassert(false, "Failed to fopen %s: %s", path.c_str(), error.c_str());
    }

    if (::unlink(_symlink_path.c_str()) != 0) {
        if (errno != ENOENT) {
            std::string error(dsn::utils::safe_strerror(errno));
            fprintf(stdout, "Failed to unlink %s: %s\n", _symlink_path.c_str(), error.c_str());
        }
    }

    if (::symlink(file_name.c_str(), _symlink_path.c_str()) != 0) {
        std::string error(dsn::utils::safe_strerror(errno));
        fprintf(stdout,
                "Failed to symlink %s as %s: %s\n",
                file_name.c_str(),
                _symlink_path.c_str(),
                error.c_str());
    }

    remove_redundant_files();
}

void simple_logger::remove_redundant_files()
{
    std::vector<std::string> matching_files;
    dsn::utils::glob(_file_path_pattern, matching_files);

    auto max_matches = static_cast<size_t>(FLAGS_max_number_of_log_files_on_disk);
    if (matching_files.size() <= max_matches) {
        return;
    }

    std::vector<std::pair<time_t, std::string>> matching_file_mtimes;
    for (auto &matching_file_path : matching_files) {
        struct stat s;
        if (::stat(matching_file_path.c_str(), &s) != 0) {
            std::string error(dsn::utils::safe_strerror(errno));
            fprintf(stdout, "Failed to stat %s: %s\n", matching_file_path.c_str(), error.c_str());
            return;
        }

        int64_t mtime = s.st_mtim.tv_sec * 1000000 + s.st_mtim.tv_nsec / 1000;
        matching_file_mtimes.emplace_back(mtime, std::move(matching_file_path));
    }

    // Use mtime to determine which matching files to delete. This could
    // potentially be ambiguous, depending on the resolution of last-modified
    // timestamp in the filesystem, but that is part of the contract.
    std::sort(matching_file_mtimes.begin(), matching_file_mtimes.end());
    matching_file_mtimes.resize(matching_file_mtimes.size() - max_matches);

    for (const auto &matching_file : matching_file_mtimes) {
        const auto &path = matching_file.second;
        if (::remove(path.c_str()) != 0) {
            // if remove failed, just print log and ignore it.
            std::string error(dsn::utils::safe_strerror(errno));
            fprintf(stdout,
                    "Failed to remove redundant log file %s: %s\n",
                    path.c_str(),
                    error.c_str());
        }
    }
}

simple_logger::~simple_logger(void)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fclose(_log);
}

void simple_logger::flush()
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    ::fflush(_log);
    ::fflush(stdout);
}

void simple_logger::dsn_logv(const char *file,
                             const char *function,
                             const int line,
                             dsn_log_level_t log_level,
                             const char *fmt,
                             va_list args)
{
    va_list args2;
    if (log_level >= _stderr_start_level) {
        va_copy(args2, args);
    }

    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    dassert(_log != nullptr, "Log file hasn't been initialized yet");

    write_header(log_level);
    if (!FLAGS_short_header) {
        write_log("%s:%d:%s(): ", file, line, function);
    }
    write_logv(fmt, args);
    write_log("\n");
    if (FLAGS_fast_flush || log_level >= LOG_LEVEL_ERROR) {
        ::fflush(_log);
    }

    if (log_level >= _stderr_start_level) {
        print_header(stdout, log_level);
        if (!FLAGS_short_header) {
            printf("%s:%d:%s(): ", file, line, function);
        }
        vprintf(fmt, args2);
        printf("\n");
    }

    if (_file_bytes >= FLAGS_max_log_file_bytes) {
        create_log_file();
    }
}

void simple_logger::dsn_log(const char *file,
                            const char *function,
                            const int line,
                            dsn_log_level_t log_level,
                            const char *str)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    dassert(_log != nullptr, "Log file hasn't been initialized yet");

    write_header(log_level);
    if (!FLAGS_short_header) {
        write_log("%s:%d:%s(): ", file, line, function);
    }
    write_log("%s\n", str);
    if (FLAGS_fast_flush || log_level >= LOG_LEVEL_ERROR) {
        ::fflush(_log);
    }

    if (log_level >= _stderr_start_level) {
        print_header(stdout, log_level);
        if (!FLAGS_short_header) {
            printf("%s:%d:%s(): ", file, line, function);
        }
        printf("%s\n", str);
    }

    if (dsn_unlikely(log_level >= LOG_LEVEL_FATAL)) {
        dsn_coredump();
    }

    if (_file_bytes >= FLAGS_max_log_file_bytes) {
        create_log_file();
    }
}

} // namespace tools
} // namespace dsn
