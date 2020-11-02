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

#pragma once

#include <dsn/dist/replication/replica_base.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <rrdb/rrdb_types.h>

namespace pegasus {
namespace server {

class capacity_unit_calculator : public dsn::replication::replica_base
{
public:
    explicit capacity_unit_calculator(replica_base *r);

    void add_get_cu(int32_t status, const dsn::blob &key, const dsn::blob &value);
    void add_multi_get_cu(int32_t status,
                          const dsn::blob &hash_key,
                          const std::vector<::dsn::apps::key_value> &kvs);
    void add_scan_cu(int32_t status, const std::vector<::dsn::apps::key_value> &kvs);
    void add_sortkey_count_cu(int32_t status);
    void add_ttl_cu(int32_t status);

    void add_put_cu(int32_t status, const dsn::blob &key, const dsn::blob &value);
    void add_remove_cu(int32_t status, const dsn::blob &key);
    void add_multi_put_cu(int32_t status,
                          const dsn::blob &hash_key,
                          const std::vector<::dsn::apps::key_value> &kvs);
    void add_multi_remove_cu(int32_t status,
                             const dsn::blob &hash_key,
                             const std::vector<::dsn::blob> &sort_keys);
    void add_incr_cu(int32_t status);
    void add_check_and_set_cu(int32_t status,
                              const dsn::blob &hash_key,
                              const dsn::blob &check_sort_key,
                              const dsn::blob &set_sort_key,
                              const dsn::blob &value);
    void add_check_and_mutate_cu(int32_t status,
                                 const dsn::blob &hash_key,
                                 const dsn::blob &check_sort_key,
                                 const std::vector<::dsn::apps::mutate> &mutate_list);

protected:
    friend class capacity_unit_calculator_test;

#ifdef PEGASUS_UNIT_TEST
    virtual int64_t add_read_cu(int64_t read_data_size);
    virtual int64_t add_write_cu(int64_t write_data_size);
#else
    int64_t add_read_cu(int64_t read_data_size);
    int64_t add_write_cu(int64_t write_data_size);
#endif

private:
    uint64_t _read_capacity_unit_size;
    uint64_t _write_capacity_unit_size;
    uint32_t _log_read_cu_size;
    uint32_t _log_write_cu_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;

    ::dsn::perf_counter_wrapper _pfc_get_bytes;
    ::dsn::perf_counter_wrapper _pfc_multi_get_bytes;
    ::dsn::perf_counter_wrapper _pfc_scan_bytes;
    ::dsn::perf_counter_wrapper _pfc_put_bytes;
    ::dsn::perf_counter_wrapper _pfc_multi_put_bytes;
    ::dsn::perf_counter_wrapper _pfc_check_and_set_bytes;
    ::dsn::perf_counter_wrapper _pfc_check_and_mutate_bytes;
};

} // namespace server
} // namespace pegasus
