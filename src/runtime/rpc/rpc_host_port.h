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

#include <string>
#include <vector>

#include "runtime/rpc/rpc_address.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"

namespace dsn {

class host_port
{
public:
    explicit host_port() = default;
    explicit host_port(std::string host, uint16_t port);
    explicit host_port(rpc_address addr);

    host_port(const host_port &other) { *this = other; }
    host_port &operator=(const host_port &other);

    void reset();
    ~host_port() { reset(); }

    dsn_host_type_t type() const { return _type; }
    const std::string &host() const { return _host; }
    uint16_t port() const { return _port; }
    rpc_group_address *group_address() const { return _group_address; }

    bool is_invalid() const { return _type == HOST_TYPE_INVALID; }

    void assign_group(const char *name);
    std::string to_string() const;
    error_s resolve_addresses(std::vector<rpc_address> *addresses) const;

    friend std::ostream &operator<<(std::ostream &os, const host_port &hp)
    {
        return os << hp.to_string();
    }

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

private:
    explicit host_port(rpc_group_address *group_address);

    std::string _host;
    uint16_t _port = 0;
    dsn_host_type_t _type = HOST_TYPE_INVALID;
    rpc_group_address *_group_address;
};

inline bool operator<(const host_port &hp1, const host_port &hp2)
{
    if (hp1.type() != hp2.type())
        return hp1.type() < hp2.type();

    switch (hp1.type()) {
    case HOST_TYPE_IPV4:
        return hp1.host() < hp2.host() || (hp1.host() == hp2.host() && hp1.port() < hp2.port());
    case HOST_TYPE_GROUP:
        return hp1.group_address() < hp2.group_address();
    default:
        return true;
    }
}

inline bool operator==(const host_port &hp1, const host_port &hp2)
{
    if (&hp1 == &hp2) {
        return true;
    }

    if (hp1.type() != hp2.type()) {
        return false;
    }

    switch (hp1.type()) {
    case HOST_TYPE_IPV4:
        return hp1.host() == hp2.host() && hp1.port() == hp2.port();
    case HOST_TYPE_GROUP:
        return hp1.group_address() == hp2.group_address();
    default:
        return true;
    }
}

inline bool operator!=(const host_port &hp1, const host_port &hp2) { return !(hp1 == hp2); }

} // namespace dsn

namespace std {
template <>
struct hash<::dsn::host_port>
{
    size_t operator()(const ::dsn::host_port &hp) const
    {
        switch (hp.type()) {
        case HOST_TYPE_IPV4:
            return std::hash<std::string>()(hp.host()) ^ hp.port();
        case HOST_TYPE_GROUP:
            return std::hash<void *>()(hp.group_address());
        default:
            return 0;
        }
    }
};

} // namespace std
