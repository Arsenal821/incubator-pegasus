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

#include "runtime/rpc/rpc_host_port.h"
#include "utils/safe_strerror_posix.h"
#include "utils/utils.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unordered_set>

namespace dsn {

const host_port host_port::s_invalid_host_port;

namespace {

using AddrInfo = std::unique_ptr<addrinfo, std::function<void(addrinfo *)>>;

error_s GetAddrInfo(const std::string &hostname, const addrinfo &hints, AddrInfo *info)
{
    addrinfo *res = nullptr;
    const int rc = getaddrinfo(hostname.c_str(), nullptr, &hints, &res);
    const int err = errno; // preserving the errno from the getaddrinfo() call
    AddrInfo result(res, ::freeaddrinfo);
    if (rc != 0) {
        if (rc == EAI_SYSTEM) {
            return error_s::make(ERR_NETWORK_FAILURE, utils::safe_strerror(err));
        }
        return error_s::make(ERR_NETWORK_FAILURE, gai_strerror(rc));
    }

    if (info != nullptr) {
        info->swap(result);
    }

    return error_s::ok();
}
}

host_port::host_port(std::string host, uint16_t port)
    : _host(std::move(host)), _port(port), _type(HOST_TYPE_IPV4)
{
    uint32_t ip = rpc_address::ipv4_from_host(_host.c_str());
    CHECK_NE_MSG(ip, 0, "invalid hostname: {}", _host);
}

host_port::host_port(rpc_address addr)
{
    switch (addr.type()) {
    case HOST_TYPE_IPV4: {
        std::string hostname;
        CHECK(!utils::hostname_from_ip(addr.ipv4_str(), &hostname),
              "invalid address {}",
              addr.ipv4_str());
        *this = host_port(std::move(hostname), addr.port());
    } break;
    case HOST_TYPE_GROUP: {
        auto group_address = addr.group_address();
        *this = host_port();
        this->assign_group(group_address->name());
        for (const auto &address : group_address->members()) {
            CHECK_TRUE(this->group_host_port()->add(host_port(address)));
        }
        this->group_host_port()->set_update_leader_automatically(
            group_address->is_update_leader_automatically());
        this->group_host_port()->set_leader(host_port(group_address->leader()));
    } break;
    default:
        break;
    }
}

void host_port::reset()
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        _host.clear();
        _port = 0;
        break;
    case HOST_TYPE_GROUP:
        group_host_port()->release_ref();
        break;
    default:
        break;
    }
    _type = HOST_TYPE_INVALID;
}

host_port &host_port::operator=(const host_port &other)
{
    if (this != &other) {
        reset();
        _type = other.type();
        switch (type()) {
        case HOST_TYPE_IPV4:
            _host = other.host();
            _port = other.port();
            break;
        case HOST_TYPE_GROUP:
            _group_host_port = other.group_host_port();
            group_host_port()->add_ref();
            break;
        default:
            break;
        }
    }
    return *this;
}

void host_port::assign_group(const char *name)
{
    reset();
    _type = HOST_TYPE_GROUP;
    _group_host_port = new rpc_group_host_port(name);
    // take the lifetime of rpc_uri_address, release_ref when change value or call destructor
    _group_host_port->add_ref();
}

std::string host_port::to_string() const
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        return fmt::format("{}:{}", _host, _port);
    case HOST_TYPE_GROUP:
        return fmt::format("address group {}", group_host_port()->name());
    default:
        return "invalid address";
    }
}

error_s host_port::resolve_addresses(std::vector<rpc_address> *addresses) const
{
    if (type() != HOST_TYPE_IPV4) {
        return std::move(error_s::make(dsn::ERR_INVALID_STATE, "invalid host_port type"));
    }

    rpc_address rpc_addr;
    if (rpc_addr.from_string_ipv4(this->to_string().c_str())) {
        if (addresses) {
            addresses->push_back(rpc_addr);
        }
        return error_s::ok();
    }

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    AddrInfo result;
    RETURN_NOT_OK(GetAddrInfo(_host, hints, &result));

    // DNS may return the same host multiple times. We want to return only the unique
    // addresses, but in the same order as DNS returned them. To do so, we keep track
    // of the already-inserted elements in a set.
    std::unordered_set<rpc_address> inserted;
    std::vector<rpc_address> result_addresses;
    for (const addrinfo *ai = result.get(); ai != nullptr; ai = ai->ai_next) {
        CHECK_EQ(AF_INET, ai->ai_family);
        sockaddr_in *addr = reinterpret_cast<sockaddr_in *>(ai->ai_addr);
        addr->sin_port = htons(_port);
        rpc_address rpc_addr(*addr);
        LOG_INFO("resolved address {} for host_port {}", rpc_addr, to_string());
        if (inserted.insert(rpc_addr).second) {
            result_addresses.emplace_back(rpc_addr);
        }
    }
    if (addresses) {
        *addresses = std::move(result_addresses);
    }
    return error_s::ok();
}

} // namespace dsn
