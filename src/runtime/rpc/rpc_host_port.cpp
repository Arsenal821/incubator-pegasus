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

#include "runtime/rpc/group_address.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/safe_strerror_posix.h"
#include "utils/utils.h"

#include <netdb.h>
#include <sys/socket.h>
#include <unordered_set>

namespace dsn {

namespace {

using AddrInfo = std::unique_ptr<addrinfo, std::function<void(addrinfo *)>>;

// TODO: test, copy from Kudu
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
    //CHECK_NE_MSG(rpc_address::ipv4_from_host(host.c_str()), 0, "invalid hostname {}.", host);
}

host_port::host_port(rpc_group_address *group_address)
{
    CHECK_NOTNULL(group_address, "invalid group_address!");
    _type = HOST_TYPE_GROUP;
    _group_address = group_address;
    _group_address->add_ref();
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
    case HOST_TYPE_GROUP:
        *this = host_port(addr.group_address());
        break;
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
        group_address()->release_ref();
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
            _group_address = other.group_address();
            group_address()->add_ref();
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
    _group_address = new rpc_group_address(name);
    // take the lifetime of rpc_uri_address, release_ref when change value or call destructor
    _group_address->add_ref();
}

std::string host_port::to_string() const
{
    switch (type()) {
    case HOST_TYPE_IPV4:
        return fmt::format("{}:{}", _host, _port);
    case HOST_TYPE_GROUP:
        return fmt::format("address group {}", group_address()->name());
    default:
        return "invalid address";
    }
}

error_s host_port::resolve_addresses(std::vector<rpc_address> *addresses) const
{
    CHECK_EQ_MSG(type(), HOST_TYPE_IPV4, "invalid host_port type");

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
