#include "runtime/rpc/serialization.h"

#include "dsn.layer2_types.h"

namespace dsn {
template<> inline void unmarshall(dsn::message_ex *msg, /*out*/ partition_configuration &val)
{
    ::dsn::rpc_read_stream reader(msg);
    unmarshall(reader, val, (dsn_msg_serialize_format)msg->header->context.u.serialize_format);
    FILL_OPTIONAL_HP_IF_NEEDED(val, primary);
    FILL_OPTIONAL_HP_LIST_IF_NEEDED(val, secondaries);
    FILL_OPTIONAL_HP_LIST_IF_NEEDED(val, last_drops);
}
}
