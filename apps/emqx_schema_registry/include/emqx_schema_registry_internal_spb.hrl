%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_SCHEMA_REGISTRY_INTERNAL_SPB_HRL).
-define(EMQX_SCHEMA_REGISTRY_INTERNAL_SPB_HRL, true).

-record(nbirth, {
    namespace,
    group_id,
    edge_node_id
}).
-record(ndata, {
    namespace,
    group_id,
    edge_node_id
}).
-record(dbirth, {
    namespace,
    group_id,
    edge_node_id,
    device_id
}).
-record(ddata, {
    namespace,
    group_id,
    edge_node_id,
    device_id
}).

-endif.
