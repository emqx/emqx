%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-record(channel, {
    ctx,
    conninfo,
    clientinfo,
    session,
    keepalive,
    timers,
    connection_required,
    conn_state,
    token,
    blockwise
}).

-record(transport, {
    cache,
    req_context,
    retry_interval,
    retry_count,
    observe
}).

-record(state_machine, {
    seq_id,
    id,
    token,
    observe,
    state,
    timers,
    transport
}).

-record(keepalive, {
    check_interval,
    statval,
    stat_reader,
    idle_milliseconds = 0,
    max_idle_millisecond
}).
