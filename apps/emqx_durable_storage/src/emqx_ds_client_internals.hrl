%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_CLIENT_INTERNALS_HRL).
-define(EMQX_DS_CLIENT_INTERNALS_HRL, true).

%% "Logical" sub: a record that is created for each successful `subscribe' call:
-record(sub, {
    db :: emqx_ds:db(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    ds_sub_opts :: emqx_ds:sub_opts()
}).

%% Atomic indexes:
-define(ds_sub_a_seqno, 1).
-define(ds_sub_a_stuck, 2).
-define(ds_sub_a_lagging, 3).

-record(ds_sub, {
    id,
    slab,
    db,
    stream,
    handle,
    %% SeqNo, Stuck, Lagging:
    vars = atomics:new(3, [{signed, false}])
}).

-record(stream_cache, {
    current_gen,
    pending_iterator = [],
    active = #{},
    %%    Streams that have been fully replayed:
    replayed = #{},
    %% Streams that belong to the future generations (sorted by generation):
    future = gb_sets:new()
}).

%% Client state
-record(cs, {
    ref = make_ref(),
    cbm,
    options,
    %% Retry:
    retry_tref,
    %% "Logical" subs that hold data for each subscription created by `subscribe' API:
    subs = #{},
    streams = #{},
    new_streams_watches = #{},
    ds_subs = #{},
    plan = [],
    retry = []
}).

-define(record_to_map(RECORD, VALUE),
    (fun(Val) ->
        Fields = record_info(fields, RECORD),
        [_Tag | Values] = tuple_to_list(Val),
        maps:from_list(lists:zip(Fields, Values))
    end)(
        VALUE
    )
).

-endif.
