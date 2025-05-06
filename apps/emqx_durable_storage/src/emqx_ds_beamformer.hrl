%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_DS_BEAMFORMER_HRL).
-define(EMQX_DS_BEAMFORMER_HRL, true).

-record(sub_state, {
    %% Unique ID of the subscription:
    req_id,
    %% PID of the subscribing process:
    client,
    %% Flow control:
    flowcontrol,
    %%
    rank,
    stream,
    topic_filter,
    start_key,
    %% Iterator:
    it
}).

%% Persistent term used to store various global information about the
%% shard:
-define(pt_gvar(SHARD), {emqx_ds_beamformer_gvar, SHARD}).

-define(DESTINATION(CLIENT, SUBREF, SEQNO, MASK, FLAGS, ITERATOR),
    {CLIENT, SUBREF, SEQNO, MASK, FLAGS, ITERATOR}
).

%% Bit flags that encode various subscription metadata:
-define(DISPATCH_FLAG_STUCK, 1).
-define(DISPATCH_FLAG_LAGGING, 2).

-record(unsub_req, {id :: reference()}).

-endif.
