%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_HRL).
-define(EMQX_DS_HRL, true).

-record(dsbatch, {
    operations :: [emqx_ds:operation()],
    preconditions = [] :: [emqx_ds:precondition()]
}).

-record(message_matcher, {
    %% Fields identifying the message:
    %% Client identifier
    from :: binary(),
    %% Topic that the message is published to
    topic :: emqx_types:topic(),
    %% Timestamp (Unit: millisecond)
    timestamp :: integer(),

    %% Fields the message is matched against:
    %% Message Payload
    payload,
    %% Message headers
    headers = #{} :: emqx_types:headers(),
    %% Extra filters
    %% Reserved for the forward compatibility purposes.
    filters = #{}
}).

-record(ds_sub_reply, {
    ref :: reference(),
    payload :: emqx_ds:next_result(),
    seqno :: emqx_ds:sub_seqno() | undefined,
    size :: non_neg_integer(),
    %% Set to `true' when the subscription becomes inactive due to
    %% falling behind on acks:
    stuck :: boolean() | undefined,
    %% Currently set to `true' when the subscription was fulfilled by
    %% the `catchup' worker and `false' when it's fulfilled by the RT
    %% worker:
    lagging :: boolean() | undefined
}).

-record(ds_tx_commit_reply, {
    ref :: reference(),
    payload :: {ok, emqx_ds:tx_serial()} | emqx_ds:error(_)
}).

-record(new_stream_event, {
    subref :: emqx_ds_new_streams:watch()
}).

-define(err_rec(E), {error, recoverable, E}).
-define(err_unrec(E), {error, unrecoverable, E}).

%% Transaction
-define(ds_tx_serial, tx_serial).

-define(ds_tx_write, w).
-define(ds_tx_delete_topic, dt).
-define(ds_tx_expected, e).
-define(ds_tx_unexpected, u).

-endif.
