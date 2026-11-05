%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SESSION_MEM_HRL).
-define(EMQX_SESSION_MEM_HRL, true).

-include("emqx_session_conf.hrl").

-record(session, {
    conf :: #session_conf{},
    id :: emqx_session:session_id(),
    %% Is this session a persistent session i.e. was it started with Session-Expiry > 0
    is_persistent :: boolean(),
    %% Client’s Subscriptions.
    subscriptions :: emqx_session_mem:subscriptions(),
    %% Client <- Broker: QoS1/2 messages sent to the client but
    %% have not been unacked.
    inflight :: emqx_inflight:inflight(),
    %% All QoS1/2 messages published to when client is disconnected,
    %% or QoS1/2 messages pending transmission to the Client.
    %%
    %% Optionally, QoS0 messages pending transmission to the Client.
    %% `{empty, MaxLen}` is a placeholder for an empty queue: the queue is
    %% built from the zone config on the first enqueue.
    mqueue :: emqx_mqueue:mqueue() | {empty, non_neg_integer()},
    %% Delivery rate limiters.
    %% `{lazy, ListenerId}` is a placeholder: the container is built on
    %% first delivery, once a finite limit is configured.
    quota :: emqx_limiter_client_container:t() | false | {lazy, emqx_limiter:listener_id()},
    %% Next packet id of the session
    next_pkt_id = 1 :: emqx_types:packet_id(),
    %% Client -> Broker: QoS2 messages received from the client, but
    %% have not been completely acknowledged
    awaiting_rel :: emqx_session_mem:awaiting_rel(),
    %% Created at
    created_at :: pos_integer()
}).

-ifdef(TEST).
-define(DEFAULT_BATCH_N, 10).
-else.
-define(DEFAULT_BATCH_N, 1000).
-endif.

-endif.
