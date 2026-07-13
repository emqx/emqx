%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SESSION_MEM_HRL).
-define(EMQX_SESSION_MEM_HRL, true).

-record(session, {
    id :: emqx_session:session_id(),
    %% Is this session a persistent session i.e. was it started with Session-Expiry > 0
    is_persistent :: boolean(),
    %% Client’s Subscriptions.
    subscriptions :: emqx_session_mem:subscriptions(),
    %% Max subscriptions allowed
    max_subscriptions :: non_neg_integer() | infinity,
    %% Upgrade QoS?
    upgrade_qos = false :: boolean(),
    %% Client <- Broker: QoS1/2 messages sent to the client but
    %% have not been unacked.
    inflight :: emqx_inflight:inflight(),
    %% Cached MQTT payload bytes currently retained by inflight.
    %% Kept outside `emqx_inflight` so that container stays content-agnostic.
    inflight_payload_bytes = 0 :: non_neg_integer(),
    %% All QoS1/2 messages published to when client is disconnected,
    %% or QoS1/2 messages pending transmission to the Client.
    %%
    %% Optionally, QoS0 messages pending transmission to the Client.
    mqueue :: emqx_mqueue:mqueue(),
    %% Delivery rate limiters.
    quota :: emqx_limiter_client_container:t() | false,
    %% Next packet id of the session
    next_pkt_id = 1 :: emqx_types:packet_id(),
    %% Retry interval for redelivering QoS1/2 messages (Unit: millisecond)
    retry_interval :: timeout(),
    %% Client -> Broker: QoS2 messages received from the client, but
    %% have not been completely acknowledged
    awaiting_rel :: emqx_session_mem:awaiting_rel(),
    %% Maximum number of awaiting QoS2 messages allowed
    max_awaiting_rel :: non_neg_integer() | infinity,
    %% Awaiting PUBREL Timeout (Unit: millisecond)
    await_rel_timeout :: timeout(),
    %% Created at
    created_at :: pos_integer()
}).

-endif.
