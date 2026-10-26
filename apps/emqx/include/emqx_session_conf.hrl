%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SESSION_CONF_HRL).
-define(EMQX_SESSION_CONF_HRL, true).

%% Session settings shared through emqx_connection_conf.
-record(session_conf, {
    %% Maximum number of subscriptions allowed
    max_subscriptions = infinity :: non_neg_integer() | infinity,
    %% Upgrade message QoS to the subscription QoS
    upgrade_qos = false :: boolean(),
    %% Retry interval for redelivering QoS1/2 messages (milliseconds / `infinity` disables)
    retry_interval = infinity :: pos_integer() | infinity,
    %% Maximum number of QoS2 messages awaiting PUBREL
    max_awaiting_rel = infinity :: non_neg_integer() | infinity,
    %% Awaiting PUBREL timeout (Unit: millisecond)
    await_rel_timeout = infinity :: timeout()
}).

-endif.
