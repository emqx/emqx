%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_CONNECTION_CONF_HRL).
-define(EMQX_CONNECTION_CONF_HRL, true).

%% Settings of a connection process, derived from its listener and zone
%% config. Connections of the same listener and zone share one instance,
%% see `emqx_connection_conf'.
-record(conf, {
    %% Listener Type and Name
    listener :: {Type :: atom(), Name :: atom()},
    %% Zone name
    zone :: atom(),
    %% ActiveN
    active_n :: pos_integer(),
    %% Send queue high watermark in bytes
    sendq_watermark :: non_neg_integer(),
    %% Hibernate connection process if inactive for
    hibernate_after :: integer() | infinity,
    %% Run a minor GC after this period of mailbox inactivity
    minor_gc_after :: non_neg_integer() | infinity,
    %% Forced GC thresholds, `false` if disabled
    force_gc :: false | {_EachNMessages :: pos_integer(), _EachNBytes :: pos_integer()},
    %% Forced shutdown policy
    force_shutdown :: emqx_types:oom_policy()
}).

-endif.
