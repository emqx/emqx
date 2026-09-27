%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_CONNECTION_CONF_HRL).
-define(EMQX_CONNECTION_CONF_HRL, true).

%% Connection settings derived from zone config. Connections of the same zone
%% share one instance, see `emqx_connection_conf'.
-record(zone_conf, {
    %% Zone name
    name :: atom(),
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
