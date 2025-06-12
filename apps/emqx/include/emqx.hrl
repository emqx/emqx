%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_HRL).
-define(EMQX_HRL, true).

%% Shard
%%--------------------------------------------------------------------
-define(COMMON_SHARD, emqx_common_shard).
-define(SHARED_SUB_SHARD, emqx_shared_sub_shard).
-define(CM_SHARD, emqx_cm_shard).
-define(ROUTE_SHARD, route_shard).
-define(PS_ROUTER_SHARD, persistent_session_router_shard).

%% Banner
%%--------------------------------------------------------------------

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "10.0").

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System topic
-define(SYSTOP, <<"$SYS/">>).

%%--------------------------------------------------------------------
%% alarms
%%--------------------------------------------------------------------
-define(ACTIVATED_ALARM, emqx_activated_alarm).
-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).

%%--------------------------------------------------------------------
%% Message and Delivery
%%--------------------------------------------------------------------

-record(subscription, {topic, subid, subopts}).

-include_lib("emqx_utils/include/emqx_message.hrl").

-record(delivery, {
    %% Sender of the delivery
    sender :: pid(),
    %% The message delivered
    message :: #message{}
}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(share_dest, {
    session_id :: emqx_session:session_id(),
    group :: emqx_types:group()
}).

-record(route, {
    topic :: binary(),
    dest ::
        node()
        | {binary(), node()}
        | emqx_session:session_id()
        %% One session can also have multiple subscriptions to the same topic through different groups
        | #share_dest{}
        | emqx_external_broker:dest()
}).

%%--------------------------------------------------------------------
%% Command
%%--------------------------------------------------------------------

-record(command, {
    name :: atom(),
    action :: atom(),
    args = [] :: list(),
    opts = [] :: list(),
    usage :: string(),
    descr :: string()
}).

%%--------------------------------------------------------------------
%% Banned
%%--------------------------------------------------------------------

-record(banned, {
    who :: emqx_types:banned_who(),
    by :: binary(),
    reason :: binary(),
    at :: integer(),
    until :: integer() | infinity
}).

%%--------------------------------------------------------------------
%% Configurations
%%--------------------------------------------------------------------
-define(KIND_REPLICATE, replicate).
-define(KIND_INITIATE, initiate).

-define(CONFIG_TAB, emqx_config).

-record(?CONFIG_TAB, {
    %% {Namespace, RootKey}
    root_key,
    raw_value,
    checked_value,
    extra = #{}
}).

%%--------------------------------------------------------------------
%% Client Attributes
%%--------------------------------------------------------------------
-define(CLIENT_ATTR_NAME_TNS, <<"tns">>).

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

-define(ACCESS_CONTROL_METRICS_WORKER, access_control_metrics).

-endif.
