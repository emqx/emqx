%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_HRL).
-define(EMQX_HRL, true).

%% Shard
%%--------------------------------------------------------------------
-define(COMMON_SHARD, emqx_common_shard).
-define(SHARED_SUB_SHARD, emqx_shared_sub_shard).
-define(CM_SHARD, emqx_cm_shard).
%% V2 route shard (uses regular mria tables)
-define(ROUTE_SHARD_V2, route_shard).
%% V3 route shard (uses merged mria tables)
-define(ROUTE_SHARD_V3, route_shard_m).
%% Persistent session router shard:
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

%% Message delivery routed to a channel.
-record(deliver, {
    %% Subscription topic
    topic :: emqx_types:topic(),
    %% Routed message
    message :: emqx_types:message()
}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(share_dest, {
    session_id :: emqx_session:session_id(),
    group :: emqx_types:group()
}).

-record(route, {topic, dest}).

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

%%--------------------------------------------------------------------
%% Client Attributes
%%--------------------------------------------------------------------
-define(CLIENT_ATTR_NAME_TNS, <<"tns">>).

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

-define(ACCESS_CONTROL_METRICS_WORKER, access_control_metrics).

-endif.
