%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-ifndef(EMQX_HRL).
-define(EMQX_HRL, true).

%% Shard
%%--------------------------------------------------------------------
-define(COMMON_SHARD, emqx_common_shard).
-define(SHARED_SUB_SHARD, emqx_shared_sub_shard).
-define(CM_SHARD, emqx_cm_shard).
-define(ROUTE_SHARD, route_shard).
-define(PERSISTENT_SESSION_SHARD, emqx_persistent_session_shard).

-define(BOOT_SHARDS, [
    ?ROUTE_SHARD,
    ?COMMON_SHARD,
    ?SHARED_SUB_SHARD,
    ?PERSISTENT_SESSION_SHARD
]).

%% Banner
%%--------------------------------------------------------------------

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "10.0").

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System topic
-define(SYSTOP, <<"$SYS/">>).

%% Queue topic
-define(QUEUE, <<"$queue/">>).

%%--------------------------------------------------------------------
%% alarms
%%--------------------------------------------------------------------
-define(ACTIVATED_ALARM, emqx_activated_alarm).
-define(DEACTIVATED_ALARM, emqx_deactivated_alarm).
-define(TRIE, emqx_trie).

%%--------------------------------------------------------------------
%% Message and Delivery
%%--------------------------------------------------------------------

-record(subscription, {topic, subid, subopts}).

%% See 'Application Message' in MQTT Version 5.0
-record(message, {
    %% Global unique message ID
    id :: binary(),
    %% Message QoS
    qos = 0,
    %% Message from
    from :: atom() | binary(),
    %% Message flags
    flags = #{} :: emqx_types:flags(),
    %% Message headers. May contain any metadata. e.g. the
    %% protocol version number, username, peerhost or
    %% the PUBLISH properties (MQTT 5.0).
    headers = #{} :: emqx_types:headers(),
    %% Topic that the message is published to
    topic :: emqx_types:topic(),
    %% Message Payload
    payload :: emqx_types:payload(),
    %% Timestamp (Unit: millisecond)
    timestamp :: integer(),
    %% not used so far, for future extension
    extra = [] :: term()
}).

-record(delivery, {
    %% Sender of the delivery
    sender :: pid(),
    %% The message delivered
    message :: #message{}
}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route, {
    topic :: binary(),
    dest :: node() | {binary(), node()} | emqx_session:sessionID()
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
    who ::
        {clientid, binary()}
        | {peerhost, inet:ip_address()}
        | {username, binary()},
    by :: binary(),
    reason :: binary(),
    at :: integer(),
    until :: integer()
}).

%%--------------------------------------------------------------------
%% Authentication
%%--------------------------------------------------------------------

-record(authenticator, {
    id :: binary(),
    provider :: module(),
    enable :: boolean(),
    state :: map()
}).

-record(chain, {
    name :: atom(),
    authenticators :: [#authenticator{}]
}).

-endif.
