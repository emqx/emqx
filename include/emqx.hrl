%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQ_X_HRL).
-define(EMQ_X_HRL, true).

%%--------------------------------------------------------------------
%% Banner
%%--------------------------------------------------------------------

-define(COPYRIGHT, "Copyright (c) 2013-2019 EMQ Technologies Co., Ltd").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "10.0").

%%--------------------------------------------------------------------
%% Configs
%%--------------------------------------------------------------------

-define(NO_PRIORITY_TABLE, none).

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System topic
-define(SYSTOP, <<"$SYS/">>).

%% Queue topic
-define(QUEUE,  <<"$queue/">>).

%%--------------------------------------------------------------------
%% Message and Delivery
%%--------------------------------------------------------------------

-record(session, {sid, pid}).

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
          flags :: #{atom() => boolean()},
          %% Message headers, or MQTT 5.0 Properties
          headers = #{},
          %% Topic that the message is published to
          topic :: binary(),
          %% Message Payload
          payload :: binary(),
          %% Timestamp
          timestamp :: erlang:timestamp()
        }).

-record(delivery, {
          sender  :: pid(),      %% Sender of the delivery
          message :: #message{}, %% The message delivered
          results :: list()      %% Dispatches of the message
        }).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route, {
          topic :: binary(),
          dest  :: node() | {binary(), node()}
         }).

%%--------------------------------------------------------------------
%% Trie
%%--------------------------------------------------------------------

-type(trie_node_id() :: binary() | atom()).

-record(trie_node, {
          node_id        :: trie_node_id(),
          edge_count = 0 :: non_neg_integer(),
          topic          :: binary() | undefined,
          flags          :: list(atom())
        }).

-record(trie_edge, {
          node_id :: trie_node_id(),
          word    :: binary() | atom()
        }).

-record(trie, {
          edge    :: #trie_edge{},
          node_id :: trie_node_id()
        }).

%%--------------------------------------------------------------------
%% Alarm
%%--------------------------------------------------------------------

-record(alarm, {
          id        :: binary(),
          severity  :: notice | warning | error | critical,
          title     :: iolist(),
          summary   :: iolist(),
          timestamp :: erlang:timestamp()
        }).

%%--------------------------------------------------------------------
%% Plugin
%%--------------------------------------------------------------------

-record(plugin, {
          name           :: atom(),
          version        :: string(),
          dir            :: string(),
          descr          :: string(),
          vendor         :: string(),
          active = false :: boolean(),
          info           :: map()
        }).

%%--------------------------------------------------------------------
%% Command
%%--------------------------------------------------------------------

-record(command, {
          name      :: atom(),
          action    :: atom(),
          args = [] :: list(),
          opts = [] :: list(),
          usage     :: string(),
          descr     :: string()
        }).

%%--------------------------------------------------------------------
%% Banned
%%--------------------------------------------------------------------
-type(banned_who() ::  {client_id,  binary()}
                     | {username,   binary()}
                     | {ip_address, inet:ip_address()}).

-record(banned, {
          who    :: banned_who(),
          reason :: binary(),
          by     :: binary(),
          desc   :: binary(),
          until  :: integer()
        }).

-endif.
