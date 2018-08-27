%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% Banner
%%--------------------------------------------------------------------

-define(COPYRIGHT, "Copyright (c) 2018 EMQ Technologies Co., Ltd").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "10.0").

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

-type(pubsub() :: publish | subscribe).

-define(PS(I), (I =:= publish orelse I =:= subscribe)).

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System topic
-define(SYSTOP, <<"$SYS/">>).

%% Queue topic
-define(QUEUE,  <<"$queue/">>).

%% Shared topic
-define(SHARE,  <<"$share/">>).

%%--------------------------------------------------------------------
%% Topic, subscription and subscriber
%%--------------------------------------------------------------------

-type(topic() :: binary()).

-type(subid() :: binary() | atom()).

-type(subopts() :: #{qos    => integer(),
                     share  => binary(),
                     atom() => term()}).

-record(subscription, {
          topic   :: topic(),
          subid   :: subid(),
          subopts :: subopts()
        }).

-type(subscription() :: #subscription{}).

-type(subscriber() :: {pid(), subid()}).

-type(topic_table() :: [{topic(), subopts()}]).

%%--------------------------------------------------------------------
%% Client and Session
%%--------------------------------------------------------------------

-type(protocol() :: mqtt | 'mqtt-sn' | coap | stomp | none | atom()).

-type(peername() :: {inet:ip_address(), inet:port_number()}).

-type(client_id() :: binary() | atom()).

-type(username() :: binary() | atom()).

-type(zone() :: atom()).

-record(client, {
          id         :: client_id(),
          pid        :: pid(),
          zone       :: zone(),
          peername   :: peername(),
          username   :: username(),
          protocol   :: protocol(),
          attributes :: map()
         }).

-type(client() :: #client{}).

-record(session, {
          sid :: client_id(),
          pid :: pid()
         }).

-type(session() :: #session{}).

%%--------------------------------------------------------------------
%% Payload, Message and Delivery
%%--------------------------------------------------------------------

-type(qos() :: integer()).

-type(payload() :: binary() | iodata()).

-type(message_flag() :: dup | sys | retain | atom()).

%% See 'Application Message' in MQTT Version 5.0
-record(message, {
          %% Global unique message ID
          id :: binary() | pos_integer(),
          %% Message QoS
          qos = 0 :: qos(),
          %% Message from
          from :: atom() | client_id(),
          %% Message flags
          flags :: #{message_flag() => boolean()},
          %% Message headers, or MQTT 5.0 Properties
          headers = #{} :: map(),
          %% Topic that the message is published to
          topic :: topic(),
          %% Message Payload
          payload :: binary(),
          %% Timestamp
          timestamp :: erlang:timestamp()
        }).

-type(message() :: #message{}).

-record(delivery, {
          sender  :: pid(),     %% Sender of the delivery
          message :: message(), %% The message delivered
          flows   :: list()     %% The dispatch path of message
        }).

-type(delivery() :: #delivery{}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route, {
          topic :: topic(),
          dest :: node() | {binary(), node()}
         }).

-type(route() :: #route{}).

%%--------------------------------------------------------------------
%% Trie
%%--------------------------------------------------------------------

-type(trie_node_id() :: binary() | atom()).

-record(trie_node, {
          node_id        :: trie_node_id(),
          edge_count = 0 :: non_neg_integer(),
          topic          :: topic() | undefined,
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

-type(alarm() :: #alarm{}).

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

-type(plugin() :: #plugin{}).

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

-type(command() :: #command{}).

