%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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

%%--------------------------------------------------------------------
%% Banner
%%--------------------------------------------------------------------

-define(COPYRIGHT, "Copyright (c) 2013-2018 EMQ Inc.").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

-define(PROTOCOL_VERSION, "MQTT/5.0").

-define(ERTS_MINIMUM_REQUIRED, "9.2").

%%--------------------------------------------------------------------
%% Topics' prefix: $SYS | $queue | $share
%%--------------------------------------------------------------------

%% System Topic
-define(SYSTOP, <<"$SYS/">>).

%% Queue Topic
-define(QUEUE,  <<"$queue/">>).

%% Shared Topic
-define(SHARE,  <<"$share/">>).

%%--------------------------------------------------------------------
%% Topic, subscription and subscriber
%%--------------------------------------------------------------------

-type(qos() :: integer()).

-type(topic() :: binary()).

-type(suboption() :: {qos, qos()}
                   | {share, '$queue'}
                   | {share, binary()}
                   | {atom(), term()}).

-record(subscription,
        { subid   :: binary() | atom(),
          topic   :: topic(),
          subopts :: list(suboption())
        }).

-type(subscription() :: #subscription{}).

-type(subscriber() :: binary() | pid() | {binary(), pid()}).

%%--------------------------------------------------------------------
%% Client and session
%%--------------------------------------------------------------------

-type(protocol() :: mqtt | 'mqtt-sn' | coap | stomp | none | atom()).

-type(peername() :: {inet:ip_address(), inet:port_number()}).

-type(client_id() :: binary() | atom()).

-type(username() :: binary() | atom()).

-type(mountpoint() :: binary()).

-type(connector() :: atom()).

-type(zone() :: atom()).

-record(client,
        { client_id  :: client_id(),
          client_pid :: pid(),
          zone       :: zone(),
          node       :: node(),
          username   :: username(),
          peername   :: peername(),
          protocol   :: protocol(),
          connector  :: connector(),
          mountpoint :: mountpoint(),
          attributes :: #{atom() => term()}
        }).

-type(client() :: #client{}).

-record(session,
        { sid :: client_id(),
          pid :: pid()
        }).

-type(session() :: #session{}).

%%--------------------------------------------------------------------
%% Message and delivery
%%--------------------------------------------------------------------

-type(message_id() :: binary() | undefined).

-type(message_flag() :: sys | dup | retain | atom()).

-type(message_flags() :: #{message_flag() => boolean()}).

-type(message_headers() :: #{packet_id => pos_integer(),
                             priority  => non_neg_integer(),
                             ttl       => pos_integer(),
                             atom()    => term()}).

-type(payload() :: binary()).

%% See 'Application Message' in MQTT Version 5.0
-record(message,
        { id         :: message_id(),      %% Global unique id
          qos        :: qos(),             %% Message QoS
          from       :: atom() | client(), %% Message from
          sender     :: pid(),             %% The pid of the sender/publisher
          flags      :: message_flags(),   %% Message flags
          headers    :: message_headers(), %% Message headers
          topic      :: binary(),          %% Message topic
          properties :: map(),             %% Message user properties
          payload    :: payload(),         %% Message payload
          timestamp  :: erlang:timestamp() %% Timestamp
        }).

-type(message() :: #message{}).

-record(delivery,
        { message :: message(),
          flows   :: list()
        }).

-type(delivery() :: #delivery{}).

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

-type(pubsub() :: publish | subscribe).

-define(PS(I), (I =:= publish orelse I =:= subscribe)).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route,
        { topic :: topic(),
          dest  :: {binary(), node()} | node()
        }).

-type(route() :: #route{}).

%%--------------------------------------------------------------------
%% Trie
%%--------------------------------------------------------------------

-type(trie_node_id() :: binary() | atom()).

-record(trie_node,
        { node_id        :: trie_node_id(),
          edge_count = 0 :: non_neg_integer(),
          topic          :: binary() | undefined,
          flags          :: list(atom())
        }).

-record(trie_edge,
        { node_id :: trie_node_id(),
          word    :: binary() | atom()
        }).

-record(trie,
        { edge    :: #trie_edge{},
          node_id :: trie_node_id()
        }).

%%--------------------------------------------------------------------
%% Alarm
%%--------------------------------------------------------------------

-record(alarm,
        { id        :: binary(),
          severity  :: notice | warning | error | critical,
          title     :: iolist() | binary(),
          summary   :: iolist() | binary(),
          timestamp :: erlang:timestamp()
        }).

-type(alarm() :: #alarm{}).

%%--------------------------------------------------------------------
%% Plugin
%%--------------------------------------------------------------------

-record(plugin,
        { name    :: atom(),
          version :: string(),
          dir     :: string(),
          descr   :: string(),
          vendor  :: string(),
          active  :: boolean(),
          info    :: map()
        }).

-type(plugin() :: #plugin{}).

%%--------------------------------------------------------------------
%% Command
%%--------------------------------------------------------------------

-record(command,
        { name,
          action,
          args = [],
          opts = [],
          usage,
          descr
        }).

-type(command() :: #command{}).

