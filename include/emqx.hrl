%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. All Rights Reserved.
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

-define(COPYRIGHT, "Copyright (c) 2013-2018 EMQ Enterprise, Inc.").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

-define(PROTOCOL_VERSION, "MQTT/5.0").

%%-define(ERTS_MINIMUM, "9.0").

%%--------------------------------------------------------------------
%% Sys/Queue/Share Topics' Prefix
%%--------------------------------------------------------------------

-define(SYSTOP, <<"$SYS/">>).   %% System Topic

-define(QUEUE,  <<"$queue/">>). %% Queue Topic

-define(SHARE,  <<"$share/">>). %% Shared Topic

%%--------------------------------------------------------------------
%% Client and Session
%%--------------------------------------------------------------------

-type(topic() :: binary()).

-type(subscriber() :: pid() | binary() | {binary(), pid()}).

-type(suboption() :: {qos, non_neg_integer()} | {share, {'$queue' | binary()}}).

-type(client_id() :: binary() | atom()).

-type(protocol() :: mqtt | 'mqtt-sn' | coap | stomp | atom()).

-type(client() :: #{zone      := atom(),
                    node      := atom(),
                    clientid  := client_id(),
                    protocol  := protocol(),
                    connector => atom(),
                    peername  => {inet:ip_address(), inet:port_number()},
                    username  => binary(),
                    atom()    => term()}).

-type(session() :: #{client_id       := client_id(),
                     clean_start     := boolean(),
                     expiry_interval := non_neg_integer()}).

-record(session, {client_id, sess_pid}).

%%--------------------------------------------------------------------
%% Message and Delivery
%%--------------------------------------------------------------------

-type(message_id() :: binary() | undefined).

%% -type(protocol() :: mqtt | 'mqtt-sn' | coap | stomp | atom()).

-type(message_from() :: #{zone      := atom(),
                          node      := atom(),
                          clientid  := binary(),
                          protocol  := protocol(),
                          connector => atom(),
                          peername  => {inet:ip_address(), inet:port_number()},
                          username  => binary(),
                          atom()    => term()}).

-type(message_flags() :: #{dup     => boolean(), %% Dup flag
                           qos     => 0 | 1 | 2, %% QoS
                           sys     => boolean(), %% $SYS flag
                           retain  => boolean(), %% Retain flag
                           durable => boolean(), %% Durable flag
                           atom()  => boolean()}).

-type(message_headers() :: #{packet_id => pos_integer(),
                             priority  => pos_integer(),
                             expiry    => integer(), %% Time to live
                             atom()    => term()}).

%% See 'Application Message' in MQTT Version 5.0
-record(message,
        { id         :: message_id(),      %% Global unique id
          from       :: message_from(),    %% Message from
          sender     :: pid(),             %% The pid of the sender/publisher
          flags      :: message_flags(),   %% Message flags
          headers    :: message_headers(), %% Message headers
          topic      :: binary(),          %% Message topic
          properties :: map(),             %% Message user properties
          payload    :: binary(),          %% Message payload
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

-define(PS(PS), (PS =:= publish orelse PS =:= subscribe)).

%%--------------------------------------------------------------------
%% Subscription
%%--------------------------------------------------------------------

-record(subscription,
        { subid   :: binary() | atom(),
          topic   :: binary(),
          subopts :: list()
        }).

-type(subscription() :: #subscription{}).

%%--------------------------------------------------------------------
%% MQTT Client
%%--------------------------------------------------------------------

-type(ws_header_key() :: atom() | binary() | string()).
-type(ws_header_val() :: atom() | binary() | string() | integer()).

-record(mqtt_client,
        { client_id     :: binary() | undefined,
          client_pid    :: pid(),
          username      :: binary() | undefined,
          peername      :: {inet:ip_address(), inet:port_number()},
          clean_sess    :: boolean(),
          proto_ver     :: 3 | 4,
          keepalive = 0,
          will_topic    :: undefined | binary(),
          ws_initial_headers :: list({ws_header_key(), ws_header_val()}),
          mountpoint    :: undefined | binary(),
          connected_at  :: erlang:timestamp(),
          %%TODO: Headers
          headers = []  :: list()
        }).

-type(mqtt_client() :: #mqtt_client{}).

%%--------------------------------------------------------------------
%% MQTT Session
%%--------------------------------------------------------------------

-record(mqtt_session,
        { client_id  :: binary(),
          sess_pid   :: pid(),
          clean_sess :: boolean()
        }).

-type(mqtt_session() :: #mqtt_session{}).

%%--------------------------------------------------------------------
%% Route
%%--------------------------------------------------------------------

-record(route, { topic :: binary(), dest :: {binary(), node()} | node() }).

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

-record(plugin, { name, version, descr, active = false }).

-type(plugin() :: #plugin{}).

%%--------------------------------------------------------------------
%% Command
%%--------------------------------------------------------------------

-record(command, { name, action, args = [], opts = [], usage, descr }).

-type(command() :: #command{}).

