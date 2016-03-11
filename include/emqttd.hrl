%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% MQTT Broker Header

%%--------------------------------------------------------------------
%% Banner
%%--------------------------------------------------------------------

-define(COPYRIGHT, "Copyright (C) 2012-2016, Feng Lee <feng@emqtt.io>").

-define(LICENSE_MESSAGE, "Licensed under the Apache License, Version 2.0").

-define(PROTOCOL_VERSION, "MQTT/3.1.1").

-define(ERTS_MINIMUM, "6.0").

%% System Topics.
-define(SYSTOP, <<"$SYS">>).

%% Queue Topics.
-define(QTop, <<"$Q">>).

%%--------------------------------------------------------------------
%% PubSub
%%--------------------------------------------------------------------

-type pubsub() :: publish | subscribe.

-define(IS_PUBSUB(PS), (PS =:= publish orelse PS =:= subscribe)).

%%--------------------------------------------------------------------
%% MQTT Topic
%%--------------------------------------------------------------------
-record(mqtt_topic, {
    topic   :: binary(),
    flags   :: [retained | static]
}).

-type mqtt_topic() :: #mqtt_topic{}.

%%--------------------------------------------------------------------
%% MQTT Subscription
%%--------------------------------------------------------------------
-record(mqtt_subscription, {
    subid   :: binary() | atom(),
    topic   :: binary(),
    qos = 0 :: 0 | 1 | 2
}).

-type mqtt_subscription() :: #mqtt_subscription{}.

%%--------------------------------------------------------------------
%% MQTT Route
%%--------------------------------------------------------------------
-record(mqtt_route, {
    topic   :: binary(),
    node    :: node()
}).

-type mqtt_route() :: #mqtt_route{}.

%%--------------------------------------------------------------------
%% MQTT Client
%%--------------------------------------------------------------------

-type ws_header_key() :: atom() | binary() | string().
-type ws_header_val() :: atom() | binary() | string() | integer().

-record(mqtt_client, {
    client_id     :: binary() | undefined,
    client_pid    :: pid(),
    username      :: binary() | undefined,
    peername      :: {inet:ip_address(), integer()},
    clean_sess    :: boolean(),
    proto_ver     :: 3 | 4,
    keepalive = 0,
    will_topic    :: undefined | binary(),
    ws_initial_headers :: list({ws_header_key(), ws_header_val()}),
    connected_at  :: erlang:timestamp()
}).

-type mqtt_client() :: #mqtt_client{}.

%%--------------------------------------------------------------------
%% MQTT Session
%%--------------------------------------------------------------------
-record(mqtt_session, {
    client_id   :: binary(),
    sess_pid    :: pid(),
    persistent  :: boolean()
}).

-type mqtt_session() :: #mqtt_session{}.

%%--------------------------------------------------------------------
%% MQTT Message
%%--------------------------------------------------------------------
-type mqtt_msgid() :: binary() | undefined.
-type mqtt_pktid() :: 1..16#ffff | undefined.

-record(mqtt_message, {
    msgid           :: mqtt_msgid(),          %% Global unique message ID
    pktid           :: mqtt_pktid(),          %% PacketId
    topic           :: binary(),              %% Topic that the message is published to
    from            :: binary() | atom(),     %% ClientId of the publisher
    sender          :: binary() | undefined,  %% Username of the publisher
    qos    = 0      :: 0 | 1 | 2,             %% Message QoS
    flags  = []     :: [retain | dup | sys],  %% Message Flags
    retain = false  :: boolean(),             %% Retain flag
    dup    = false  :: boolean(),             %% Dup flag
    sys    = false  :: boolean(),             %% $SYS flag
    payload         :: binary(),              %% Payload
    timestamp       :: erlang:timestamp()     %% os:timestamp
}).

-type mqtt_message() :: #mqtt_message{}.

%%--------------------------------------------------------------------
%% MQTT Alarm
%%--------------------------------------------------------------------
-record(mqtt_alarm, {
    id          :: binary(),
    severity    :: warning | error | critical,
    title       :: iolist() | binary(),
    summary     :: iolist() | binary(),
    timestamp   :: erlang:timestamp() %% Timestamp
}).

-type mqtt_alarm() :: #mqtt_alarm{}.

%%--------------------------------------------------------------------
%% MQTT Plugin
%%--------------------------------------------------------------------
-record(mqtt_plugin, {
    name,
    version,
    descr,
    config,
    active = false
}).

-type mqtt_plugin() :: #mqtt_plugin{}.

%%--------------------------------------------------------------------
%% MQTT CLI Command
%% For example: 'broker metrics'
%%--------------------------------------------------------------------
-record(mqtt_cli, {
    name,
    action,
    args = [],
    opts = [],
    usage,
    descr
}).

-type mqtt_cli() :: #mqtt_cli{}.

