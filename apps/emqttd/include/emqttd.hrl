%%------------------------------------------------------------------------------
%% Copyright (c) 2012-2015, Feng Lee <feng@emqtt.io>
%% 
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to deal
%% in the Software without restriction, including without limitation the rights
%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%% 
%% The above copyright notice and this permission notice shall be included in all
%% copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%% SOFTWARE.
%%------------------------------------------------------------------------------
%%% @doc
%%% MQTT Broker Header.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% Banner
%%------------------------------------------------------------------------------
-define(COPYRIGHT, "Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>").

-define(LICENSE_MESSAGE, "Licensed under MIT"). 

-define(PROTOCOL_VERSION, "MQTT/3.1.1").

-define(ERTS_MINIMUM, "6.0").

%%------------------------------------------------------------------------------
%% PubSub
%%------------------------------------------------------------------------------
-type pubsub() :: publish | subscribe.

%%------------------------------------------------------------------------------
%% MQTT Topic
%%------------------------------------------------------------------------------
-record(mqtt_topic, {
    topic   :: binary(),
    node    :: node()
}).

-type mqtt_topic() :: #mqtt_topic{}.

%%------------------------------------------------------------------------------
%% MQTT Subscriber
%%------------------------------------------------------------------------------
-record(mqtt_subscriber, {
    topic    :: binary(),
    qos = 0  :: 0 | 1 | 2,
    pid      :: pid()
}).

-type mqtt_subscriber() :: #mqtt_subscriber{}.

%%------------------------------------------------------------------------------
%% MQTT Client
%%------------------------------------------------------------------------------
-record(mqtt_client, {
    clientid    :: binary(),
    username    :: binary() | undefined,
    ipaddr      :: inet:ip_address()
}).

-type mqtt_client() :: #mqtt_client{}.

%%------------------------------------------------------------------------------
%% MQTT Session
%%------------------------------------------------------------------------------
-record(mqtt_session, {
    clientid,
    session_pid,
    subscriptions = [],
    awaiting_ack,
    awaiting_rel
}).

-type mqtt_session() :: #mqtt_session{}.

%%------------------------------------------------------------------------------
%% MQTT Plugin
%%------------------------------------------------------------------------------
-record(mqtt_plugin, {
    name,
    version,
    attrs,
    description
}).

-type mqtt_plugin() :: #mqtt_plugin{}.


