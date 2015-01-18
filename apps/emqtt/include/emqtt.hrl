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

%%------------------------------------------------------------------------------
%% Banner
%%------------------------------------------------------------------------------
-define(COPYRIGHT, "Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>").

-define(LICENSE_MESSAGE, "Licensed under MIT"). 

-define(PROTOCOL_VERSION, "MQTT/3.1.1").

-define(ERTS_MINIMUM, "6.0").

%%------------------------------------------------------------------------------
%% MQTT QoS
%%------------------------------------------------------------------------------

-define(QOS_0, 0).
-define(QOS_1, 1).
-define(QOS_2, 2).

-type mqtt_qos() :: ?QOS_2 | ?QOS_1 | ?QOS_0.

%%------------------------------------------------------------------------------
%% MQTT Client
%%------------------------------------------------------------------------------
-record(mqtt_client, {
    client_id,
    username
}).

-type mqtt_client() :: #mqtt_client{}.

%%------------------------------------------------------------------------------
%% MQTT Session
%%------------------------------------------------------------------------------
-record(mqtt_session, {
    client_id,
    session_pid,
    subscriptions = [],
    awaiting_ack,
    awaiting_rel
}).

-type mqtt_session() :: #mqtt_session{}.

%%------------------------------------------------------------------------------
%% MQTT Message
%%------------------------------------------------------------------------------
-record(mqtt_message, {
    msgid           :: integer() | undefined,
    qos    = ?QOS_0 :: mqtt_qos(),
    retain = false  :: boolean(),
    dup    = false  :: boolean(),
    topic           :: binary(),
    payload         :: binary()
}).

-type mqtt_message() :: #mqtt_message{}.

%%------------------------------------------------------------------------------
%% MQTT User Management
%%------------------------------------------------------------------------------
-record(mqtt_user, {
    username    :: binary(), 
    passwdhash    :: binary()
}).

%%------------------------------------------------------------------------------
%% MQTT Authorization
%%------------------------------------------------------------------------------

%%TODO: ClientId | Username --> Pub | Sub --> Topics


