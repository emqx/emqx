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

-module(emqx_types).

-include("emqx.hrl").
-include("types.hrl").

-export_type([zone/0]).

-export_type([ pubsub/0
             , topic/0
             , subid/0
             , subopts/0
             ]).

-export_type([ client_id/0
             , username/0
             , password/0
             , peername/0
             , protocol/0
             ]).

-export_type([ credentials/0
             , session/0
             ]).

-export_type([ subscription/0
             , subscriber/0
             , topic_table/0
             ]).

-export_type([ payload/0
             , message/0
             ]).

-export_type([ delivery/0
             , deliver_results/0
             ]).

-export_type([route/0]).

-export_type([ alarm/0
             , plugin/0
             , banned/0
             , command/0
             ]).

-type(zone() :: atom()).
-type(pubsub() :: publish | subscribe).
-type(topic() :: binary()).
-type(subid() :: binary() | atom()).
-type(subopts() :: #{qos    := emqx_mqtt_types:qos(),
                     share  => binary(),
                     atom() => term()
                    }).
-type(session() :: #session{}).
-type(client_id() :: binary() | atom()).
-type(username() :: maybe(binary())).
-type(password() :: maybe(binary())).
-type(peername() :: {inet:ip_address(), inet:port_number()}).
-type(auth_result() :: success
                     | client_identifier_not_valid
                     | bad_username_or_password
                     | bad_clientid_or_password
                     | not_authorized
                     | server_unavailable
                     | server_busy
                     | banned
                     | bad_authentication_method).
-type(protocol() :: mqtt | 'mqtt-sn' | coap | stomp | none | atom()).
-type(credentials() :: #{zone       := zone(),
                         client_id  := client_id(),
                         username   := username(),
                         sockname   := peername(),
                         peername   := peername(),
                         ws_cookie  := undefined | list(),
                         mountpoint := binary(),
                         password   => binary(),
                         auth_result => auth_result(),
                         anonymous => boolean(),
                         atom()    => term()
                        }).
-type(subscription() :: #subscription{}).
-type(subscriber() :: {pid(), subid()}).
-type(topic_table() :: [{topic(), subopts()}]).
-type(payload() :: binary() | iodata()).
-type(message() :: #message{}).
-type(banned() :: #banned{}).
-type(delivery() :: #delivery{}).
-type(deliver_results() :: [{route, node(), topic()} |
                            {dispatch, topic(), pos_integer()}]).
-type(route() :: #route{}).
-type(alarm() :: #alarm{}).
-type(plugin() :: #plugin{}).
-type(command() :: #command{}).
