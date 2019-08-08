%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_protocol,
        [ handle_in/2
        , handle_out/2
        ]).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Test cases for handle_in
%%--------------------------------------------------------------------

t_handle_in_connect(_) ->
    'TODO'.

t_handle_in_publish(_) ->
    'TODO'.

t_handle_in_puback(_) ->
    'TODO'.

t_handle_in_pubrec(_) ->
    'TODO'.

t_handle_in_pubrel(_) ->
    'TODO'.

t_handle_in_pubcomp(_) ->
    'TODO'.

t_handle_in_subscribe(_) ->
    'TODO'.

t_handle_in_unsubscribe(_) ->
    'TODO'.

t_handle_in_pingreq(_) ->
    with_proto(fun(PState) ->
                       {ok, ?PACKET(?PINGRESP), PState} = handle_in(?PACKET(?PINGREQ), PState)
               end).

t_handle_in_disconnect(_) ->
    'TODO'.

t_handle_in_auth(_) ->
    'TODO'.

%%--------------------------------------------------------------------
%% Test cases for handle_deliver
%%--------------------------------------------------------------------

t_handle_deliver(_) ->
    'TODO'.

%%--------------------------------------------------------------------
%% Test cases for handle_out
%%--------------------------------------------------------------------

t_handle_out_conack(_) ->
    'TODO'.

t_handle_out_publish(_) ->
    'TODO'.

t_handle_out_puback(_) ->
    'TODO'.

t_handle_out_pubrec(_) ->
    'TODO'.

t_handle_out_pubrel(_) ->
    'TODO'.

t_handle_out_pubcomp(_) ->
    'TODO'.

t_handle_out_suback(_) ->
    'TODO'.

t_handle_out_unsuback(_) ->
    'TODO'.

t_handle_out_disconnect(_) ->
    'TODO'.

t_handle_out_auth(_) ->
    'TODO'.

%%--------------------------------------------------------------------
%% Test cases for handle_timeout
%%--------------------------------------------------------------------

t_handle_timeout(_) ->
    'TODO'.

%%--------------------------------------------------------------------
%% Test cases for terminate
%%--------------------------------------------------------------------

t_terminate(_) ->
    'TODO'.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

with_proto(Fun) ->
    Fun(emqx_protocol:init(#{peername => {{127,0,0,1}, 3456},
                             sockname => {{127,0,0,1}, 1883},
                             conn_mod => emqx_channel},
                           #{zone => ?MODULE})).

