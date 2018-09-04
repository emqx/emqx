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

-module(emqx_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

all() -> 
    [{group, connection}].

groups() ->
    [{connection, [sequence], [t_attrs]}].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.
    
end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().


t_attrs(_) ->
    {ok, C, _} = emqx_client:start_link([{host, "localhost"}, {client_id, <<"simpleClient">>}, {username, <<"plain">>}, {password, <<"plain">>}]),
    [{<<"simpleClient">>, ConnPid}] = emqx_cm:lookup_connection(<<"simpleClient">>),
    Attrs = emqx_connection:attrs(ConnPid),
    <<"simpleClient">> = proplists:get_value(client_id, Attrs),
    <<"plain">> = proplists:get_value(username, Attrs),
    emqx_client:disconnect(C).

%% t_stats() ->
%%     {ok, C, _ } = emqx_client;
%% t_stats() ->

