%%--------------------------------------------------------------------
%% Copyright (c) 2017-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_routing_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

all() ->
    [
        {group, routing_schema_v1},
        {group, routing_schema_v2}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {routing_schema_v1, [], TCs},
        {routing_schema_v2, [], TCs}
    ].

init_per_group(GroupName, Config) ->
    WorkDir = filename:join([?config(priv_dir, Config), ?MODULE, GroupName]),
    NodeSpec = #{
        apps => [
            {emqx, #{
                config => mk_config(GroupName),
                after_start => fun() ->
                    % NOTE
                    % This one is actually defined on `emqx_conf_schema` level, but used
                    % in `emqx_broker`. Thus we have to resort to this ugly hack.
                    emqx_config:force_put([rpc, mode], async)
                end
            }}
        ]
    },
    NodeSpecs = [
        {emqx_routing_SUITE1, NodeSpec#{role => core}},
        {emqx_routing_SUITE2, NodeSpec#{role => core}},
        {emqx_routing_SUITE3, NodeSpec#{role => replicant}}
    ],
    Nodes = emqx_cth_cluster:start(NodeSpecs, #{work_dir => WorkDir}),
    [{cluster, Nodes}, Config].

end_per_group(_GroupName, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)).

mk_config(routing_schema_v1) ->
    "broker.routing.storage_schema = v1";
mk_config(routing_schema_v2) ->
    "broker.routing.storage_schema = v2".

t_cluster_routing(Config) ->
    Cluster = ?config(cluster, Config),
    Clients = [C1, C2, C3] = [start_client(N) || N <- Cluster],
    Commands = [
        {fun publish/3, [C1, <<"a/b/c">>, <<"wontsee">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]},
        {fun subscribe/2, [C3, <<"a/+/c/#">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"01">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]},
        {fun subscribe/2, [C1, <<"a/b/c">>]},
        {fun subscribe/2, [C2, <<"a/b/+">>]},
        {fun publish/3, [C3, <<"a/b/c">>, <<"02">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"03">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"04">>]},
        {fun subscribe/2, [C3, <<"a/b/d">>]},
        {fun publish/3, [C1, <<"a/b/d">>, <<"05">>]},
        {fun unsubscribe/2, [C3, <<"a/+/c/#">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"06">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"07">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"08">>]},
        {fun unsubscribe/2, [C2, <<"a/b/+">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"09">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"10">>]},
        {fun publish/3, [C2, <<"a/b/c/d">>, <<"11">>]},
        {fun unsubscribe/2, [C3, <<"a/b/d">>]},
        {fun unsubscribe/2, [C1, <<"a/b/c">>]},
        {fun publish/3, [C1, <<"a/b/c">>, <<"wontsee">>]},
        {fun publish/3, [C2, <<"a/b/d">>, <<"wontsee">>]}
    ],
    ok = lists:foreach(fun({F, Args}) -> erlang:apply(F, Args) end, Commands),
    _ = [emqtt:stop(C) || C <- Clients],
    Deliveries = ?drainMailbox(),
    ?assertMatch(
        [
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"06">>}},
            {pub, C1, #{topic := <<"a/b/c">>, payload := <<"09">>}},
            {pub, C2, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"03">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"05">>}},
            {pub, C2, #{topic := <<"a/b/c">>, payload := <<"06">>}},
            {pub, C2, #{topic := <<"a/b/d">>, payload := <<"07">>}},
            {pub, C3, #{topic := <<"a/b/c">>, payload := <<"01">>}},
            {pub, C3, #{topic := <<"a/b/c">>, payload := <<"02">>}},
            {pub, C3, #{topic := <<"a/b/c/d">>, payload := <<"04">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"05">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"07">>}},
            {pub, C3, #{topic := <<"a/b/d">>, payload := <<"10">>}}
        ],
        lists:sort(Deliveries)
    ).

start_client(Node) ->
    Self = self(),
    {ok, C} = emqtt:start_link(#{
        port => get_mqtt_tcp_port(Node),
        msg_handler => #{
            publish => fun(Msg) -> Self ! {pub, self(), Msg} end
        }
    }),
    {ok, _Props} = emqtt:connect(C),
    C.

publish(C, Topic, Payload) ->
    {ok, #{reason_code := 0}} = emqtt:publish(C, Topic, Payload, 1).

subscribe(C, Topic) ->
    % NOTE: sleeping here as lazy way to wait for subscribe to replicate
    {ok, _Props, [0]} = emqtt:subscribe(C, Topic),
    ok = timer:sleep(200).

unsubscribe(C, Topic) ->
    % NOTE: sleeping here as lazy way to wait for unsubscribe to replicate
    {ok, _Props, undefined} = emqtt:unsubscribe(C, Topic),
    ok = timer:sleep(200).

get_mqtt_tcp_port(Node) ->
    {_, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.
