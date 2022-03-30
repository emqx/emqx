%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_delayed_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-import(emqx_mgmt_api_test_util, [request_api/2, request_api/5, api_path/1, auth_header_/0]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    emqx_config:put([dealyed], #{enable => true, max_delayed_messages => 10}),
    meck:new(emqx_config, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_config,
        get_schema_mod,
        fun
            (delayed) -> emqx_conf_schema;
            (Any) -> meck:passthrough(Any)
        end
    ),

    ok = emqx_delayed:mnesia(boot),
    emqx_mgmt_api_test_util:init_suite([emqx_modules]),
    emqx_delayed:enable(),
    Config.

end_per_suite(Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_config),
    ok = emqx_delayed:disable(),
    emqx_mgmt_api_test_util:end_suite([emqx_modules]),
    Config.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    Config.

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------
t_status(_Config) ->
    Path = api_path(["mqtt", "delayed"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, R1} = request_api(
        put,
        Path,
        "",
        Auth,
        #{enable => false, max_delayed_messages => 10}
    ),
    ?assertMatch(#{enable := false, max_delayed_messages := 10}, decode_json(R1)),

    {ok, R2} = request_api(
        put,
        Path,
        "",
        Auth,
        #{enable => true, max_delayed_messages => 12}
    ),
    ?assertMatch(#{enable := true, max_delayed_messages := 12}, decode_json(R2)),

    {ok, ConfJson} = request_api(get, Path),
    ReturnConf = decode_json(ConfJson),
    ?assertMatch(#{enable := true, max_delayed_messages := 12}, ReturnConf).

t_messages(_) ->
    clear_all_record(),

    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    timer:sleep(500),

    Each = fun(I) ->
        Topic = list_to_binary(io_lib:format("$delayed/~B/msgs", [I + 60])),
        emqtt:publish(
            C1,
            Topic,
            <<"">>,
            [{qos, 0}, {retain, true}]
        )
    end,

    lists:foreach(Each, lists:seq(1, 5)),
    timer:sleep(500),

    Msgs = get_messages(5),
    [First | _] = Msgs,

    ?assertMatch(
        #{
            delayed_interval := _,
            delayed_remaining := _,
            expected_at := _,
            from_clientid := _,
            from_username := _,
            msgid := _,
            node := _,
            publish_at := _,
            qos := _,
            topic := <<"msgs">>
        },
        First
    ),

    MsgId = maps:get(msgid, First),
    {ok, LookupMsg} = request_api(
        get,
        api_path(["mqtt", "delayed", "messages", node(), MsgId])
    ),

    ?assertEqual(MsgId, maps:get(msgid, decode_json(LookupMsg))),

    {ok, _} = request_api(
        delete,
        api_path(["mqtt", "delayed", "messages", node(), MsgId])
    ),

    _ = get_messages(4),

    ok = emqtt:disconnect(C1).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------
decode_json(Data) ->
    BinJson = emqx_json:decode(Data, [return_maps]),
    emqx_map_lib:unsafe_atom_key_map(BinJson).

clear_all_record() ->
    ets:delete_all_objects(emqx_delayed).

get_messages(Len) ->
    {ok, MsgsJson} = request_api(get, api_path(["mqtt", "delayed", "messages"])),
    #{data := Msgs} = decode_json(MsgsJson),
    MsgLen = erlang:length(Msgs),
    ?assert(
        MsgLen =:= Len,
        lists:flatten(io_lib:format("message length is:~p~n", [MsgLen]))
    ),
    Msgs.
