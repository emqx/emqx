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

-module(emqx_retainer_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_retainer.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-import(emqx_mgmt_api_test_util, [request_api/2, request_api/5, api_path/1, auth_header_/0]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    emqx_retainer_SUITE:load_conf(),
    emqx_mgmt_api_test_util:init_suite([emqx_retainer, emqx_conf]),
    %% make sure no "$SYS/#" topics
    emqx_conf:update([sys_topics], raw_systopic_conf(), #{override_to => cluster}),
    Config.

end_per_suite(Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    emqx_mgmt_api_test_util:end_suite([emqx_retainer, emqx_conf]),
    Config.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    Config.

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_config(_Config) ->
    Path = api_path(["mqtt", "retainer"]),
    {ok, ConfJson} = request_api(get, Path),
    ReturnConf = decode_json(ConfJson),
    ?assertMatch(
        #{
            backend := _,
            enable := _,
            flow_control := _,
            max_payload_size := _,
            msg_clear_interval := _,
            msg_expiry_interval := _
        },
        ReturnConf
    ),

    UpdateConf = fun(Enable) ->
        RawConf = emqx_json:decode(ConfJson, [return_maps]),
        UpdateJson = RawConf#{<<"enable">> := Enable},
        {ok, UpdateResJson} = request_api(
            put,
            Path,
            [],
            auth_header_(),
            UpdateJson
        ),
        UpdateRawConf = emqx_json:decode(UpdateResJson, [return_maps]),
        ?assertEqual(Enable, maps:get(<<"enable">>, UpdateRawConf))
    end,

    UpdateConf(false),
    UpdateConf(true).

t_messages(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqx_retainer:clean(),

    Each = fun(I) ->
        emqtt:publish(
            C1,
            <<"retained/", (I + 60)>>,
            <<"retained">>,
            [{qos, 0}, {retain, true}]
        )
    end,

    ?check_trace(
        ?wait_async_action(
            lists:foreach(Each, lists:seq(1, 5)),
            #{?snk_kind := message_retained, topic := <<"retained/A">>},
            500
        ),
        []
    ),

    {ok, MsgsJson} = request_api(get, api_path(["mqtt", "retainer", "messages"])),
    #{data := Msgs, meta := _} = decode_json(MsgsJson),
    MsgLen = erlang:length(Msgs),
    ?assert(
        MsgLen =:= 5,
        io_lib:format("message length is:~p~n", [MsgLen])
    ),

    [First | _] = Msgs,
    ?assertMatch(
        #{
            msgid := _,
            topic := _,
            qos := _,
            publish_at := _,
            from_clientid := _,
            from_username := _
        },
        First
    ),

    ok = emqtt:disconnect(C1).

t_messages_page(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqx_retainer:clean(),

    Each = fun(I) ->
        emqtt:publish(
            C1,
            <<"retained/", (I + 60)>>,
            <<"retained">>,
            [{qos, 0}, {retain, true}]
        )
    end,

    ?check_trace(
        ?wait_async_action(
            lists:foreach(Each, lists:seq(1, 5)),
            #{?snk_kind := message_retained, topic := <<"retained/A">>},
            500
        ),
        []
    ),
    Page = 4,

    {ok, MsgsJson} = request_api(
        get,
        api_path([
            "mqtt", "retainer", "messages?page=" ++ erlang:integer_to_list(Page) ++ "&limit=1"
        ])
    ),
    #{data := Msgs, meta := #{page := Page, limit := 1}} = decode_json(MsgsJson),
    MsgLen = erlang:length(Msgs),
    ?assert(
        MsgLen =:= 1,
        io_lib:format("message length is:~p~n", [MsgLen])
    ),

    [OnlyOne] = Msgs,
    Topic = <<"retained/", (Page + 60)>>,
    ?assertMatch(
        #{
            msgid := _,
            topic := Topic,
            qos := _,
            publish_at := _,
            from_clientid := _,
            from_username := _
        },
        OnlyOne
    ),

    ok = emqtt:disconnect(C1).

t_lookup_and_delete(_) ->
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    emqx_retainer:clean(),
    timer:sleep(300),

    emqtt:publish(C1, <<"retained/api">>, <<"retained">>, [{qos, 0}, {retain, true}]),
    timer:sleep(300),

    API = api_path(["mqtt", "retainer", "message", "retained%2Fapi"]),
    {ok, LookupJson} = request_api(get, API),
    LookupResult = decode_json(LookupJson),

    ?assertMatch(
        #{
            msgid := _,
            topic := _,
            qos := _,
            payload := _,
            publish_at := _,
            from_clientid := _,
            from_username := _
        },
        LookupResult
    ),

    {ok, []} = request_api(delete, API),

    {error, {"HTTP/1.1", 404, "Not Found"}} = request_api(get, API),

    ok = emqtt:disconnect(C1).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------
decode_json(Data) ->
    BinJson = emqx_json:decode(Data, [return_maps]),
    emqx_map_lib:unsafe_atom_key_map(BinJson).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
raw_systopic_conf() ->
    #{
        <<"sys_event_messages">> =>
            #{
                <<"client_connected">> => false,
                <<"client_disconnected">> => false,
                <<"client_subscribed">> => false,
                <<"client_unsubscribed">> => false
            },
        <<"sys_heartbeat_interval">> => <<"1440m">>,
        <<"sys_msg_interval">> => <<"1440m">>
    }.
