%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/asserts.hrl").

-import(emqx_mgmt_api_test_util, [
    request_api/2, request_api/4, request_api/5, api_path/1, auth_header_/0
]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx,
            emqx_retainer,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    _ = emqx_common_test_http:create_default_app(),
    %% make sure no "$SYS/#" topics
    _ = emqx_conf:update([sys_topics], raw_systopic_conf(), #{override_to => cluster}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_common_test_http:delete_default_app(),
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_, Config) ->
    snabbkaffe:start_trace(),
    emqx_retainer:clean(),
    {ok, C} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),
    [{client, C} | Config].

end_per_testcase(_, Config) ->
    ok = emqtt:disconnect(?config(client, Config)),
    snabbkaffe:stop(),
    ok.

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
            max_payload_size := _,
            msg_clear_interval := _,
            msg_expiry_interval := _
        },
        ReturnConf
    ),

    UpdateConf = fun(Enable) ->
        RawConf = emqx_utils_json:decode(ConfJson, [return_maps]),
        UpdateJson = RawConf#{<<"enable">> := Enable},
        {ok, UpdateResJson} = request_api(
            put,
            Path,
            [],
            auth_header_(),
            UpdateJson
        ),
        UpdateRawConf = emqx_utils_json:decode(UpdateResJson, [return_maps]),
        ?assertEqual(Enable, maps:get(<<"enable">>, UpdateRawConf))
    end,

    UpdateConf(false),
    UpdateConf(true).

t_messages1(Config) ->
    C = ?config(client, Config),

    Each = fun(I) ->
        emqtt:publish(
            C,
            <<"retained/", (I + 60)>>,
            <<"retained">>,
            [{qos, 0}, {retain, true}]
        )
    end,

    ?assertWaitEvent(
        lists:foreach(Each, lists:seq(1, 5)),
        #{?snk_kind := message_retained, topic := <<"retained/A">>},
        500
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
    ).

t_messages2(Config) ->
    C = ?config(client, Config),

    ok = lists:foreach(
        fun(Topic) ->
            ?assertWaitEvent(
                emqtt:publish(C, Topic, <<"retained">>, [{qos, 0}, {retain, true}]),
                #{?snk_kind := message_retained, topic := Topic},
                500
            )
        end,
        [<<"c">>, <<"c/1">>, <<"c/1/1">>]
    ),

    {ok, MsgsJson} = request_api(get, api_path(["mqtt", "retainer", "messages?topic=c"])),

    #{data := Msgs, meta := _} = decode_json(MsgsJson),

    ?assertEqual(1, length(Msgs)).

t_message1(Config) ->
    C = ?config(client, Config),

    ?assertWaitEvent(
        emqtt:publish(C, <<"c/1">>, <<"retained">>, [{qos, 0}, {retain, true}]),
        #{?snk_kind := message_retained, topic := <<"c/1">>},
        500
    ),

    ?assertMatch(
        {error, {_, 404, _}},
        request_api(
            get,
            api_path(["mqtt", "retainer", "message", "c"])
        )
    ),

    {ok, Json} =
        request_api(
            get,
            api_path(["mqtt", "retainer", "message", "c%2F1"])
        ),

    ?assertMatch(
        #{
            topic := <<"c/1">>,
            payload := <<"cmV0YWluZWQ=">>
        },
        decode_json(Json)
    ).

t_message2(Config) ->
    C = ?config(client, Config),

    ?assertWaitEvent(
        emqtt:publish(C, <<"c">>, <<"retained">>, [{qos, 0}, {retain, true}]),
        #{?snk_kind := message_retained, topic := <<"c">>},
        500
    ),

    ?assertMatch(
        {error, {_, 404, _}},
        request_api(
            get,
            api_path(["mqtt", "retainer", "message", "c%2F%2B"])
        )
    ),

    {ok, Json0} =
        request_api(
            get,
            api_path(["mqtt", "retainer", "message", "c"])
        ),

    ?assertMatch(
        #{
            topic := <<"c">>,
            payload := <<"cmV0YWluZWQ=">>
        },
        decode_json(Json0)
    ),

    {ok, Json1} =
        request_api(
            get,
            api_path(["mqtt", "retainer", "message", "c%2F%23"])
        ),

    ?assertMatch(
        #{
            topic := <<"c">>,
            payload := <<"cmV0YWluZWQ=">>
        },
        decode_json(Json1)
    ).

t_messages_page(Config) ->
    C = ?config(client, Config),

    Each = fun(I) ->
        emqtt:publish(
            C,
            <<"retained/", (I + 60)>>,
            <<"retained">>,
            [{qos, 0}, {retain, true}]
        )
    end,

    ?assertWaitEvent(
        lists:foreach(Each, lists:seq(1, 5)),
        #{?snk_kind := message_retained, topic := <<"retained/A">>},
        500
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
    ).

t_lookup_and_delete(Config) ->
    C = ?config(client, Config),
    timer:sleep(300),

    emqtt:publish(C, <<"retained/api">>, <<"retained">>, [{qos, 0}, {retain, true}]),
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
    {error, {"HTTP/1.1", 404, "Not Found"}} = request_api(delete, API).

t_change_storage_type(_Config) ->
    Path = api_path(["mqtt", "retainer"]),
    {ok, ConfJson} = request_api(get, Path),
    RawConf = emqx_utils_json:decode(ConfJson, [return_maps]),
    %% pre-conditions
    ?assertMatch(
        #{
            <<"backend">> := #{
                <<"type">> := <<"built_in_database">>,
                <<"storage_type">> := <<"ram">>
            },
            <<"enable">> := true
        },
        RawConf
    ),
    ?assertEqual(ram_copies, mnesia:table_info(?TAB_INDEX_META, storage_type)),
    ?assertEqual(ram_copies, mnesia:table_info(?TAB_MESSAGE, storage_type)),
    ?assertEqual(ram_copies, mnesia:table_info(?TAB_INDEX, storage_type)),
    %% insert some retained messages
    {ok, C0} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C0),
    ok = snabbkaffe:start_trace(),
    Topic = <<"retained">>,
    Payload = <<"retained">>,
    {ok, {ok, _}} =
        ?wait_async_action(
            emqtt:publish(C0, Topic, Payload, [{qos, 0}, {retain, true}]),
            #{?snk_kind := message_retained, topic := Topic},
            500
        ),
    emqtt:stop(C0),
    ok = snabbkaffe:stop(),
    {ok, MsgsJson0} = request_api(get, api_path(["mqtt", "retainer", "messages"])),
    #{data := Msgs0, meta := _} = decode_json(MsgsJson0),
    ?assertEqual(1, length(Msgs0)),

    ChangedConf = emqx_utils_maps:deep_merge(
        RawConf,
        #{
            <<"backend">> =>
                #{<<"storage_type">> => <<"disc">>}
        }
    ),
    {ok, UpdateResJson} = request_api(
        put,
        Path,
        [],
        auth_header_(),
        ChangedConf
    ),
    UpdatedRawConf = emqx_utils_json:decode(UpdateResJson, [return_maps]),
    ?assertMatch(
        #{
            <<"backend">> := #{
                <<"type">> := <<"built_in_database">>,
                <<"storage_type">> := <<"disc">>
            },
            <<"enable">> := true
        },
        UpdatedRawConf
    ),
    ?assertEqual(disc_copies, mnesia:table_info(?TAB_INDEX_META, storage_type)),
    ?assertEqual(disc_copies, mnesia:table_info(?TAB_MESSAGE, storage_type)),
    ?assertEqual(disc_copies, mnesia:table_info(?TAB_INDEX, storage_type)),
    %% keep retained messages
    {ok, MsgsJson1} = request_api(get, api_path(["mqtt", "retainer", "messages"])),
    #{data := Msgs1, meta := _} = decode_json(MsgsJson1),
    ?assertEqual(1, length(Msgs1)),
    {ok, C1} = emqtt:start_link([{clean_start, true}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, Topic),

    receive
        {publish, #{topic := T, payload := P, retain := R}} ->
            ?assertEqual(Payload, P),
            ?assertEqual(Topic, T),
            ?assert(R),
            ok
    after 500 ->
        emqtt:stop(C1),
        ct:fail("should have preserved retained messages")
    end,
    emqtt:stop(C1),

    ok.

t_match_and_clean(Config) ->
    C = ?config(client, Config),
    timer:sleep(300),

    _ = [
        emqtt:publish(C, <<P/binary, "/", S/binary>>, <<"retained">>, [{qos, 0}, {retain, true}])
     || P <- [<<"t">>, <<"f">>], S <- [<<"1">>, <<"2">>, <<"3">>]
    ],

    timer:sleep(1000),

    API = api_path(["mqtt", "retainer", "messages"]),
    {ok, LookupJson} = request_api(get, API, "topic=t/%2B", auth_header_()),
    LookupResult = decode_json(LookupJson),

    Expected = lists:usort([<<"t/1">>, <<"t/2">>, <<"t/3">>]),
    ?assertMatch(
        Expected,
        lists:usort([Topic || #{topic := Topic} <- maps:get(data, LookupResult)])
    ),

    CleanAPI = api_path(["mqtt", "retainer", "messages"]),
    {ok, []} = request_api(delete, CleanAPI),

    {ok, LookupJson2} = request_api(get, API),
    ?assertMatch(#{data := []}, decode_json(LookupJson2)).

%% Checks that we can see `$SYS' messages in the API.
%% https://emqx.atlassian.net/browse/EMQX-13399
t_retained_sys_messages(_Config) ->
    Msg0 = emqx_message:make(emqx_sys, <<"$SYS/brokers">>, atom_to_binary(node())),
    Msg = emqx_message:set_flags(#{sys => true, retain => true}, Msg0),
    _ = emqx:publish(Msg),
    API = api_path(["mqtt", "retainer", "messages"]),
    {ok, LookupJson} = request_api(get, API, "", auth_header_()),
    ?assertMatch(
        #{
            data := [_ | _],
            meta := #{count := N}
        } when N > 0,
        decode_json(LookupJson)
    ),
    ok.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

decode_json(Data) ->
    BinJson = emqx_utils_json:decode(Data, [return_maps]),
    emqx_utils_maps:unsafe_atom_key_map(BinJson).

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
