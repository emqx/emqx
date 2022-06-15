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

-define(BASE_CONF, #{
    <<"dealyed">> => <<"true">>,
    <<"max_delayed_messages">> => <<"0">>
}).

-import(emqx_dashboard_api_test_helpers, [request/2, request/3, uri/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_modules_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),

    ok = emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_modules, emqx_dashboard],
        fun set_special_configs/1
    ),
    emqx_delayed:load(),
    Config.

end_per_suite(Config) ->
    ok = emqx_delayed:unload(),
    emqx_common_test_helpers:stop_apps([emqx_conf, emqx_dashboard, emqx_modules]),
    Config.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(),
    Config.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------
t_status(_Config) ->
    Path = uri(["mqtt", "delayed"]),
    {ok, 200, R1} = request(
        put,
        Path,
        #{enable => false, max_delayed_messages => 10}
    ),
    ?assertMatch(#{enable := false, max_delayed_messages := 10}, decode_json(R1)),

    {ok, 200, R2} = request(
        put,
        Path,
        #{enable => true, max_delayed_messages => 12}
    ),
    ?assertMatch(#{enable := true, max_delayed_messages := 12}, decode_json(R2)),

    ?assertMatch(
        {ok, 200, _},
        request(
            put,
            Path,
            #{enable => true}
        )
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            put,
            Path,
            #{enable => true, max_delayed_messages => -5}
        )
    ),

    {ok, 200, ConfJson} = request(get, Path),
    ReturnConf = decode_json(ConfJson),
    ?assertMatch(#{enable := true, max_delayed_messages := 12}, ReturnConf).

t_messages(_) ->
    clear_all_record(),
    emqx_delayed:load(),

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
    timer:sleep(1000),

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
    {ok, 200, LookupMsg} = request(
        get,
        uri(["mqtt", "delayed", "messages", node(), MsgId])
    ),

    ?assertEqual(MsgId, maps:get(msgid, decode_json(LookupMsg))),

    ?assertMatch(
        {ok, 404, _},
        request(
            get,
            uri(["mqtt", "delayed", "messages", node(), emqx_guid:to_hexstr(emqx_guid:gen())])
        )
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            get,
            uri(["mqtt", "delayed", "messages", node(), "invalid_msg_id"])
        )
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            get,
            uri(["mqtt", "delayed", "messages", atom_to_list('unknownnode@127.0.0.1'), MsgId])
        )
    ),

    ?assertMatch(
        {ok, 400, _},
        request(
            get,
            uri(["mqtt", "delayed", "messages", "some_unknown_atom", MsgId])
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            delete,
            uri(["mqtt", "delayed", "messages", node(), emqx_guid:to_hexstr(emqx_guid:gen())])
        )
    ),

    ?assertMatch(
        {ok, 204, _},
        request(
            delete,
            uri(["mqtt", "delayed", "messages", node(), MsgId])
        )
    ),

    _ = get_messages(4),

    ok = emqtt:disconnect(C1).

t_large_payload(_) ->
    clear_all_record(),
    emqx_delayed:load(),

    {ok, C1} = emqtt:start_link([{clean_start, true}]),
    {ok, _} = emqtt:connect(C1),
    timer:sleep(500),
    Topic = <<"$delayed/123/msgs">>,
    emqtt:publish(
        C1,
        Topic,
        iolist_to_binary([<<"x">> || _ <- lists:seq(1, 5000)]),
        [{qos, 0}, {retain, true}]
    ),

    timer:sleep(1000),

    [#{msgid := MsgId}] = get_messages(1),

    {ok, 200, Msg} = request(
        get,
        uri(["mqtt", "delayed", "messages", node(), MsgId])
    ),

    ?assertMatch(
        #{
            payload := <<"PAYLOAD_TOO_LARGE">>,
            topic := <<"msgs">>
        },
        decode_json(Msg)
    ).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

decode_json(Data) ->
    BinJson = emqx_json:decode(Data, [return_maps]),
    emqx_map_lib:unsafe_atom_key_map(BinJson).

clear_all_record() ->
    ets:delete_all_objects(emqx_delayed).

get_messages(Len) ->
    {ok, 200, MsgsJson} = request(get, uri(["mqtt", "delayed", "messages"])),
    #{data := Msgs} = decode_json(MsgsJson),
    MsgLen = erlang:length(Msgs),
    ?assertEqual(
        Len,
        MsgLen,
        lists:flatten(
            io_lib:format("message length is:~p~nWhere:~p~nHooks:~p~n", [
                MsgLen, erlang:whereis(emqx_delayed), ets:tab2list(emqx_hooks)
            ])
        )
    ),
    Msgs.
