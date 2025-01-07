%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_trace_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/zip.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

t_http_test(_Config) ->
    emqx_trace:clear(),
    load(),
    %% list
    {ok, Empty} = request_api(get, api_path("trace")),
    ?assertEqual([], json(Empty)),
    %% create
    ErrorTrace = #{},
    Opts = #{return_all => true},
    {error, {{"HTTP/1.1", 400, "Bad Request"}, _, Body}} =
        emqx_mgmt_api_test_util:request_api(post, api_path("trace"), [], [], ErrorTrace, Opts),
    ?assertMatch(#{<<"code">> := <<"BAD_REQUEST">>}, json(Body)),

    Name = <<"test-name">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create)),

    {ok, List} = request_api(get, api_path("trace")),
    [Data] = json(List),
    ?assertEqual(Name, maps:get(<<"name">>, Data)),

    %% update
    {ok, Update} = request_api(put, api_path("trace/test-name/stop"), #{}),
    ?assertEqual(
        #{
            <<"enable">> => false,
            <<"name">> => <<"test-name">>
        },
        json(Update)
    ),

    ?assertMatch(
        {error, {"HTTP/1.1", 404, _}},
        request_api(put, api_path("trace/test-name-not-found/stop"), #{})
    ),
    {ok, List1} = request_api(get, api_path("trace")),
    [Data1] = json(List1),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"status">> := <<"stopped">>,
            <<"name">> := <<"test-name">>,
            <<"log_size">> := #{Node := _},
            <<"start_at">> := _,
            <<"end_at">> := _,
            <<"type">> := <<"topic">>,
            <<"topic">> := <<"/x/y/z">>
        },
        Data1
    ),

    %% delete
    {ok, Delete} = request_api(delete, api_path("trace/test-name")),
    ?assertEqual(<<>>, Delete),

    {error, {{"HTTP/1.1", 404, "Not Found"}, _, DeleteNotFound}} =
        emqx_mgmt_api_test_util:request_api(delete, api_path("trace/test-name"), [], [], [], Opts),
    ?assertEqual(
        #{
            <<"code">> => <<"NOT_FOUND">>,
            <<"message">> => <<"test-name NOT FOUND">>
        },
        json(DeleteNotFound)
    ),

    {ok, List2} = request_api(get, api_path("trace")),
    ?assertEqual([], json(List2)),

    %% clear
    {ok, Create1} = request_api(post, api_path("trace"), Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create1)),

    {ok, Clear} = request_api(delete, api_path("trace")),
    ?assertEqual(<<>>, Clear),

    unload(),
    ok.

t_http_test_rule_trace(_Config) ->
    emqx_trace:clear(),
    load(),
    %% create
    Name = atom_to_binary(?FUNCTION_NAME),
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"ruleid">>},
        {<<"ruleid">>, Name}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create)),

    {ok, List} = request_api(get, api_path("trace")),
    [Data] = json(List),
    ?assertEqual(Name, maps:get(<<"name">>, Data)),

    %% update
    {ok, Update} = request_api(put, api_path(iolist_to_binary(["trace/", Name, "/stop"])), #{}),
    ?assertEqual(
        #{
            <<"enable">> => false,
            <<"name">> => Name
        },
        json(Update)
    ),
    {ok, List1} = request_api(get, api_path("trace")),
    [Data1] = json(List1),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"status">> := <<"stopped">>,
            <<"name">> := Name,
            <<"log_size">> := #{Node := _},
            <<"start_at">> := _,
            <<"end_at">> := _,
            <<"type">> := <<"ruleid">>,
            <<"ruleid">> := Name
        },
        Data1
    ),

    %% delete
    {ok, Delete} = request_api(delete, api_path(["trace/", Name])),
    ?assertEqual(<<>>, Delete),

    emqx_trace:clear(),
    unload(),
    ok.

t_http_test_json_formatter(_Config) ->
    emqx_trace:clear(),
    load(),

    Name = <<"testname">>,
    Topic = <<"/x/y/z">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, Topic},
        {<<"formatter">>, <<"json">>}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create)),

    {ok, List} = request_api(get, api_path("trace")),
    [Data] = json(List),
    ?assertEqual(<<"json">>, maps:get(<<"formatter">>, Data)),

    {ok, List1} = request_api(get, api_path("trace")),
    [Data1] = json(List1),
    ?assertMatch(
        #{
            <<"formatter">> := <<"json">>
        },
        Data1
    ),

    %% Check that the log is empty
    ok = emqx_trace_handler_SUITE:filesync(Name, topic),
    {ok, _Detail} = request_api(get, api_path("trace/" ++ binary_to_list(Name) ++ "/log_detail")),
    %% Trace is empty which results in a not found error
    {error, _} = request_api(get, api_path("trace/" ++ binary_to_list(Name) ++ "/download")),

    %% Start a client and send a message to get info to the log
    ClientId = <<"my_client_id">>,
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    %% Normal message
    emqtt:publish(Client, Topic, #{}, <<"log_this_message">>, [{qos, 2}]),
    %% Escape line breaks
    emqtt:publish(Client, Topic, #{}, <<"\nlog\nthis\nmessage">>, [{qos, 2}]),
    %% Escape escape character
    emqtt:publish(Client, Topic, #{}, <<"\\\nlog\n_\\n_this\nmessage\\">>, [{qos, 2}]),
    %% Escape end of string
    emqtt:publish(Client, Topic, #{}, <<"\"log_this_message\"">>, [{qos, 2}]),

    %% Manually create some trace messages to test the JSON formatter

    %% String key and value
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, "str" => "str"}),
    %% Log Erlang term
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, term => {notjson}}),
    %% Log Erlang term key
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, {'notjson'} => term}),
    %% Log Integer
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, integer => 42}),
    %% Log Float
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, float => 1.2}),
    %% Log Integer Key
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, 42 => integer}),
    %% Log Float Key
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, 1.2 => float}),
    %% Log Map Key
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, #{} => value}),
    %% Empty submap
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, sub => #{}}),
    %% Non-empty submap
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, sub => #{key => value}}),
    %% Bolean values
    ?TRACE("CUSTOM", "my_log_msg", #{topic => Topic, true => true, false => false}),
    %% Key value list
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        list => [
            {<<"key">>, <<"value">>},
            {<<"key2">>, <<"value2">>}
        ]
    }),
    %% We do special formatting for client_ids and rule_ids
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        client_ids => maps:from_keys([<<"a">>, <<"b">>, <<"c">>], true)
    }),
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        rule_ids => maps:from_keys([<<"a">>, <<"b">>, <<"c">>], true)
    }),
    %% action_id should be rendered as action_info
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        action_id =>
            <<"action:http:emqx_bridge_http_test_lib:connector:http:emqx_bridge_http_test_lib">>
    }),
    %% We should handle report style logging
    ?SLOG(error, #{msg => "recursive_republish_detected"}, #{topic => Topic}),
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        %% This will be converted to map
        map_key => [{a, a}, {b, b}]
    }),
    ?TRACE("CUSTOM", "my_log_msg", #{
        topic => Topic,
        %% We should not convert this to a map as we will lose information
        map_key => [{a, a}, {a, b}]
    }),
    ok = emqx_trace_handler_SUITE:filesync(Name, topic),
    {ok, _Detail2} = request_api(get, api_path("trace/" ++ binary_to_list(Name) ++ "/log_detail")),
    {ok, Bin} = request_api(get, api_path("trace/" ++ binary_to_list(Name) ++ "/download")),
    {ok, [
        _Comment,
        #zip_file{
            name = _ZipName,
            info = #file_info{size = Size, type = regular, access = read_write}
        }
    ]} = zip:table(Bin),
    ?assert(Size > 0),
    {ok, [{_, LogContent}]} = zip:unzip(Bin, [memory]),
    LogEntriesTrailing = string:split(LogContent, "\n", all),
    LogEntries = lists:droplast(LogEntriesTrailing),
    DecodedLogEntries = [
        begin
            ct:pal("LOG ENTRY\n~s\n", [JSONEntry]),
            emqx_utils_json:decode(JSONEntry)
        end
     || JSONEntry <- LogEntries
    ],
    ListIterFun =
        fun
            ListIterFunRec([]) ->
                ok;
            ListIterFunRec([Item | Rest]) ->
                receive
                    From ->
                        From ! {list_iter_item, Item}
                end,
                ListIterFunRec(Rest)
        end,
    ListIter = spawn_link(fun() -> ListIterFun(DecodedLogEntries) end),
    NextFun =
        fun() ->
            ListIter ! self(),
            receive
                {list_iter_item, Item} ->
                    Item
            end
        end,
    ?assertMatch(
        #{<<"meta">> := #{<<"payload">> := <<"log_this_message">>}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"payload">> := <<"\nlog\nthis\nmessage">>}},
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{<<"payload">> := <<"\\\nlog\n_\\n_this\nmessage\\">>}
        },
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"payload">> := <<"\"log_this_message\"">>}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"str">> := <<"str">>}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"term">> := <<"{notjson}">>}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := <<_/binary>>},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"integer">> := 42}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"float">> := 1.2}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := <<_/binary>>},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := <<_/binary>>},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := <<_/binary>>},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"sub">> := #{}}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"sub">> := #{<<"key">> := <<"value">>}}},
        NextFun()
    ),
    ?assertMatch(
        #{<<"meta">> := #{<<"true">> := true, <<"false">> := false}},
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"list">> := #{
                    <<"key">> := <<"value">>,
                    <<"key2">> := <<"value2">>
                }
            }
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"client_ids">> := [<<"a">>, <<"b">>, <<"c">>]
            }
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"rule_ids">> := [<<"a">>, <<"b">>, <<"c">>]
            }
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"action_info">> := #{
                    <<"type">> := <<"http">>,
                    <<"name">> := <<"emqx_bridge_http_test_lib">>
                }
            }
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"level">> := <<"error">>,
            <<"meta">> :=
                #{
                    <<"msg">> := <<"recursive_republish_detected">>,
                    <<"topic">> := <<"/x/y/z">>
                },
            <<"msg">> := <<"recursive_republish_detected">>,
            <<"time">> := _
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"map_key">> := #{
                    <<"a">> := <<"a">>,
                    <<"b">> := <<"b">>
                }
            }
        },
        NextFun()
    ),
    ?assertMatch(
        #{
            <<"meta">> := #{
                <<"map_key">> := [_, _]
            }
        },
        NextFun()
    ),
    {ok, Delete} = request_api(delete, api_path("trace/" ++ binary_to_list(Name))),
    ?assertEqual(<<>>, Delete),

    {ok, List2} = request_api(get, api_path("trace")),
    ?assertEqual([], json(List2)),

    ok = emqtt:disconnect(Client),
    unload(),
    emqx_trace:clear(),
    ok.

t_create_failed(_Config) ->
    load(),
    Now = erlang:system_time(second),
    Trace = [{<<"type">>, <<"topic">>}, {<<"topic">>, <<"/x/y/z">>}, {<<"start_at">>, Now}],

    BadName1 = {<<"name">>, <<"test/bad">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        request_api(post, api_path("trace"), [BadName1 | Trace])
    ),
    BadName2 = {<<"name">>, list_to_binary(lists:duplicate(257, "t"))},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        request_api(post, api_path("trace"), [BadName2 | Trace])
    ),

    %% already_exist
    GoodName = {<<"name">>, <<"test-name-0">>},
    {ok, Create} = request_api(post, api_path("trace"), [GoodName | Trace]),
    ?assertMatch(#{<<"name">> := <<"test-name-0">>}, json(Create)),
    ?assertMatch(
        {error, {"HTTP/1.1", 409, _}},
        request_api(post, api_path("trace"), [GoodName | Trace])
    ),

    %% MAX Limited
    lists:map(
        fun(Seq) ->
            Name0 = list_to_binary("name" ++ integer_to_list(Seq)),
            Trace0 = [
                {name, Name0},
                {type, topic},
                {topic, list_to_binary("/x/y/" ++ integer_to_list(Seq))}
            ],
            {ok, _} = emqx_trace:create(Trace0)
        end,
        lists:seq(1, 30 - ets:info(emqx_trace, size))
    ),
    GoodName1 = {<<"name">>, <<"test-name-1">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        request_api(post, api_path("trace"), [GoodName1 | Trace])
    ),
    %% clear, delete all
    ?assertMatch({ok, _}, request_api(delete, api_path("trace"), [])),
    %% allow create using test-name-0 again
    {ok, Create1} = request_api(post, api_path("trace"), [GoodName | Trace]),
    ?assertMatch(#{<<"name">> := <<"test-name-0">>}, json(Create1)),
    %% new name but same trace in the same second
    GoodName2 = {<<"name">>, <<"test-name-1">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 409, _}},
        request_api(post, api_path("trace"), [GoodName2 | Trace])
    ),
    %% new name but bad payload-encode
    GoodName3 = {<<"name">>, <<"test-name-2">>},
    PayloadEncode = {<<"payload_encode">>, <<"bad">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        request_api(post, api_path("trace"), [GoodName3, PayloadEncode | Trace])
    ),

    unload(),
    emqx_trace:clear(),
    ok.

t_log_file(_Config) ->
    ClientId = <<"client-test-download">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id">>,
    load(),
    create_trace(Name, ClientId, Now),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [
        begin
            _ = emqtt:ping(Client)
        end
     || _ <- lists:seq(1, 5)
    ],
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    ?assertMatch(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        request_api(get, api_path("trace/test_client_not_found/log_detail"))
    ),
    {ok, Detail} = request_api(get, api_path("trace/test_client_id/log_detail")),
    ?assertMatch([#{<<"mtime">> := _, <<"size">> := _, <<"node">> := _}], json(Detail)),
    {ok, Binary} = request_api(get, api_path("trace/test_client_id/download")),
    {ok, [
        Comment,
        #zip_file{
            name = ZipName,
            info = #file_info{size = Size, type = regular, access = read_write}
        }
    ]} = zip:table(Binary),
    ?assert(Size > 0),
    ZipNamePrefix = lists:flatten(io_lib:format("~s-trace_~s", [node(), Name])),
    ?assertNotEqual(nomatch, re:run(ZipName, [ZipNamePrefix])),
    Path = api_path("trace/test_client_id/download?node=" ++ atom_to_list(node())),
    {ok, Binary2} = request_api(get, Path),
    ?assertMatch(
        {ok, [
            Comment,
            #zip_file{
                name = ZipName,
                info = #file_info{size = Size, type = regular, access = read_write}
            }
        ]},
        zip:table(Binary2)
    ),
    {error, {_, 404, _}} =
        request_api(
            get,
            api_path("trace/test_client_id/download?node=unknown_node")
        ),
    {error, {_, 404, _}} =
        request_api(
            get,
            % known atom but unknown node
            api_path("trace/test_client_id/download?node=undefined")
        ),
    ?assertMatch(
        {error, {"HTTP/1.1", 404, "Not Found"}},
        request_api(
            get,
            api_path("trace/test_client_not_found/download?node=" ++ atom_to_list(node()))
        )
    ),
    ok = emqtt:disconnect(Client),
    ok.

create_trace(Name, ClientId, Start) ->
    create_trace(Name, clientid, ClientId, Start).

create_trace(Name, Type, TypeValue, Start) ->
    ?check_trace(
        #{timetrap => 900},
        begin
            {ok, _} = emqx_trace:create([
                {<<"name">>, Name},
                {<<"type">>, Type},
                {atom_to_binary(Type), TypeValue},
                {<<"start_at">>, Start}
            ]),
            ?block_until(#{?snk_kind := update_trace_done})
        end,
        fun(Trace) ->
            ?assertMatch([#{} | _], ?of_kind(update_trace_done, Trace))
        end
    ).

create_rule_trace(RuleId) ->
    Now = erlang:system_time(second),
    emqx_mgmt_api_trace_SUITE:create_trace(atom_to_binary(?FUNCTION_NAME), ruleid, RuleId, Now - 2).

t_create_rule_trace(_Config) ->
    load(),
    create_rule_trace(atom_to_binary(?FUNCTION_NAME)),
    unload(),
    ok.

t_stream_log(_Config) ->
    emqx_trace:clear(),
    load(),
    ClientId = <<"client-stream">>,
    Now = erlang:system_time(second),
    Name = <<"test_stream_log">>,
    create_trace(Name, ClientId, Now - 10),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [
        begin
            _ = emqtt:ping(Client)
        end
     || _ <- lists:seq(1, 5)
    ],
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood1">>, [{qos, 0}]),
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood2">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ct:sleep(200),
    File = emqx_trace:log_file(Name, Now),
    ct:pal("FileName: ~p", [File]),
    {ok, FileBin} = file:read_file(File),
    ct:pal("FileBin: ~p ~s", [byte_size(FileBin), FileBin]),
    {ok, Binary} = request_api(get, api_path("trace/test_stream_log/log?bytes=10")),
    #{<<"meta">> := Meta, <<"items">> := Bin} = json(Binary),
    ?assertEqual(10, byte_size(Bin)),
    ?assertEqual(#{<<"position">> => 10, <<"bytes">> => 10}, Meta),
    Path = api_path("trace/test_stream_log/log?position=20&bytes=10"),
    {ok, Binary1} = request_api(get, Path),
    #{<<"meta">> := Meta1, <<"items">> := Bin1} = json(Binary1),
    ?assertEqual(#{<<"position">> => 30, <<"bytes">> => 10}, Meta1),
    ?assertEqual(10, byte_size(Bin1)),
    ct:pal("~p vs ~p", [Bin, Bin1]),
    %% in theory they could be the same but we know they shouldn't
    ?assertNotEqual(Bin, Bin1),
    BadReqPath = api_path("trace/test_stream_log/log?&bytes=1000000000000"),
    {error, {_, 400, _}} = request_api(get, BadReqPath),
    meck:new(file, [passthrough, unstick]),
    meck:expect(file, read, 2, {error, enomem}),
    {error, {_, 503, _}} = request_api(get, Path),
    meck:unload(file),
    {error, {_, 404, _}} =
        request_api(
            get,
            api_path("trace/test_stream_log/log?node=unknown_node")
        ),
    {error, {_, 404, _}} =
        request_api(
            get,
            % known atom but not a node
            api_path("trace/test_stream_log/log?node=undefined")
        ),
    {error, {_, 404, _}} =
        request_api(
            get,
            api_path("trace/test_stream_log_not_found/log")
        ),
    unload(),
    ok.

t_trace_files_are_deleted_after_download(_Config) ->
    ClientId = <<"client-test-delete-after-download">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id">>,
    load(),
    create_trace(Name, ClientId, Now),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [
        begin
            _ = emqtt:ping(Client)
        end
     || _ <- lists:seq(1, 5)
    ],
    ok = emqtt:disconnect(Client),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),

    %% Check that files have been removed after download and that zip
    %% directories uses unique session ids
    ?check_trace(
        begin
            %% Download two zip files
            Path = api_path(["trace/", binary_to_list(Name), "/download"]),
            {ok, Binary1} = request_api(get, Path),
            {ok, Binary2} = request_api(get, Path),
            ?assertMatch({ok, _}, zip:table(Binary1)),
            ?assertMatch({ok, _}, zip:table(Binary2))
        end,
        fun(Trace) ->
            [
                #{session_id := SessionId1, zip_dir := ZipDir1},
                #{session_id := SessionId2, zip_dir := ZipDir2}
            ] = ?of_kind(trace_api_download_trace_log, Trace),
            ?assertEqual({error, enoent}, file:list_dir(ZipDir1)),
            ?assertEqual({error, enoent}, file:list_dir(ZipDir2)),
            ?assertNotEqual(SessionId1, SessionId2),
            ?assertNotEqual(ZipDir1, ZipDir2)
        end
    ),
    ok.

t_download_empty_trace(_Config) ->
    ClientId = <<"client-test-empty-trace-download">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id_empty_trace">>,
    load(),
    create_trace(Name, ClientId, Now),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertMatch(
                    {ok, _}, request_api(put, api_path(<<"trace/", Name/binary, "/stop">>), #{})
                ),
                #{?snk_kind := update_trace_done}
            )
        end,
        []
    ),
    {error, {{_, 404, _}, _Headers, Body}} =
        request_api(get, api_path(<<"trace/", Name/binary, "/download">>), [], #{return_all => true}),
    ?assertMatch(#{<<"message">> := <<"Trace is empty">>}, emqx_utils_json:decode(Body)),
    File = emqx_trace:log_file(Name, Now),
    ct:pal("FileName: ~p", [File]),
    ?assertEqual({ok, <<>>}, file:read_file(File)),
    ?assertEqual(ok, file:delete(File)),
    %% return 404 if trace file is not found
    {error, {{_, 404, _}, _Headers, Body}} =
        request_api(get, api_path(<<"trace/", Name/binary, "/download">>), [], #{return_all => true}),
    ?assertMatch(#{<<"message">> := <<"Trace is empty">>}, emqx_utils_json:decode(Body)),
    ok.

to_rfc3339(Second) ->
    list_to_binary(calendar:system_time_to_rfc3339(Second)).

request_api(Method, Url) ->
    request_api(Method, Url, []).

request_api(Method, Url, Body) ->
    request_api(Method, Url, Body, #{}).

request_api(Method, Url, Body, Opts) ->
    Opts1 = Opts#{httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, Url, [], [], Body, Opts1).

api_path(Path) ->
    emqx_mgmt_api_test_util:api_path([Path]).

json(Data) ->
    {ok, Jsx} = emqx_utils_json:safe_decode(Data, [return_maps]),
    Jsx.

load() ->
    emqx_trace:start_link().

unload() ->
    gen_server:stop(emqx_trace).
