%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_api_rule_apply_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(REPUBLISH_TOPIC, <<"rule_apply_test_SUITE">>).

all() ->
    [
        emqx_common_test_helpers:all(?MODULE),
        {group, republish},
        {group, console_print}
    ].

groups() ->
    [
        {republish, [], basic_tests()},
        {console_print, [], basic_tests()}
    ].

basic_tests() ->
    [
        t_basic_apply_rule_trace_ruleid,
        t_basic_apply_rule_trace_clientid,
        t_basic_apply_rule_trace_ruleid_stop_after_render
    ].

init_per_suite(Config) ->
    AppsToStart = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge,
        emqx_bridge_http,
        emqx_rule_engine,
        emqx_modules,
        emqx_management,
        emqx_mgmt_api_test_util:emqx_dashboard()
    ],
    Apps = emqx_cth_suite:start(
        AppsToStart,
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

init_per_group(GroupName, Config) ->
    [{group_name, GroupName} | Config].

end_per_group(_GroupName, Config) ->
    Config.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    HTTPPath = get_tc_prop(TestCase, http_path, <<"/path">>),
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(success_http_handler(#{})),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = emqx_bridge_schema_testlib:http_connector_config(#{
        <<"url">> => emqx_bridge_v2_testlib:fmt(<<"http://localhost:${p}">>, #{p => HTTPPort})
    }),
    ActionName = ConnectorName,
    ActionConfig = emqx_bridge_schema_testlib:http_action_config(#{
        <<"connector">> => ConnectorName
    }),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, http},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, http},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {http_server, #{port => HTTPPort, path => Path}}
        | TCConfig
    ].

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_bridge_http_connector_test_server:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    meck:unload(),
    ok.

t_basic_apply_rule_trace_ruleid(Config) ->
    basic_apply_rule_test_helper(?FUNCTION_NAME, get_action(Config), ruleid, false, text).

t_basic_apply_rule_trace_ruleid_hidden_payload(Config) ->
    basic_apply_rule_test_helper(?FUNCTION_NAME, get_action(Config), ruleid, false, hidden).

t_basic_apply_rule_trace_clientid(Config) ->
    basic_apply_rule_test_helper(?FUNCTION_NAME, get_action(Config), clientid, false, text).

t_basic_apply_rule_trace_ruleid_stop_after_render(Config) ->
    basic_apply_rule_test_helper(?FUNCTION_NAME, get_action(Config), ruleid, true, text).

get_action(Config) ->
    case ?config(group_name, Config) of
        republish ->
            republish_action();
        console_print ->
            console_print_action();
        _ ->
            make_http_bridge(Config)
    end.

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

make_http_bridge(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    {201, _} = emqx_bridge_v2_testlib:create_connector_api2(TCConfig, #{}),
    {201, _} = emqx_bridge_v2_testlib:create_action_api2(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${.id}">>}
    }),
    emqx_bridge_resource:bridge_id(Type, Name).

republish_action() ->
    #{
        <<"args">> =>
            #{
                <<"mqtt_properties">> => #{},
                <<"payload">> => <<"MY PL">>,
                <<"qos">> => 0,
                <<"retain">> => false,
                <<"topic">> => ?REPUBLISH_TOPIC,
                <<"user_properties">> => <<>>
            },
        <<"function">> => <<"republish">>
    }.

console_print_action() ->
    #{<<"function">> => <<"console">>}.

basic_apply_rule_test_helper(TestCase, Action, TraceType, StopAfterRender, PayloadEncode) ->
    %% Create Rule
    RuleTopic = iolist_to_binary([<<"my_rule_topic/">>, atom_to_binary(?FUNCTION_NAME)]),
    SQL = <<"SELECT payload.id as id, payload as payload FROM \"", RuleTopic/binary, "\"">>,
    {ok, #{<<"id">> := RuleId}} =
        emqx_bridge_testlib:create_rule_and_action(
            Action,
            RuleTopic,
            #{sql => SQL}
        ),
    ClientId = <<"c_emqx">>,
    %% ===================================
    %% Create trace for RuleId
    %% ===================================
    TraceName = atom_to_binary(?FUNCTION_NAME),
    TraceValue =
        case TraceType of
            ruleid ->
                RuleId;
            clientid ->
                ClientId
        end,
    create_trace(TraceName, TraceType, TraceValue, PayloadEncode),
    %% ===================================
    Context = #{
        clientid => ClientId,
        event_type => message_publish,
        payload => <<"{\"msg\": \"my_payload_msg\"}">>,
        qos => 1,
        topic => RuleTopic,
        username => <<"u_emqx">>
    },
    Params = #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => StopAfterRender
    },
    Now = erlang:system_time(second) - 10,
    ?assertMatch({ok, _}, call_apply_rule_api(RuleId, Params)),
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        begin
            Bin = read_rule_trace_file(TraceName, TraceType, Now),
            ct:pal("THELOG:~n~s", [Bin]),
            case PayloadEncode of
                hidden ->
                    ?assertEqual(nomatch, binary:match(Bin, [<<"my_payload_msg">>]));
                text ->
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"my_payload_msg">>]))
            end,
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"rule_activated">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"SQL_yielded_result">>])),
            case Action of
                A when is_binary(A) ->
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"bridge_action">>])),
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"QUERY_ASYNC">>]));
                _ ->
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"call_action_function">>]))
            end,
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_template_rendered">>]))
        end
    ),
    case StopAfterRender of
        true ->
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                begin
                    Bin = read_rule_trace_file(TraceName, TraceType, Now),
                    ct:pal("THELOG2:~n~s", [Bin]),
                    ?assertNotEqual(
                        nomatch, binary:match(Bin, [<<"action_stopped_after_template_rendering">>])
                    )
                end
            );
        false ->
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                begin
                    Bin = read_rule_trace_file(TraceName, TraceType, Now),
                    ct:pal("THELOG3:~n~s", [Bin]),
                    ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_success">>])),
                    do_final_log_check(TestCase, Action, Bin)
                end
            )
    end,
    %% Check that rule_trigger_ts meta field is present in all log entries
    Log0 = read_rule_trace_file(TraceName, TraceType, Now),
    Log1 = binary:split(Log0, <<"\n">>, [global, trim]),
    Log2 = lists:join(<<",\n">>, Log1),
    Log3 = iolist_to_binary(["[", Log2, "]"]),
    LogEntries = emqx_utils_json:decode(Log3),
    [#{<<"meta">> := #{<<"rule_trigger_ts">> := [RuleTriggerTime]}} | _] = LogEntries,
    [
        ?assert(lists:member(RuleTriggerTime, maps:get(<<"rule_trigger_ts">>, Meta, [])))
     || #{<<"meta">> := Meta} <- LogEntries
    ],
    ok.

do_final_log_check(TestCase, Action, Bin0) when is_binary(Action) ->
    TestCaseBin = atom_to_binary(TestCase),
    %% The last line in the Bin should be the action_success entry
    Bin1 = string:trim(Bin0),
    LastEntry = unicode:characters_to_binary(lists:last(string:split(Bin1, <<"\n">>, all))),
    LastEntryJSON = emqx_utils_json:decode(LastEntry),
    %% Check that lazy formatting of the action result works correctly
    ?assertMatch(
        #{
            <<"level">> := <<"debug">>,
            <<"meta">> :=
                #{
                    <<"action_info">> :=
                        #{
                            <<"name">> := TestCaseBin,
                            <<"type">> := <<"http">>
                        },
                    <<"clientid">> := <<"c_emqx">>,
                    <<"result">> :=
                        #{
                            <<"response">> :=
                                #{
                                    <<"body">> := <<"hello">>,
                                    <<"headers">> :=
                                        #{
                                            <<"content-type">> := <<"text/plain">>,
                                            <<"date">> := _,
                                            <<"server">> := _
                                        },
                                    <<"status">> := 200
                                },
                            <<"result">> := <<"ok">>
                        },
                    <<"rule_id">> := _,
                    <<"rule_trigger_ts">> := _,
                    <<"trace_tag">> := <<"ACTION">>
                },
            <<"msg">> := <<"action_success">>,
            <<"time">> := _
        },
        LastEntryJSON
    );
do_final_log_check(_, _, _) ->
    ok.

create_trace(TraceName, TraceType, TraceValue, PayloadEncode) ->
    Now = erlang:system_time(second) - 10,
    Start = Now,
    End = Now + 60,
    Trace = #{
        name => TraceName,
        filter => {TraceType, TraceValue},
        start_at => Start,
        end_at => End,
        formatter => json,
        payload_encode => PayloadEncode
    },
    {ok, _} = CreateRes = emqx_trace:create(Trace),
    emqx_common_test_helpers:on_exit(fun() ->
        ok = emqx_trace:delete(TraceName)
    end),
    CreateRes.

t_apply_rule_test_batch_separation_stop_after_render(_Config) ->
    meck_in_test_connector(),
    {ok, _} = emqx_connector:create(?global_ns, rule_engine_test, ?FUNCTION_NAME, #{}),
    Name = atom_to_binary(?FUNCTION_NAME),
    ActionConf =
        #{
            <<"connector">> => Name,
            <<"parameters">> =>
                #{
                    <<"values">> =>
                        #{
                            <<"send_to_pid">> => emqx_utils:bin_to_hexstr(
                                term_to_binary(self()), upper
                            )
                        }
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1000,
                <<"batch_time">> => 500
            }
        },
    {ok, _} = emqx_bridge_v2_testlib:create_kind_api(
        [
            {bridge_kind, action},
            {action_type, rule_engine_test},
            {action_name, ?FUNCTION_NAME},
            {action_config, ActionConf}
        ]
    ),
    SQL = <<"SELECT payload.is_stop_after_render as stop_after_render FROM \"", Name/binary, "\"">>,
    {ok, RuleID} = create_rule_with_action(
        rule_engine_test,
        ?FUNCTION_NAME,
        SQL
    ),
    create_trace(Name, ruleid, RuleID, text),
    Now = erlang:system_time(second) - 10,
    %% Stop
    ParmsStopAfterRender = apply_rule_parms(true, Name),
    ParmsNoStopAfterRender = apply_rule_parms(false, Name),
    %% Check that batching is working
    Count = 200,
    CountMsgFun =
        fun
            CountMsgFunRec(0 = _CurCount, GotBatchWithAtLeastTwo) ->
                GotBatchWithAtLeastTwo;
            CountMsgFunRec(CurCount, GotBatchWithAtLeastTwo) ->
                receive
                    List ->
                        Len = length(List),
                        CountMsgFunRec(CurCount - Len, GotBatchWithAtLeastTwo orelse (Len > 1))
                end
        end,
    lists:foreach(
        fun(_) ->
            {ok, _} = call_apply_rule_api(RuleID, ParmsStopAfterRender)
        end,
        lists:seq(1, Count)
    ),
    %% We should get the messages and at least one batch with more than 1
    true = CountMsgFun(Count, false),
    %% We should check that we don't get any mixed batch
    CheckBatchesFun =
        fun
            CheckBatchesFunRec(0 = _CurCount) ->
                ok;
            CheckBatchesFunRec(CurCount) ->
                receive
                    [{_, FirstMsg} | _] = List ->
                        StopValue = maps:get(<<"stop_after_render">>, FirstMsg, false),
                        [
                            ?assertEqual(StopValue, maps:get(<<"stop_after_render">>, Msg, false))
                         || {_, Msg} <- List
                        ],
                        Len = length(List),
                        CheckBatchesFunRec(CurCount - Len)
                end
        end,
    lists:foreach(
        fun(_) ->
            case rand:normal() < 0 of
                true ->
                    {ok, _} = call_apply_rule_api(RuleID, ParmsStopAfterRender);
                false ->
                    {ok, _} = call_apply_rule_api(RuleID, ParmsNoStopAfterRender)
            end
        end,
        lists:seq(1, Count)
    ),
    CheckBatchesFun(Count),
    %% Just check that the log file is created as expected
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        begin
            Bin = read_rule_trace_file(Name, ruleid, Now),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_success">>])),
            ?assertNotEqual(
                nomatch, binary:match(Bin, [<<"action_stopped_after_template_rendering">>])
            )
        end
    ),
    ok.

t_apply_rule_test_format_action_failed(_Config) ->
    MeckOpts = [passthrough, no_link, no_history, non_strict],
    catch meck:new(emqx_rule_engine_test_connector, MeckOpts),
    meck:expect(
        emqx_rule_engine_test_connector,
        on_query,
        3,
        {error, {unrecoverable_error, <<"MY REASON">>}}
    ),
    CheckFun =
        fun(Bin0) ->
            %% The last line in the Bin should be the action_failed entry
            ?assertNotEqual(nomatch, binary:match(Bin0, [<<"action_failed">>])),
            Bin1 = string:trim(Bin0),
            LastEntry = unicode:characters_to_binary(lists:last(string:split(Bin1, <<"\n">>, all))),
            LastEntryJSON = emqx_utils_json:decode(LastEntry),
            ?assertMatch(
                #{
                    <<"level">> := <<"debug">>,
                    <<"meta">> := #{
                        <<"action_info">> := #{
                            <<"name">> := _,
                            <<"type">> := <<"rule_engine_test">>
                        },
                        <<"clientid">> := _,
                        <<"reason">> := <<"MY REASON">>,
                        <<"rule_id">> := _,
                        <<"rule_trigger_ts">> := _,
                        <<"trace_tag">> := <<"ACTION">>
                    },
                    <<"msg">> := <<"action_failed">>,
                    <<"time">> := _
                },
                LastEntryJSON
            ),
            MetaMap = maps:get(<<"meta">>, LastEntryJSON),
            ?assert(not maps:is_key(<<"client_ids">>, MetaMap)),
            ?assert(not maps:is_key(<<"rule_ids">>, MetaMap))
        end,
    do_apply_rule_test_format_action_failed_test(1, CheckFun).

t_apply_rule_test_format_action_out_of_service_query(_Config) ->
    Reason = <<"MY_RECOVERABLE_REASON">>,
    CheckFun = out_of_service_check_fun(<<"send_error">>, Reason),
    meck_test_connector_recoverable_errors(Reason),
    do_apply_rule_test_format_action_failed_test(1, CheckFun).

t_apply_rule_test_format_action_out_of_service_batch_query(_Config) ->
    Reason = <<"MY_RECOVERABLE_REASON">>,
    CheckFun = out_of_service_check_fun(<<"send_error">>, Reason),
    meck_test_connector_recoverable_errors(Reason),
    do_apply_rule_test_format_action_failed_test(10, CheckFun).

t_apply_rule_test_format_action_out_of_service_async_query(_Config) ->
    Reason = <<"MY_RECOVERABLE_REASON">>,
    CheckFun = out_of_service_check_fun(<<"async_send_error">>, Reason),
    meck_test_connector_recoverable_errors(Reason),
    meck:expect(
        emqx_rule_engine_test_connector,
        callback_mode,
        0,
        async_if_possible
    ),
    do_apply_rule_test_format_action_failed_test(1, CheckFun).

t_apply_rule_test_format_action_out_of_service_async_batch_query(_Config) ->
    Reason = <<"MY_RECOVERABLE_REASON">>,
    CheckFun = out_of_service_check_fun(<<"async_send_error">>, Reason),
    meck_test_connector_recoverable_errors(Reason),
    meck:expect(
        emqx_rule_engine_test_connector,
        callback_mode,
        0,
        async_if_possible
    ),
    do_apply_rule_test_format_action_failed_test(10, CheckFun).

out_of_service_check_fun(SendErrorMsg, Reason) ->
    fun(Bin0) ->
        %% The last line in the Bin should be the action_failed entry
        ?assertNotEqual(nomatch, binary:match(Bin0, [<<"action_failed">>])),
        io:format("LOG:\n~s", [Bin0]),
        Bin1 = string:trim(Bin0),
        LastEntry = unicode:characters_to_binary(lists:last(string:split(Bin1, <<"\n">>, all))),
        LastEntryJSON = emqx_utils_json:decode(LastEntry),
        ?assertMatch(
            #{
                <<"level">> := <<"debug">>,
                <<"meta">> :=
                    #{
                        <<"action_info">> :=
                            #{
                                <<"name">> := _,
                                <<"type">> := <<"rule_engine_test">>
                            },
                        <<"clientid">> := _,
                        <<"reason">> := <<"request_expired">>,
                        <<"rule_id">> := _,
                        <<"rule_trigger_ts">> := _,
                        <<"trace_tag">> := <<"ACTION">>
                    },
                <<"msg">> := <<"action_failed">>,
                <<"time">> := _
            },
            LastEntryJSON
        ),
        %% We should have at least one entry containing Reason
        [ReasonLine | _] = find_lines_with(Bin1, Reason),
        ReasonEntryJSON = emqx_utils_json:decode(ReasonLine),
        ?assertMatch(
            #{
                <<"level">> := <<"debug">>,
                <<"meta">> :=
                    #{
                        <<"clientid">> := _,
                        <<"id">> := _,
                        <<"reason">> :=
                            #{
                                <<"additional_info">> := _,
                                <<"error_type">> := <<"recoverable_error">>,
                                <<"msg">> := <<"MY_RECOVERABLE_REASON">>
                            },
                        <<"rule_id">> := _,
                        <<"rule_trigger_ts">> := _,
                        <<"trace_tag">> := <<"ERROR">>
                    },
                <<"msg">> := SendErrorMsg,
                <<"time">> := _
            },
            ReasonEntryJSON
        ),
        MetaMap = maps:get(<<"meta">>, ReasonEntryJSON),
        ?assert(not maps:is_key(<<"client_ids">>, MetaMap)),
        ?assert(not maps:is_key(<<"rule_ids">>, MetaMap))
    end.

meck_test_connector_recoverable_errors(Reason) ->
    MeckOpts = [passthrough, no_link, no_history, non_strict],
    catch meck:new(emqx_rule_engine_test_connector, MeckOpts),
    meck:expect(
        emqx_rule_engine_test_connector,
        on_query,
        3,
        {error, {recoverable_error, Reason}}
    ),
    meck:expect(
        emqx_rule_engine_test_connector,
        on_batch_query,
        3,
        {error, {recoverable_error, Reason}}
    ),
    meck:expect(
        emqx_rule_engine_test_connector,
        on_query_async,
        4,
        {error, {recoverable_error, Reason}}
    ),
    meck:expect(
        emqx_rule_engine_test_connector,
        on_batch_query_async,
        4,
        {error, {recoverable_error, Reason}}
    ).

find_lines_with(Data, InLineText) ->
    % Split the binary data into lines
    Lines = re:split(Data, "\n", [{return, binary}]),

    % Use a list comprehension to filter lines containing 'Reason'
    [Line || Line <- Lines, re:run(Line, InLineText, [multiline, {capture, none}]) =/= nomatch].

do_apply_rule_test_format_action_failed_test(BatchSize, CheckLastTraceEntryFun) ->
    meck_in_test_connector(),
    {ok, _} = emqx_connector:create(?global_ns, rule_engine_test, ?FUNCTION_NAME, #{}),
    Name = atom_to_binary(?FUNCTION_NAME),
    ActionConf =
        #{
            <<"connector">> => Name,
            <<"parameters">> => #{<<"values">> => #{}},
            <<"resource_opts">> => #{
                <<"batch_size">> => BatchSize,
                <<"batch_time">> => 10,
                <<"request_ttl">> => 200
            }
        },
    {ok, _} = emqx_bridge_v2_testlib:create_kind_api(
        [
            {bridge_kind, action},
            {action_type, rule_engine_test},
            {action_name, ?FUNCTION_NAME},
            {action_config, ActionConf}
        ]
    ),
    SQL = <<"SELECT payload.is_stop_after_render as stop_after_render FROM \"", Name/binary, "\"">>,
    {ok, RuleID} = create_rule_with_action(
        rule_engine_test,
        ?FUNCTION_NAME,
        SQL
    ),
    create_trace(Name, ruleid, RuleID, text),
    Now = erlang:system_time(second) - 10,
    %% Stop
    ParmsNoStopAfterRender = apply_rule_parms(false, Name),
    {ok, _} = call_apply_rule_api(RuleID, ParmsNoStopAfterRender),
    %% Just check that the log file is created as expected
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 100,
        begin
            Bin = read_rule_trace_file(Name, ruleid, Now),
            CheckLastTraceEntryFun(Bin)
        end
    ),
    ok.

meck_in_test_connector() ->
    MeckOpts = [passthrough, no_history],
    catch meck:new(emqx_connector_info, MeckOpts),
    meck:expect(
        emqx_connector_info,
        hard_coded_test_connector_info_modules,
        0,
        [emqx_rule_engine_test_connector_info]
    ),
    emqx_connector_info:clean_cache(),
    catch meck:new(emqx_action_info, MeckOpts),
    meck:expect(
        emqx_action_info,
        hard_coded_test_action_info_modules,
        0,
        [emqx_rule_engine_test_action_info]
    ),
    emqx_action_info:clean_cache().

apply_rule_parms(StopAfterRender, Name) ->
    Payload = #{<<"is_stop_after_render">> => StopAfterRender},
    Context = #{
        clientid => Name,
        event_type => message_publish,
        payload => emqx_utils_json:encode(Payload),
        qos => 1,
        topic => Name,
        username => <<"u_emqx">>
    },
    #{
        <<"context">> => Context,
        <<"stop_action_after_template_rendering">> => StopAfterRender
    }.

create_rule_with_action(ActionType, ActionName, SQL) ->
    BridgeId = emqx_bridge_resource:bridge_id(ActionType, ActionName),
    Params = #{
        enable => true,
        sql => SQL,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    ct:pal("rule action params: ~p", [Params]),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res0} ->
            #{<<"id">> := RuleId} = emqx_utils_json:decode(Res0),
            {ok, RuleId};
        Error ->
            Error
    end.

%% Helper Functions

call_apply_rule_api(RuleId, Params) ->
    Method = post,
    Path = emqx_mgmt_api_test_util:api_path(["rules", RuleId, "test"]),
    Res = request(Method, Path, Params),
    Res.

request(Method, Path, Params) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Path, "", AuthHeader, Params, Opts) of
        {ok, {Status, Headers, Body0}} ->
            Body = maybe_json_decode(Body0),
            {ok, {Status, Headers, Body}};
        {error, {Status, Headers, Body0}} ->
            Body =
                case emqx_utils_json:safe_decode(Body0) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, Headers, Body}};
        Error ->
            Error
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

read_rule_trace_file(TraceName, _TraceType, From) ->
    emqx_trace:check(),
    %% NOTE: Twice as long as `?LOG_HANDLER_FILESYNC_INTERVAL` in `emqx_trace_handler`.
    timer:sleep(2 * 100),
    {ok, Bin} = file:read_file(emqx_trace:log_file(TraceName, From)),
    Bin.

success_http_handler(Opts) ->
    ResponseDelay = maps:get(response_delay, Opts, 0),
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        Headers = cowboy_req:headers(Req),
        ct:pal("http request received: ~p", [
            #{body => Body, headers => Headers, response_delay => ResponseDelay}
        ]),
        ResponseDelay > 0 andalso timer:sleep(ResponseDelay),
        TestPid ! {http, Headers, Body},
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"text/plain">>},
            <<"hello">>,
            Req
        ),
        {ok, Rep, State}
    end.
