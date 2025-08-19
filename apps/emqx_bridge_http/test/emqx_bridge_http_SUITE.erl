%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, http).
-define(CONNECTOR_TYPE_BIN, <<"http">>).
-define(ACTION_TYPE, http).
-define(ACTION_TYPE_BIN, <<"http">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
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
    ConnectorConfig = connector_config(#{
        <<"url">> => emqx_bridge_v2_testlib:fmt(<<"http://localhost:${p}">>, #{p => HTTPPort})
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {http_server, #{port => HTTPPort, path => Path}}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"url">> => <<"please override">>,
        <<"connect_timeout">> => <<"15s">>,
        <<"headers">> => #{<<"content-type">> => <<"application/json">>},
        <<"max_inactive">> => <<"10s">>,
        <<"pool_size">> => 1,
        <<"pool_type">> => <<"random">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"path">> => <<"/path">>,
            <<"method">> => <<"post">>,
            <<"headers">> => #{<<"headerk">> => <<"headerv">>},
            <<"body">> => <<"${.}">>,
            <<"max_retries">> => 2
        },
        <<"resource_opts">> =>
            maps:without(
                [<<"batch_size">>, <<"batch_time">>],
                emqx_bridge_v2_testlib:common_action_resource_opts()
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

receive_request_notifications(MessageIDs, _Acc) when map_size(MessageIDs) =:= 0 ->
    ok;
receive_request_notifications(MessageIDs, Acc) ->
    receive
        {http, _Headers, Body} ->
            RemainingMessageIDs = remove_message_id(MessageIDs, Body),
            receive_request_notifications(RemainingMessageIDs, [Body | Acc])
    after (30 * 1000) ->
        ct:pal("Waited a long time but did not get any message"),
        ct:pal("Messages received so far:\n  ~p", [Acc]),
        ct:pal("Mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("All requests did not reach server at least once")
    end.

remove_message_id(MessageIDs, IDBin) ->
    ID = erlang:binary_to_integer(IDBin),
    %% It is acceptable to get the same message more than once
    maps:without([ID], MessageIDs).

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

not_found_http_handler() ->
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep = cowboy_req:reply(
            404,
            #{<<"content-type">> => <<"text/plain">>},
            <<"not found">>,
            Req
        ),
        {ok, Rep, State}
    end.

too_many_requests_http_handler() ->
    fail_then_success_http_handler(429).

service_unavailable_http_handler() ->
    fail_then_success_http_handler(503).

fail_then_success_http_handler(FailStatusCode) ->
    on_exit(fun() -> persistent_term:erase({?MODULE, times_called}) end),
    GetAndBump =
        fun() ->
            NCalled = persistent_term:get({?MODULE, times_called}, 0),
            persistent_term:put({?MODULE, times_called}, NCalled + 1),
            NCalled + 1
        end,
    TestPid = self(),
    fun(Req0, State) ->
        N = GetAndBump(),
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep =
            case N >= 3 of
                true ->
                    cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"text/plain">>},
                        <<"ok">>,
                        Req
                    );
                false ->
                    cowboy_req:reply(
                        FailStatusCode,
                        #{<<"content-type">> => <<"text/plain">>},
                        %% Body and no body to trigger different code paths
                        case N of
                            1 -> <<"slow down, buddy">>;
                            _ -> <<>>
                        end,
                        Req
                    )
            end,
        {ok, Rep, State}
    end.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_connector_api2(Config, Overrides).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_action_api2(Config, Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

trace_log_stream_api(TraceName, Opts) ->
    emqx_bridge_v2_testlib:trace_log_stream_api(TraceName, Opts).

probe_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:probe_bridge_api_simple(TCConfig, Overrides).

start_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:start_connector_api(Name, Type)
    ).

disable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(
        TCConfig
    ),
    emqx_bridge_v2_testlib:disable_kind_api(Kind, Type, Name).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, emqx_connector_http_stopped).

t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_rule_action(Config) when is_list(Config) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        {http, Headers, BodyRaw} = ?assertReceive({http, _, _}),
        ?assertMatch(#{<<"headerk">> := <<"headerv">>}, Headers),
        ?assertMatch(
            #{<<"payload">> := Payload},
            emqx_utils_json:decode(BodyRaw),
            #{payload => Payload}
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts).

%% This test ensures that https://emqx.atlassian.net/browse/CI-62 is fixed.
%% When the connection time out all the queued requests where dropped in
t_send_async_connection_timeout(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${.payload}">>},
        <<"resource_opts">> => #{<<"query_mode">> => <<"async">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    NumberOfMessagesToSend = 10,
    lists:foreach(
        fun(N) ->
            emqtt:publish(C, Topic, integer_to_binary(N), [{qos, 0}])
        end,
        lists:seq(1, NumberOfMessagesToSend)
    ),
    %% Make sure server receives all messages
    ct:pal("Sent messages\n"),
    MessageIDs = maps:from_keys(lists:seq(1, NumberOfMessagesToSend), void),
    receive_request_notifications(MessageIDs, []),
    ok.

t_send_get_trace_messages(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${.payload.id}">>}
    }),
    #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
    {200, #{<<"name">> := TraceName}} = emqx_bridge_v2_testlib:start_rule_test_trace(RuleId, #{}),
    on_exit(fun() -> emqx_bridge_v2_testlib:stop_rule_test_trace(TraceName) end),
    ?assertMatch({200, #{<<"items">> := <<"">>}}, trace_log_stream_api(TraceName, #{})),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 1
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),
    %% NOTE: See `?LOG_HANDLER_FILESYNC_INTERVAL` in `emqx_trace_handler`.
    ?retry(2 * 200, 20, begin
        emqx_trace:check(),
        {200, #{<<"items">> := Bin}} = trace_log_stream_api(TraceName, #{
            query_params => #{
                <<"bytes">> => integer_to_binary(1 bsl 20)
            }
        }),
        ?assertNotEqual(nomatch, binary:match(Bin, [<<"rule_activated">>])),
        ?assertNotEqual(nomatch, binary:match(Bin, [<<"SQL_yielded_result">>])),
        ?assertNotEqual(nomatch, binary:match(Bin, [<<"bridge_action">>])),
        ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_template_rendered">>])),
        ?assertNotEqual(nomatch, binary:match(Bin, [<<"QUERY_ASYNC">>]))
    end),
    ok.

t_async_free_retries(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Publish = fun() -> emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]) end,
    %% Fail 5 times then succeed.
    Context = #{error_attempts => 5},
    ExpectedAttempts = 6,
    Fn = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    do_t_async_retries(?FUNCTION_NAME, Context, {error, normal}, Fn),
    do_t_async_retries(?FUNCTION_NAME, Context, {error, {shutdown, normal}}, Fn),
    ok.

t_async_common_retries(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Publish = fun() -> emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]) end,
    %% Keeps failing until connector gives up.
    Context = #{error_attempts => infinity},
    ExpectedAttempts = 3,
    FnSucceed = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    FnFail = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    %% These two succeed because they're further retried by the buffer
    %% worker synchronously, and we're not mock that call.
    do_t_async_retries(
        ?FUNCTION_NAME, Context, {error, {closed, "The connection was lost."}}, FnSucceed
    ),
    do_t_async_retries(?FUNCTION_NAME, Context, {error, {shutdown, closed}}, FnSucceed),
    %% This fails because this error is treated as unrecoverable.
    do_t_async_retries(?FUNCTION_NAME, Context, {error, something_else}, FnFail),
    ok.

do_t_async_retries(TestCase, TestContext, Error, Fn) ->
    #{error_attempts := ErrorAttempts} = TestContext,
    PTKey = {?MODULE, TestCase, attempts},
    persistent_term:put(PTKey, 0),
    on_exit(fun() -> persistent_term:erase(PTKey) end),
    Get = fun() -> persistent_term:get(PTKey) end,
    GetAndBump = fun() ->
        Attempts = persistent_term:get(PTKey),
        persistent_term:put(PTKey, Attempts + 1),
        Attempts + 1
    end,
    emqx_common_test_helpers:with_mock(
        emqx_bridge_http_connector,
        reply_delegator,
        fun(Context, ReplyFunAndArgs, Result) ->
            Attempts = GetAndBump(),
            case Attempts > ErrorAttempts of
                true ->
                    ct:pal("succeeding ~p : ~p", [Error, Attempts]),
                    meck:passthrough([Context, ReplyFunAndArgs, Result]);
                false ->
                    ct:pal("failing ~p : ~p", [Error, Attempts]),
                    meck:passthrough([Context, ReplyFunAndArgs, Error])
            end
        end,
        fun() -> Fn(Get, Error) end
    ),
    persistent_term:erase(PTKey),
    ok.

t_bridge_probes_header_atoms(TCConfig) ->
    ?check_trace(
        begin
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
            Overrides = #{
                <<"parameters">> => #{
                    <<"headers">> => #{<<"some-non-existent-atom">> => <<"x">>}
                }
            },
            ?assertMatch({204, _}, probe_action_api(TCConfig, Overrides)),
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_action_api(TCConfig, Overrides)
            ),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"hi">>, [{qos, 1}]),
            receive
                {http, Headers, _Body} ->
                    ?assertMatch(#{<<"some-non-existent-atom">> := <<"x">>}, Headers),
                    ok
            after 5_000 ->
                ct:pal("mailbox: ~p", [process_info(self(), messages)]),
                ct:fail("request not made")
            end,
            ok
        end,
        []
    ),
    ok.

t_rule_action_expired(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{
                <<"url">> => <<"http://non.existent.host:9999">>
            }),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"request_ttl">> => <<"100ms">>,
                    <<"metrics_flush_interval">> => <<"300ms">>
                }
            }),
            #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            'actions.failed' := 1,
                            'actions.failed.unknown' := 1,
                            'actions.total' := 1
                        }
                    },
                    emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_too_many_requests(TCConfig) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(too_many_requests_http_handler()),
    check_send_is_retried(TCConfig).

t_service_unavailable(Config) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(service_unavailable_http_handler()),
    check_send_is_retried(Config).

check_send_is_retried(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"infinity">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            %% should retry
            ?assertReceive({http, _, _}),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0,
                            <<"success">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_empty_path() ->
    [{http_path, <<"/">>}].
t_empty_path(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{<<"path">> => <<"">>},
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"infinity">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0,
                            <<"success">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_path_not_found(TCConfig) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(not_found_http_handler()),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"infinity">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 1,
                            <<"dropped">> := 0,
                            <<"success">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_bad_bridge_config(TCConfig) ->
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"Connection refused">>
        }},
        create_connector_api(TCConfig, #{<<"url">> => <<"http://localhost:12345">>})
    ),
    %% try `/start` bridge
    ?assertMatch(
        {400, #{<<"message">> := <<"Connection refused">>}},
        start_connector_api(TCConfig)
    ),
    ok.

t_compose_connector_url_and_action_path() ->
    [{http_path, <<"/foo/bar">>}].
t_compose_connector_url_and_action_path(TCConfig) ->
    #{port := HTTPPort} = get_config(http_server, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{
        <<"url">> => emqx_bridge_v2_testlib:fmt(<<"http://localhost:${p}/foo">>, #{p => HTTPPort})
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"path">> => <<"/bar">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}),
    ok.

%% Checks that we can successfully update a connector containing sensitive headers and
%% they won't be clobbered by the update.
t_update_with_sensitive_data(TCConfig) ->
    AuthHeader = <<"Bearer some_token">>,
    {201, #{<<"headers">> := #{<<"authorization">> := Obfuscated}}} =
        create_connector_api(TCConfig, #{
            <<"headers">> => #{
                <<"authorization">> => AuthHeader,
                <<"x-test-header">> => <<"from-connector">>
            }
        }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"headers">> => #{<<"x-test-header">> => <<"from-action">>}}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>
            },
            _}
    ),

    %% Now update the connector and see if the header stays deobfuscated.  We send the old
    %% auth header as an obfuscated value to simulate the behavior of the frontend.
    {200, _} = update_connector_api(TCConfig, #{
        <<"headers">> => #{
            <<"authorization">> => Obfuscated,
            <<"x-test-header">> => <<"from-connector-new">>,
            <<"x-test-header-2">> => <<"from-connector-new">>,
            <<"other_header">> => <<"new">>
        }
    }),

    emqtt:publish(C, Topic, <<"hey2">>, [{qos, 1}]),
    %% Should not be obfuscated.
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>,
                <<"x-test-header-2">> := <<"from-connector-new">>
            },
            _},
        2_000
    ),
    ok.

t_disable_action_counters(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}, 2_000),

    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 1,
                    'actions.discarded' := 0
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),

    %% disable the action
    {204, _} = disable_action_api(TCConfig),

    %% this will trigger a discard
    emqtt:publish(C, Topic, <<"hey2">>, [{qos, 1}]),
    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 2,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 2,
                    'actions.discarded' := 1
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),
    ok.

t_rule_test_trace(TCConfig) ->
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, #{}).
