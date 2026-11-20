%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_delayed_republish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_modules,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    {200, DelayedConfig} = request(get, ["mqtt", "delayed"], []),
    on_exit(fun() ->
        {200, _} = request(put, ["mqtt", "delayed"], DelayedConfig)
    end),
    {200, _} = request(put, ["mqtt", "delayed"], #{enable => true, max_delayed_messages => 0}),
    ok = emqx_delayed:clear_all(),
    on_exit(fun emqx_delayed:clear_all/0),
    {ok, Client} = emqtt:start_link(#{clientid => atom_to_binary(Case), proto_ver => v5}),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    {ok, _} = emqtt:connect(Client),
    [{client, Client} | Config].

end_per_testcase(_Case, _Config) ->
    emqx_common_test_helpers:call_janitor().

-doc "Check delayed delivery, downstream rule execution, and action metrics.".
t_delayed_republish(Config) ->
    delayed_republish(Config, <<"5">>, true).

-doc "Check action metrics and delivery for an absolute delayed publish timestamp.".
t_delayed_republish_absolute(Config) ->
    Timestamp = integer_to_binary(erlang:system_time(second) + 5),
    delayed_republish(Config, Timestamp, true).

-doc "Check action metrics and delivery when the delayed interval is zero.".
t_delayed_republish_immediate(Config) ->
    delayed_republish(Config, <<"0">>, false).

-doc "Check that a full delayed queue remains an action failure.".
t_delayed_queue_full(Config) ->
    Client = ?config(client, Config),
    {200, _} = request(put, ["mqtt", "delayed"], #{enable => true, max_delayed_messages => 1}),
    Input = <<"delayed_republish/input">>,
    Id = create_rule(upstream, Input, <<"$delayed/60/delayed_republish/output">>),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"accepted">>, 1),
    {ok, _} = emqtt:publish(Client, Input, <<"rejected">>, 1),
    assert_metrics(Id, 1, 1),
    assert_received_metrics(ReceivedBefore, 3),
    {200, #{<<"data">> := Messages}} = request(get, ["mqtt", "delayed", "messages"], []),
    ?assertEqual(1, length(Messages)).

-doc "Check that an unrelated publish hook rejection remains an action failure.".
t_republish_blocked(Config) ->
    Client = ?config(client, Config),
    Input = <<"delayed_republish/input">>,
    Output = <<"delayed_republish/blocked">>,
    Id = create_rule(upstream, Input, Output),
    ok = emqx_hooks:put('message.publish', {?MODULE, block_publish, [Output]}, 1000),
    on_exit(fun() -> emqx_hooks:del('message.publish', {?MODULE, block_publish}) end),
    {ok, _, [1]} = emqtt:subscribe(Client, Output, 1),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"blocked">>, 1),
    assert_metrics(Id, 0, 1),
    assert_received_metrics(ReceivedBefore, 1),
    ?assertNotReceive({publish, _}, 300, blocked_message_delivered).

-doc "Check that ordinary republishing without subscribers still succeeds.".
t_republish_no_subscribers(Config) ->
    Client = ?config(client, Config),
    Input = <<"delayed_republish/input">>,
    Id = create_rule(upstream, Input, <<"delayed_republish/no_subscribers">>),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"ordinary">>, 1),
    assert_metrics(Id, 1, 0),
    assert_received_metrics(ReceivedBefore, 2).

delayed_republish(Config, Delay, CheckDelay) ->
    Client = ?config(client, Config),
    Input = <<"delayed_republish/input">>,
    Output = <<"delayed_republish/output">>,
    Final = <<"delayed_republish/final">>,
    Upstream = create_rule(upstream, Input, <<"$delayed/", Delay/binary, "/", Output/binary>>),
    Downstream = create_rule(downstream, Output, Final),
    {ok, _, [1]} = emqtt:subscribe(Client, Final, 1),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"delayed">>, 1),
    assert_delayed_delivery(CheckDelay),
    ?assertReceive({publish, #{topic := Final, payload := <<"delayed">>}}, 7000),
    assert_metrics(Upstream, 1, 0),
    assert_metrics(Downstream, 1, 0),
    assert_received_metrics(ReceivedBefore, 3),
    ?assertNotReceive({publish, _}, 300, duplicate_delivery).

assert_delayed_delivery(false) ->
    ok;
assert_delayed_delivery(true) ->
    ?assertNotReceive({publish, _}, 500, delivered_too_early).

received_metrics() ->
    #{
        received => emqx_metrics:val('messages.received'),
        qos1_received => emqx_metrics:val('messages.qos1.received')
    }.

assert_received_metrics(Before, Count) ->
    Expected = maps:map(fun(_Metric, Value) -> Value + Count end, Before),
    ?assertEqual(Expected, received_metrics()).

block_publish(Msg = #message{topic = Topic}, Topic) ->
    {stop, emqx_message:set_header(allow_publish, false, Msg)};
block_publish(Msg, _Topic) ->
    {ok, Msg}.

create_rule(Name, Input, Output) ->
    Id = <<"delayed_republish_", (atom_to_binary(Name))/binary>>,
    SQL = iolist_to_binary(
        io_lib:format(
            """
            SELECT * FROM "~s"
            """,
            [Input]
        )
    ),
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(#{
        id => Id,
        sql => SQL,
        actions => [
            #{
                function => republish,
                args => #{topic => Output, qos => 1, retain => false, payload => <<"${payload}">>}
            }
        ]
    }),
    on_exit(fun() ->
        {204, _} = emqx_bridge_v2_testlib:delete_rule_api(Id)
    end),
    Id.

assert_metrics(Id, Success, Failed) ->
    Expected = #{
        <<"actions.total">> => Success + Failed,
        <<"actions.success">> => Success,
        <<"actions.failed">> => Failed,
        <<"actions.failed.unknown">> => Failed
    },
    ?retry(50, 20, begin
        {200, #{<<"metrics">> := Metrics}} = request(get, ["rules", Id, "metrics"], []),
        ?assertEqual(Expected, maps:with(maps:keys(Expected), Metrics))
    end).

request(Method, Path, Body) ->
    emqx_mgmt_api_test_util:simple_request(
        Method, emqx_mgmt_api_test_util:api_path(Path), Body
    ).
