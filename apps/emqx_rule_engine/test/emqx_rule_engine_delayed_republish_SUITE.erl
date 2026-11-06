%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_delayed_republish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

all() ->
    [
        t_delayed_republish,
        t_delayed_republish_absolute,
        t_delayed_republish_immediate,
        t_delayed_queue_full,
        t_republish_blocked,
        t_republish_no_subscribers
    ].

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
    {200, DelayedConfig} = request(get, ["mqtt", "delayed"], #{}),
    {200, _} = request(put, ["mqtt", "delayed"], #{enable => true, max_delayed_messages => 0}),
    ok = emqx_delayed:clear_all(),
    {ok, Client} = emqtt:start_link(#{clientid => atom_to_binary(Case), proto_ver => v5}),
    {ok, _} = emqtt:connect(Client),
    [{client, Client}, {delayed_config, DelayedConfig} | Config].

end_per_testcase(_Case, Config) ->
    ok = emqx_hooks:del('message.publish', {?MODULE, block_publish}),
    lists:foreach(
        fun(Id) -> request(delete, ["rules", Id], #{}) end,
        [<<"delayed_republish_upstream">>, <<"delayed_republish_downstream">>]
    ),
    emqtt:stop(?config(client, Config)),
    ok = emqx_delayed:clear_all(),
    {200, _} = request(put, ["mqtt", "delayed"], ?config(delayed_config, Config)),
    ok.

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
    receive
        {publish, #{topic := Final, payload := <<"delayed">>}} -> ok
    after 7000 -> ct:fail(delayed_message_missing)
    end,
    assert_metrics(Upstream, 1, 0),
    assert_metrics(Downstream, 1, 0),
    assert_received_metrics(ReceivedBefore, 3),
    receive
        {publish, Message1} -> ct:fail({duplicate_delivery, Message1})
    after 300 -> ok
    end.

assert_delayed_delivery(false) ->
    ok;
assert_delayed_delivery(true) ->
    receive
        {publish, Message} -> ct:fail({delivered_too_early, Message})
    after 500 -> ok
    end.

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
    {200, #{<<"data">> := Messages}} = request(get, ["mqtt", "delayed", "messages"], #{}),
    ?assertEqual(1, length(Messages)).

-doc "Check that an unrelated publish hook rejection remains an action failure.".
t_republish_blocked(Config) ->
    Client = ?config(client, Config),
    Input = <<"delayed_republish/input">>,
    Output = <<"delayed_republish/blocked">>,
    Id = create_rule(upstream, Input, Output),
    ok = emqx_hooks:put('message.publish', {?MODULE, block_publish, [Output]}, 1000),
    {ok, _, [1]} = emqtt:subscribe(Client, Output, 1),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"blocked">>, 1),
    assert_metrics(Id, 0, 1),
    assert_received_metrics(ReceivedBefore, 1),
    receive
        {publish, Message} -> ct:fail({blocked_message_delivered, Message})
    after 300 -> ok
    end.

-doc "Check that ordinary republishing without subscribers still succeeds.".
t_republish_no_subscribers(Config) ->
    Client = ?config(client, Config),
    Input = <<"delayed_republish/input">>,
    Id = create_rule(upstream, Input, <<"delayed_republish/no_subscribers">>),
    ReceivedBefore = received_metrics(),
    {ok, _} = emqtt:publish(Client, Input, <<"ordinary">>, 1),
    assert_metrics(Id, 1, 0),
    assert_received_metrics(ReceivedBefore, 2).

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
    {201, _} = request(post, ["rules"], #{
        id => Id,
        sql => SQL,
        actions => [
            #{
                function => republish,
                args => #{topic => Output, qos => 1, retain => false, payload => <<"${payload}">>}
            }
        ]
    }),
    Id.

assert_metrics(Id, Success, Failed) ->
    assert_metrics(Id, Success, Failed, 20).

assert_metrics(Id, Success, Failed, Retries) ->
    {200, #{<<"metrics">> := Metrics}} = request(get, ["rules", Id, "metrics"], #{}),
    Expected = #{
        <<"actions.total">> => Success + Failed,
        <<"actions.success">> => Success,
        <<"actions.failed">> => Failed,
        <<"actions.failed.unknown">> => Failed
    },
    Actual = maps:with(maps:keys(Expected), Metrics),
    case Actual =:= Expected orelse Retries =:= 0 of
        true ->
            ?assertEqual(Expected, Actual);
        false ->
            timer:sleep(50),
            assert_metrics(Id, Success, Failed, Retries - 1)
    end.

request(Method, Path, Body) ->
    Params =
        case Method of
            get -> [];
            delete -> [];
            _ -> Body
        end,
    emqx_mgmt_api_test_util:simple_request(
        Method, emqx_mgmt_api_test_util:api_path(Path), Params
    ).
