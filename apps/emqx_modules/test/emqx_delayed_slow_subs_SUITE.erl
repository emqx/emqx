%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_delayed_slow_subs_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(DELAY, 3).
-define(DELAY_MS, 3000).
-define(SLOW_THRESHOLD_MS, 1000).
-define(SLOW_SUBS_CONF, <<
    "slow_subs {\n"
    "    enable = true\n"
    "    top_k_num = 10\n"
    "    threshold = 1000ms\n"
    "    expire_interval = 60s\n"
    "    stats_type = whole\n"
    "}"
>>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_modules,
            {emqx_slow_subs, ?SLOW_SUBS_CONF}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok = emqx_delayed:load(),
    ok = emqx_slow_subs:clear_history(),
    ok = emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, [self()]}, -1000),
    ok = emqx_hooks:add('message.publish', {?MODULE, on_publish, [self()]}, -1000),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    ok = emqx_hooks:del('message.publish', {?MODULE, on_publish}),
    ok = emqx_delayed:unload(),
    {atomic, ok} = mria:clear_table(emqx_delayed),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_delayed_publish_not_counted_as_slow(_Config) ->
    lists:foreach(fun verify_delay_not_counted/1, [whole, internal]).

t_republish_timestamp_refreshed(_Config) ->
    Topic = stats_topic(timestamp_refresh),
    Original = publish_delayed(Topic, <<"payload">>),
    Republished =
        receive
            {published, #message{topic = Topic} = Msg} -> Msg
        after ?DELAY_MS * 4 ->
            ct:fail(republish_timeout)
        end,
    Elapsed = Republished#message.timestamp - Original#message.timestamp,
    ?assert(Elapsed >= ?DELAY_MS - 100),
    ?assert(Elapsed < 10000),
    ?assertEqual(Original#message.id, Republished#message.id),
    ?assertEqual(Original#message.from, Republished#message.from),
    ?assertEqual(Original#message.qos, Republished#message.qos),
    ?assertEqual(Original#message.payload, Republished#message.payload),
    ?assertEqual(Original#message.flags, Republished#message.flags),
    ?assertEqual(Original#message.headers, Republished#message.headers),
    ok.

t_real_delivery_latency_still_counted(_Config) ->
    Topic = stats_topic(real_latency),
    _ = set_slow_subs(whole, 100),
    with_subscriber(Topic, 500, fun() ->
        %% Keep the observed publish time strictly after the session birth time.
        timer:sleep(2000),
        publish_delayed(Topic, <<"delayed">>),
        await_delivered(Topic, <<"delayed">>),
        [#{topic := Topic, timespan := TimeSpan}] = emqx_slow_subs_api:get_history(),
        ?assert(TimeSpan >= 400),
        ?assert(TimeSpan < 2000)
    end).

%%--------------------------------------------------------------------
%% Test case helpers
%%--------------------------------------------------------------------

verify_delay_not_counted(StatsType) ->
    Topic = stats_topic(StatsType),
    _ = set_slow_subs(StatsType, ?SLOW_THRESHOLD_MS),
    with_subscriber(Topic, 0, fun() ->
        %% Keep the observed publish time strictly after the session birth time.
        timer:sleep(2000),
        %% Sanity check: a genuinely slow delivery must still be recorded,
        %% otherwise the negative assertion below is vacuous.
        publish(Topic, <<"sanity">>, erlang:system_time(millisecond) - 1500),
        await_delivered(Topic, <<"sanity">>),
        ?assert(
            lists:any(
                fun(#{timespan := TimeSpan}) -> TimeSpan >= ?SLOW_THRESHOLD_MS end,
                emqx_slow_subs_api:get_history()
            )
        ),
        ok = emqx_slow_subs:clear_history(),
        %% The configured delay must not be accounted as delivery latency.
        publish_delayed(Topic, <<"delayed">>),
        await_delivered(Topic, <<"delayed">>),
        ?assertEqual([], emqx_slow_subs_api:get_history())
    end).

with_subscriber(Topic, ResponseDelay, Fun) ->
    {ok, Client} = emqtt:start_link([
        {clientid, <<"delayed_slow_subs_subscriber">>},
        {msg_handler, #{
            publish => fun(_) -> timer:sleep(ResponseDelay) end,
            disconnected => fun(_) -> ok end
        }}
    ]),
    try
        {ok, _} = emqtt:connect(Client),
        {ok, _, [1]} = emqtt:subscribe(Client, Topic, 1),
        Fun()
    after
        emqtt:stop(Client)
    end.

publish(Topic, Payload, Timestamp) ->
    Msg = emqx_message:make(?MODULE, 1, Topic, Payload),
    emqx:publish(Msg#message{timestamp = Timestamp}).

publish_delayed(Topic, Payload) ->
    DelayedTopic = <<"$delayed/", (integer_to_binary(?DELAY))/binary, "/", Topic/binary>>,
    Msg = emqx_message:make(?MODULE, 1, DelayedTopic, Payload),
    emqx:publish(Msg),
    Msg.

await_delivered(Topic, Payload) ->
    receive
        {delivered, Topic, Payload} -> ok
    after ?DELAY_MS * 4 ->
        ct:fail({delivery_timeout, Topic, Payload})
    end.

set_slow_subs(StatsType, ThresholdMs) ->
    emqx_slow_subs:update_settings(#{
        <<"enable">> => true,
        <<"expire_interval">> => <<"60s">>,
        <<"stats_type">> => atom_to_binary(StatsType, utf8),
        <<"threshold">> => <<(integer_to_binary(ThresholdMs))/binary, "ms">>,
        <<"top_k_num">> => 10
    }).

stats_topic(StatsType) ->
    <<"delayed_slow_subs/", (atom_to_binary(StatsType, utf8))/binary>>.

%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------

on_delivery_completed(#message{topic = Topic, payload = Payload}, _Env, TestPid) ->
    TestPid ! {delivered, Topic, Payload},
    ok.

on_publish(Msg, TestPid) ->
    TestPid ! {published, Msg},
    {ok, Msg}.
