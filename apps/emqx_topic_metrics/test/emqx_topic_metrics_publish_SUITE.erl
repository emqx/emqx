%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_topic_metrics_publish_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_config.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            {emqx, #{override_env => [{boot_modules, [broker]}]}},
            emqx_topic_metrics
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_Case, Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = emqx_topic_metrics2:deregister_all(),
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_publish_increments_global(_Config) ->
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    publish(<<"a/1">>, ?QOS_1, undefined),
    ?assertMatch(
        {ok, #{
            metrics := #{
                'messages.in.count' := 1
            }
        }},
        emqx_topic_metrics2:lookup(<<"a">>, ?global_ns)
    ).

t_wildcard_match_fans_out(_Config) ->
    %% One publish hits two overlapping collections.
    ok = emqx_topic_metrics2:register(<<"any">>, <<"a/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"exact">>, <<"a/+/x">>, ?global_ns),
    publish(<<"a/foo/x">>, ?QOS_0, undefined),
    {ok, #{metrics := #{'messages.in.count' := 1}}} =
        emqx_topic_metrics2:lookup(<<"any">>, ?global_ns),
    {ok, #{metrics := #{'messages.in.count' := 1}}} =
        emqx_topic_metrics2:lookup(<<"exact">>, ?global_ns).

t_namespace_isolation(_Config) ->
    ok = emqx_topic_metrics2:register(<<"g">>, <<"a/#">>, ?global_ns),
    ok = emqx_topic_metrics2:register(<<"ns_a">>, <<"a/#">>, <<"acme">>),
    ok = emqx_topic_metrics2:register(<<"ns_b">>, <<"a/#">>, <<"bravo">>),

    %% Publisher in namespace "acme" hits both global and acme-owned
    %% collections; never the bravo-owned one.
    publish(<<"a/1">>, ?QOS_1, <<"acme">>),

    {ok, #{metrics := #{'messages.in.count' := G}}} =
        emqx_topic_metrics2:lookup(<<"g">>, ?global_ns),
    {ok, #{metrics := #{'messages.in.count' := A}}} =
        emqx_topic_metrics2:lookup(<<"ns_a">>, <<"acme">>),
    {ok, #{metrics := #{'messages.in.count' := B}}} =
        emqx_topic_metrics2:lookup(<<"ns_b">>, <<"bravo">>),
    ?assertEqual({1, 1, 0}, {G, A, B}),

    %% Publisher with no namespace hits only the global one.
    publish(<<"a/2">>, ?QOS_0, undefined),
    {ok, #{metrics := #{'messages.in.count' := 2}}} =
        emqx_topic_metrics2:lookup(<<"g">>, ?global_ns),
    {ok, #{metrics := #{'messages.in.count' := 1}}} =
        emqx_topic_metrics2:lookup(<<"ns_a">>, <<"acme">>),
    {ok, #{metrics := #{'messages.in.count' := 0}}} =
        emqx_topic_metrics2:lookup(<<"ns_b">>, <<"bravo">>).

t_delivered_and_dropped(_Config) ->
    ok = emqx_topic_metrics2:register(<<"a">>, <<"a/#">>, ?global_ns),
    Msg = make_msg(<<"a/1">>, ?QOS_1, undefined),
    ok = emqx_topic_metrics_hooks:on_message_delivered(undefined, Msg),
    ok = emqx_topic_metrics_hooks:on_message_dropped(Msg, undefined, normal),
    {ok, #{
        metrics := #{
            'messages.out.count' := 1,
            'messages.dropped.count' := 1,
            'messages.in.count' := 0
        }
    }} = emqx_topic_metrics2:lookup(<<"a">>, ?global_ns).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

publish(Topic, QoS, Tns) ->
    Msg = make_msg(Topic, QoS, Tns),
    %% Invoke the hook callback directly so the test is independent
    %% of broker/dispatcher state; the hook code is what matters.
    ok = emqx_topic_metrics_hooks:on_message_publish(Msg).

make_msg(Topic, QoS, Tns) ->
    Msg0 = emqx_message:make(<<"client">>, QoS, Topic, <<"payload">>),
    Headers = #{client_attrs => client_attrs(Tns)},
    Msg0#message{headers = maps:merge(Msg0#message.headers, Headers)}.

client_attrs(undefined) -> #{};
client_attrs(Tns) when is_binary(Tns) -> #{<<"tns">> => Tns}.
