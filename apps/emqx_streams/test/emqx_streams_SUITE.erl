%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_streams_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                emqx_durable_storage,
                {emqx, emqx_streams_test_utils:cth_config(emqx)},
                {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
                {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_streams_test_utils:cleanup_streams(),
    ok = emqx_streams_test_utils:reset_config().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

% t_smoke(_Config) ->
%     Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
%     ok = emqx_streams_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
%     AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
%     ?assertEqual(10, length(AllMessages)),
%     ok.

t_read_earliest(_Config) ->
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/earliest/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/earliest/t/#">>),

    {ok, _Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout1 = 500),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, _Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500),

    ok = emqtt:disconnect(CSub).

t_read_latest(_Config) ->
    _Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/latest/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/latest/t/#">>),

    {ok, Msgs0} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 0, _Timeout1 = 500),
    ?assertEqual(0, length(Msgs0)),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),
    {ok, _Msgs1} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 500).

t_read_offset(_Config) ->
    Stream = emqx_streams_test_utils:create_stream(#{topic_filter => <<"t/#">>}),
    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    AllMessages = emqx_streams_message_db:dirty_read_all(Stream),
    ?assertEqual(50, length(AllMessages)),
    {_, Offset, _} = lists:last(AllMessages),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    CSub = emqx_streams_test_utils:emqtt_connect([]),
    OffsetBin = integer_to_binary(Offset),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"0/", OffsetBin/binary, "/t/#">>),
    ok = emqx_streams_test_utils:emqtt_sub_stream(CSub, <<"1/", OffsetBin/binary, "/t/#">>),

    {ok, _} = emqx_streams_test_utils:emqtt_drain(_MinMsg0 = 50, _Timeout0 = 1000),

    ok = emqx_streams_test_utils:populate(50, #{topic_prefix => <<"t/">>, different_clients => true}),

    {ok, _} = emqx_streams_test_utils:emqtt_drain(_MinMsg1 = 50, _Timeout1 = 1000).
