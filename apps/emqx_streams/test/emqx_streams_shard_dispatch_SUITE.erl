%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_shard_dispatch_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-import(emqx_common_test_helpers, [on_exit/1, call_janitor/0]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, """
            durable_storage.streams_states {
                backend = builtin_local
                n_shards = 4
            }
            """},
            {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
            {emqx_streams, """
            streams.enable = true
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_streams_app:wait_readiness(5_000),
    ok = emqx_streams:register_sdisp_hooks(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_streams:unregister_sdisp_hooks(),
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    _ = call_janitor(),
    ok.

%%

-define(group, atom_to_binary(?FUNCTION_NAME)).

-define(tok_sdisp, <<"$sdisp">>).
-define(tok_consume, <<"consume">>).

t_consumer_single(_Config) ->
    C = emqtt_connect(<<"Cons1">>),
    Group = ?group,
    Stream = <<"t/#">>,
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _, [?RC_GRANTED_QOS_1]},
                emqtt:subscribe(C, mk_topic_consume(Group, Stream), qos1)
            ),
            {publish, #{topic := TopicLease1}} = ?assertReceive({publish, #{}}),
            {lease, Shard1, OffsetS1} = parse_topic(Group, Stream, TopicLease1),
            TopicProgress1 = mk_topic_progress(Group, Shard1, OffsetS1 + 1),
            ?assertMatch(
                {ok, #{reason_code := ?RC_SUCCESS}},
                emqtt:publish(C, TopicProgress1, <<>>, ?QOS_1)
            )
        end,
        fun(Trace) ->
            ct:pal("~p", [Trace])
        end
    ).

emqtt_connect(ClientID) ->
    {ok, CPid} = emqtt:start_link(#{clientid => ClientID}),
    case emqtt:connect(CPid) of
        {ok, _ConnAck} ->
            on_exit(fun() -> catch emqtt:stop(CPid) end),
            unlink(CPid),
            CPid;
        {error, Reason} ->
            error(Reason)
    end.

parse_topic(Group, Stream, Topic) ->
    StreamTokens = emqx_topic:tokens(Stream),
    case emqx_topic:tokens(Topic) of
        [?tok_sdisp, ?tok_consume, Group, <<"lease">>, Shard, OffsetB | StreamTokens] ->
            {lease, Shard, binary_to_integer(OffsetB)};
        [?tok_sdisp, ?tok_consume, Group, <<"release">>, Shard | StreamTokens] ->
            {release, Shard};
        _ ->
            error({unexpected_topic, Topic})
    end.

mk_topic_consume(Group, Stream) ->
    emqx_topic:join([?tok_sdisp, ?tok_consume, Group, Stream]).

mk_topic_progress(Group, Shard, Offset) ->
    emqx_topic:join([?tok_sdisp, <<"progress">>, Group, Shard, integer_to_binary(Offset)]).
