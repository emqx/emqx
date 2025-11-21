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

-include("../src/emqx_streams_internal.hrl").

-import(emqx_common_test_helpers, [on_exit/1, call_janitor/0]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, """
            durable_storage.streams_states {
                backend = builtin_local
                n_shards = 4
                transaction.idle_flush_interval = "0ms"
            }
            """},
            {emqx_streams, """
            streams.enable = true
            """}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok = emqx_streams_app:wait_readiness(5_000),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

%%

-define(group, atom_to_binary(?FUNCTION_NAME)).

-define(tok_sdisp, <<"$sdisp">>).
-define(tok_consume, <<"consume">>).

t_consumer_smoke(_Config) ->
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
            ),
            ?assertMatch(
                {ok, #{reason_code := ?RC_IMPLEMENTATION_SPECIFIC_ERROR}},
                emqtt:publish(C, mk_topic_progress(Group, Shard1, OffsetS1), <<>>, ?QOS_1)
            )
        end,
        fun(Trace) ->
            ct:pal("~p", [Trace])
        end
    ).

t_concurrent_consumers(_Config) ->
    Group = ?group,
    Stream = <<"t/#">>,
    ?check_trace(
        begin
            trace("------------ Starting 3 consumers", []),
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := "sdisp_group_progress", leased := false}),
                _NShards = 16,
                5_000
            ),

            {ok, C1} = emqx_streams_shard_disp_client:start_link(<<"XX">>, Group, Stream, #{}),
            {ok, C2} = emqx_streams_shard_disp_client:start_link(<<"YY">>, Group, Stream, #{}),
            {ok, C3} = emqx_streams_shard_disp_client:start_link(<<"ZZ">>, Group, Stream, #{}),

            ?assertMatch({ok, _Events}, snabbkaffe:receive_events(SRef)),

            trace("------------ All shards are allocated", []),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Gracefully stopping consumer 1", []),
            emqx_streams_shard_disp_client:stop(C1),

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Gracefully stopping consumer 2", []),
            emqx_streams_shard_disp_client:stop(C2),

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Gracefully stopping consumer 3", []),
            emqx_streams_shard_disp_client:stop(C3),

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Restarting consumers 1 and 2", []),
            {ok, C11} = emqx_streams_shard_disp_client:start_link(<<"XX">>, Group, Stream, #{}),
            {ok, C21} = emqx_streams_shard_disp_client:start_link(<<"YY">>, Group, Stream, #{}),

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Starting consumer 3", []),
            {ok, C31} = emqx_streams_shard_disp_client:start_link(<<"ZZ">>, Group, Stream, #{}),

            ok = timer:sleep(10_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))]),

            trace("------------ Stopping consumers", []),
            emqx_streams_shard_disp_client:stop(C11),
            emqx_streams_shard_disp_client:stop(C21),
            emqx_streams_shard_disp_client:stop(C31),

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))])
        end,
        []
    ).

            ok = timer:sleep(5_000),
            trace("[SGROUP] ~s ~s~n~s", [Stream, Group, indent(30, format_sgroup_st(Group, Stream))])

        end,
        fun(Trace) ->
            ct:pal("~p", [Trace])
        end
    ).

emqtt_connect(ClientID) ->
    {ok, CPid} = emqtt:start_link(#{clientid => ClientID, proto_ver => v5}),
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

%%

trace(Fmt, Args) ->
    io:format(user, "~ts (ct) " ++ Fmt ++ "~n", [rfc3339(nowts()) | Args]).

indent(S, Lines) ->
    Indent = lists:duplicate(S, $\s),
    [[Indent | L] || L <- Lines].

rfc3339(Timestamp) ->
    calendar:system_time_to_rfc3339(Timestamp, [{unit, millisecond}, {offset, "Z"}]).

nowts() ->
    erlang:system_time(millisecond).

format_sgroup_st(Group, Stream) ->
    SGroup = ?streamgroup(Group, Stream),
    TS = erlang:system_time(second),
    {ok, Shards} = emqx_streams_shard_dispatch:get_stream_info(Stream, shards),
    {ConsumerHBs, Leases} = emqx_streams_state_db:shard_leases_dirty(SGroup),
    Consumers = maps:keys(ConsumerHBs),
    Alloc = current_allocation(Consumers, Leases, Shards),
    [
        [
            "(",
            C,
            ") ",
            format_hb(maps:get(C, ConsumerHBs), TS),
            " ",
            format_leases(emqx_streams_allocation:lookup_member(C, Alloc)),
            "\n"
        ]
     || C <- lists:sort(Consumers)
    ].

format_hb(HB, TS) ->
    [
        "hb:",
        calendar:system_time_to_rfc3339(HB, [{unit, second}, {offset, "Z"}]),
        case HB < TS of
            true -> " [DEAD]";
            false -> "       "
        end
    ].

format_leases(Shards) when is_list(Shards) ->
    ["shards:[", lists:join(", ", Shards), "]"];
format_leases(_) ->
    ["shards:-"].

current_allocation(Consumers, Leases, Shards) ->
    maps:fold(
        fun(Shard, Consumer, Alloc0) ->
            Alloc = emqx_streams_allocation:add_member(Consumer, Alloc0),
            emqx_streams_allocation:occupy_resource(Shard, Consumer, Alloc)
        end,
        emqx_streams_allocation:new(Shards, Consumers),
        Leases
    ).
