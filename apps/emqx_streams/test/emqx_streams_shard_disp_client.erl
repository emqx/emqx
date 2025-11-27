%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_shard_disp_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").

%%

start_link(ClientID, Group, Stream, Opts) ->
    proc_lib:start_link(?MODULE, before_loop, [ClientID, Group, Stream, Opts]).

stop(Pid) ->
    gen:stop(Pid, normal, infinity).

%%

system_continue(_Parent, _Debug, St) ->
    ?MODULE:loop(St).

system_terminate(_Reason, _Parent, _Debug, St) ->
    terminate(St).

%%

-define(tok_sdisp, <<"$sdisp">>).
-define(tok_consume, <<"consume">>).

before_loop(ClientID, Group, Stream, Opts) ->
    {ok, CPid} = emqtt:start_link(#{
        clientid => ClientID,
        proto_ver => v5
    }),
    case emqtt:connect(CPid) of
        {ok, _ConnAck} ->
            St = #{
                connection => CPid,
                clientid => ClientID,
                group => Group,
                stream => Stream,
                opts => Opts
            },
            trace(St, "[START] ~s:~s", [Group, Stream]),
            proc_lib:init_ack({ok, self()}),
            erlang:process_flag(trap_exit, true),
            loop(subscribe_consumer(St));
        {error, Reason} ->
            exit({connect_error, Reason})
    end.

subscribe_consumer(St = #{group := Group, stream := Stream}) ->
    case mqtt_subscribe(St, mk_topic_consume(Group, Stream), qos1) of
        ?RC_GRANTED_QOS_1 ->
            St;
        RC ->
            exit({subscribe_denied, emqx_reason_codes:name(RC)})
    end.

loop(St) ->
    TickInterval = opt(tick_interval, St),
    receive
        {publish, Message = #{topic := Topic, qos := QoS, payload := Payload}} ->
            trace(St, "[MQTT] << PUB ~s qos:~0p pl:~0p", [Topic, QoS, Payload]),
            handle_message(Message, St);
        {system, From, Req} ->
            [Parent | _] = get('$ancestors'),
            sys:handle_system_msg(Req, From, Parent, ?MODULE, _Debug = [], St)
    after TickInterval ->
        trace(St, "[TICK] shards:~s", [fmt_shards(list_shards(St))]),
        handle_tick(St)
    end.

terminate(St) ->
    Shards = list_shards(St),
    trace(St, "[TERMINATE] shards:~s", [fmt_shards(Shards)]),
    lists:foldl(fun try_release_shard/2, St, Shards).

handle_message(#{topic := Topic, payload := <<>>}, St) ->
    #{group := Group, stream := Stream} = St,
    handle_proposal(parse_topic(Group, Stream, Topic), St);
handle_message(Msg, _St) ->
    exit({unexpected_payload, Msg}).

handle_proposal({lease, Shard, Offset}, St) ->
    trace(St, "[PROPOSAL] << LEASE ~s offs:~p", [Shard, Offset]),
    case shard(Shard, St) of
        undefined ->
            handle_proposed_lease(Shard, Offset, St);
        _ShardSt ->
            exit({unexpected_lease, Shard, Offset})
    end;
handle_proposal({release, Shard}, St) ->
    trace(St, "[PROPOSAL] << RELEASE ~s", [Shard]),
    case shard(Shard, St) of
        ShardSt = #{} ->
            handle_proposed_release(Shard, ShardSt, St);
        undefined ->
            exit({unexpected_release, Shard})
    end.

handle_proposed_lease(Shard, Offset, St) ->
    ShardSt = #{
        proposed_at => rfc3339(nowts()),
        leased_at => undefined,
        last_progress_at => undefined,
        offset => Offset
    },
    loop(progress_shard(Shard, set_shard(Shard, ShardSt, St))).

handle_proposed_release(Shard, ShardSt, St) ->
    loop(release_shard(Shard, ShardSt, St)).

handle_tick(St) ->
    Deadline = nowts() - opt(progress_update_interval, St),
    case find_stale_shard(Deadline, St) of
        ok ->
            loop(St);
        Shard ->
            loop(progress_shard(Shard, St))
    end.

progress_shard(Shard, St) ->
    ShardSt = shard(Shard, St),
    OffsetNext = next_offset(ShardSt),
    Topic = mk_topic_progress(group(St), Shard, OffsetNext),
    case mqtt_publish(St, Topic, <<>>, ?QOS_1) of
        #{reason_code := ?RC_SUCCESS} ->
            trace(St, "[PROGRESS] << ~s ~s offs:~p", [mrk_progress(ShardSt), Shard, OffsetNext]),
            set_shard(Shard, update_offset(OffsetNext, ShardSt), St);
        #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS} ->
            trace(St, "[PROGRESS] << CONFLICT ~s", [Shard]),
            unset_shard(Shard, St);
        #{reason_code := RC} ->
            exit({lease_progress_denied, emqx_reason_codes:name(RC)})
    end.

release_shard(Shard, ShardSt, St) ->
    Offset = offset(ShardSt),
    Topic = mk_topic_release(group(St), Shard, Offset),
    case mqtt_publish(St, Topic, <<>>, ?QOS_1) of
        #{reason_code := ?RC_SUCCESS} ->
            trace(St, "[PROGRESS] << RELEASED ~s offs:~p", [Shard, Offset]),
            unset_shard(Shard, St);
        #{reason_code := RC} ->
            exit({release_denied, emqx_reason_codes:name(RC)})
    end.

try_release_shard(Shard, St) ->
    case shard(Shard, St) of
        ShardSt = #{leased_at := Ts} when Ts =/= undefined ->
            release_shard(Shard, ShardSt, St);
        #{} ->
            St
    end.

% schedule_progress(Shard, _St) ->
%     erlang:send_after(?SHARD_PROGRESS_INTERVAL, self(), {progress, Shard}).

find_stale_shard(Deadline, St) ->
    catch maps:foreach(
        fun
            ({shard, Shard}, ShardSt) ->
                last_progress(ShardSt) < Deadline andalso throw(Shard);
            (_, _) ->
                ok
        end,
        St
    ).

conn(#{connection := CPid}) ->
    CPid.

group(#{group := Group}) ->
    Group.

opt(Opt, #{opts := Opts}) ->
    maps:get(Opts, Opts, opt_default(Opt)).

opt_default(tick_interval) ->
    1_000;
opt_default(progress_update_interval) ->
    3_000.

shard(Shard, St) ->
    maps:get({shard, Shard}, St, undefined).

set_shard(Shard, ShardSt, St) ->
    maps:put({shard, Shard}, ShardSt, St).

unset_shard(Shard, St) ->
    maps:remove({shard, Shard}, St).

list_shards(St) ->
    [Shard || {shard, Shard} <- maps:keys(St)].

offset(#{offset := Offset}) ->
    Offset.

next_offset(#{offset := Offset}) ->
    Offset + 1.

update_offset(Offset, ShardSt) ->
    ensure_leased_at(
        ShardSt#{
            last_progress_at := rfc3339(nowts()),
            offset := Offset
        }
    ).

last_progress(#{last_progress_at := undefined}) ->
    0;
last_progress(#{last_progress_at := Ts}) ->
    parse_rfc3339(Ts).

ensure_leased_at(ShardSt = #{leased_at := undefined}) ->
    ShardSt#{leased_at := rfc3339(nowts())};
ensure_leased_at(ShardSt) ->
    ShardSt.

%%

mqtt_subscribe(St, Topic, QoS) ->
    trace(St, "[MQTT] >> SUB ~s qos:~0p", [Topic, QoS]),
    case emqtt:subscribe(conn(St), Topic, QoS) of
        {ok, _Props, [RC]} ->
            trace(St, "[MQTT] << SUBACK ~p", [emqx_reason_codes:name(RC)]),
            RC;
        {error, Reason} ->
            exit({subscribe_error, Reason})
    end.

mqtt_publish(St, Topic, Payload, QoS) ->
    trace(St, "[MQTT] >> PUB ~s qos:~0p pl:~0p", [Topic, QoS, Payload]),
    case emqtt:publish(conn(St), Topic, Payload, QoS) of
        {ok, Ack = #{reason_code_name := RCN}} ->
            trace(St, "[MQTT] << PUBACK ~p pkt:~p", [RCN, maps:get(packet_id, Ack, undefined)]),
            Ack;
        {error, Reason} ->
            exit({publish_error, Reason})
    end.

trace(St, Fmt, Args) ->
    io:format(user, "~ts (~s) " ++ Fmt ++ "~n", [rfc3339(nowts()), maps:get(clientid, St) | Args]).

fmt_shards(Shards) when is_list(Shards) ->
    ["[", lists:join(", ", Shards), "]"].

mrk_progress(#{last_progress_at := undefined}) ->
    "LEASED";
mrk_progress(#{last_progress_at := _Ts}) ->
    "OK".

parse_topic(Group, Stream, Topic) ->
    StreamTokens = emqx_topic:tokens(Stream),
    case emqx_topic:tokens(Topic) of
        [?tok_sdisp, ?tok_consume, Group, <<"lease">>, Shard, OffsetB | StreamTokens] ->
            {lease, Shard, binary_to_integer(OffsetB)};
        [?tok_sdisp, ?tok_consume, Group, <<"release">>, Shard | StreamTokens] ->
            {release, Shard};
        _ ->
            %% TODO
            error({unexpected_topic, Topic})
    end.

mk_topic_consume(Group, Stream) ->
    emqx_topic:join([?tok_sdisp, ?tok_consume, Group, Stream]).

mk_topic_progress(Group, Shard, Offset) ->
    emqx_topic:join([?tok_sdisp, <<"progress">>, Group, Shard, integer_to_binary(Offset)]).

mk_topic_release(Group, Shard, Offset) ->
    emqx_topic:join([?tok_sdisp, <<"release">>, Group, Shard, integer_to_binary(Offset)]).

%%

rfc3339(Timestamp) ->
    calendar:system_time_to_rfc3339(Timestamp, [{unit, millisecond}, {offset, "Z"}]).

nowts() ->
    erlang:system_time(millisecond).

parse_rfc3339(String) ->
    calendar:rfc3339_to_system_time(String, [{unit, millisecond}]).
