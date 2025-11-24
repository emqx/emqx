%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_shard_dispatch).

-include("emqx_streams_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_channel.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-export([
    on_subscription/3,
    on_unsubscription/3,
    on_publish/2,
    on_puback/3,
    on_tx_commit/4,
    on_command/3
    %% TODO
    %% on_terminate/2
]).

%% FIXME
-export([get_stream_info/2]).

-type consumer() :: emqx_types:clientid().
-type group() :: binary().
-type stream() :: binary().
% -type shard() :: binary().
% -type offset() :: non_neg_integer().

-type st() :: #{
    consumer := emqx_types:clientid(),
    {group, group()} => emqx_streams_shard_disp_group:st(),
    {stream, group()} => stream(),
    {tx, reference()} => _Context,
    pending => _Request
}.

-define(N_CONCURRENT_PROPOSALS, 1).
-define(N_CONCURRENT_TAKEOVERS, 2).
-define(REPROVISION_TIMEOUT, 1_000).
-define(REPROVISION_RETRY_TIMEOUT, 2_500).

-define(HEARTBEAT_LIFETIME, 30).
-define(ANNOUNCEMENT_LIFETIME, 15).

%% Protocol interaction

-record(ret, {reply = undefined, delivers = [], st = unchanged}).

on_subscription(ClientInfo, Topic, St) ->
    case parse_subtopic(Topic) of
        {consume, Group, Stream} ->
            Consumer = maps:get(clientid, ClientInfo),
            on_subscription_consume(Consumer, Group, Stream, St);
        false ->
            protocol_error({subscribe, Topic})
    end.

on_subscription_consume(Consumer, Group, Stream, St) ->
    SGroup = ?streamgroup(Group, Stream),
    maybe
        _Stream = undefined ?= find_stream(Group, St),
        {ok, Shards} ?= get_stream_info(Stream, shards),
        GSt = announce_myself(Consumer, SGroup, emqx_streams_shard_disp_group:new()),
        Ret = handle_provision(Consumer, Group, Stream, Shards, GSt),
        ok = channel_deliver(Ret#ret.delivers),
        {ok, set_sgroup_state(Group, Stream, Ret#ret.st, set_consumer(Consumer, St))}
    else
        Stream ->
            %% Already started.
            ok;
        StreamAnother when is_binary(StreamAnother) ->
            protocol_error({stream_mismatch, SGroup, StreamAnother});
        {error, Reason} ->
            ?tp(info, "streams_shard_dispatch_start_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Reason
            })
    end.

on_unsubscription(_ClientInfo, Topic, St) ->
    case parse_subtopic(Topic) of
        {consume, Stream, Group} ->
            ok;
        false ->
            protocol_error({subscribe, Topic})
    end.

on_publish(#message{topic = Topic} = Msg, St) ->
    case parse_topic(Topic) of
        Request = {_Command, Group, _Shard, _Offset} ->
            case sgroup_state(Group, St) of
                _GroupSt = #{} ->
                    {stop, stop_publish(Msg), St#{pending => Request}};
                undefined ->
                    ?tp(info, "streams_shard_dispatch_unexpected_message", #{
                        consumer => consumer(St),
                        topic => Topic,
                        payload => Msg#message.payload
                    }),
                    {stop, stop_publish(Msg)}
            end;
        false ->
            protocol_error(Msg)
    end.

stop_publish(Message) ->
    emqx_message:set_header(allow_publish, false, Message).

on_puback(PacketId, #message{}, St0) ->
    {Request, St1} = maps:take(pending, St0),
    Ret = on_request(PacketId, Request, St1),
    Delivers = Ret#ret.delivers,
    Delivers =/= [] andalso channel_deliver(Delivers),
    case Ret of
        #ret{reply = MaybeRC, st = St} when St =/= unchanged ->
            {stop, MaybeRC, St};
        #ret{reply = MaybeRC} ->
            {stop, MaybeRC}
    end.

on_request(PacketId, {Verb, Group, Shard, Offset}, St0) ->
    Consumer = consumer(St0),
    SGroup = sgroup(Group, St0),
    GSt0 = sgroup_state(Group, St0),
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => SGroup,
        shard => Shard,
        offset => Offset
    },
    case handle_request(Consumer, SGroup, Verb, Shard, Offset, GSt0) of
        {tx, Ref, Ctx, GSt} ->
            ?tp(debug, "streams_shard_dispatch_progress_tx_started", TraceCtx#{tx => Ref}),
            St = update_sgroup_state(Group, GSt, St0),
            #ret{st = stash_tx(Ref, {PacketId, Group, Ctx}, St)};
        Ret ->
            handle_request_outcome(Consumer, Group, Verb, Ret, TraceCtx, St0)
    end.

handle_request(Consumer, SGroup, progress, Shard, Offset, GSt0) ->
    HB = heartbeat(),
    case emqx_streams_shard_disp_group:lookup_lease(Shard, GSt0) of
        undefined ->
            {Removed, GSt} = remove_proposal({lease, Shard}, GSt0),
            %% TODO invalidate?
            Removed orelse
                ?tp(notice, "streams_shard_dispatch_leasing_no_proposal", #{
                    consumer => Consumer,
                    streamgroup => SGroup,
                    shard => Shard
                });
        _ ->
            GSt = GSt0
    end,
    emqx_streams_shard_disp_group:progress(Consumer, SGroup, Shard, Offset, HB, GSt);
handle_request(Consumer, SGroup, release, Shard, Offset, GSt0) ->
    {_Removed, GSt} = remove_proposal({release, Shard}, GSt0),
    emqx_streams_shard_disp_group:release(Consumer, SGroup, Shard, Offset, GSt).

on_tx_commit(Ref, Reply, RetAcc0 = #{replies := Replies, deliver := Deliver}, St0) ->
    Consumer = consumer(St0),
    case pop_tx(Ref, St0) of
        {{takeover, Group, Ctx}, St1} ->
            Ret = handle_takeover_tx_commit(Consumer, Group, Ref, Reply, Ctx, St1),
            case Ret of
                #ret{st = St} when St =/= unchanged ->
                    {stop, RetAcc0, St};
                #ret{} ->
                    {stop, RetAcc0}
            end;
        {{PacketId, Group, Ctx}, St1} ->
            Ret = handle_tx_commit(Consumer, Group, PacketId, Ref, Reply, Ctx, St1),
            RetAcc = #{
                replies => append_puback(PacketId, Ret#ret.reply, Replies),
                deliver => Deliver ++ Ret#ret.delivers
            },
            case Ret of
                #ret{st = St} when St =/= unchanged ->
                    {stop, RetAcc, St};
                #ret{} ->
                    {stop, RetAcc}
            end;
        error ->
            %% Not our transaction apparently:
            ok
    end.

append_puback(_PacketId, undefined, Replies) ->
    Replies;
append_puback(PacketId, RC, Replies) ->
    Replies ++ [?REPLY_OUTGOING(mk_puback(PacketId, RC))].

handle_tx_commit(Consumer, Group, PacketId, Ref, Reply, Ctx, St0) ->
    GSt0 = sgroup_state(Group, St0),
    SGroup = sgroup(Group, St0),
    Verb = element(1, Ctx),
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => SGroup,
        tx => Ref
    },
    case emqx_streams_shard_disp_group:handle_tx_reply(Consumer, SGroup, Ref, Reply, Ctx, GSt0) of
        {tx, NRef, NCtx, GSt} ->
            ?tp(debug, "streams_shard_dispatch_progress_tx_restarted", TraceCtx#{tx => NRef}),
            St = update_sgroup_state(Group, GSt, St0),
            #ret{st = stash_tx(NRef, {PacketId, Group, NCtx}, St)};
        Ret ->
            handle_request_outcome(Consumer, Group, Verb, Ret, TraceCtx, St0)
    end.

handle_request_outcome(Consumer, Group, progress, Ret, TraceCtx, St) ->
    handle_progress_outcome(Consumer, Group, Ret, TraceCtx, St);
handle_request_outcome(Consumer, Group, release, Ret, TraceCtx, St) ->
    handle_release_outcome(Consumer, Group, Ret, TraceCtx, St).

handle_progress_outcome(Consumer, Group, GSt0 = #{}, TraceCtx, St0) ->
    ?tp(debug, "streams_shard_dispatch_progress_success", TraceCtx),
    GSt = deannounce_myself(Consumer, sgroup(Group, St0), GSt0),
    St = update_sgroup_state(Group, GSt, St0),
    case GSt of
        #{proposed := []} ->
            _ = postpone_command(Group, reprovision, "no proposals");
        #{proposed := [_ | _]} ->
            ok
    end,
    #ret{reply = ?RC_SUCCESS, st = St};
handle_progress_outcome(_Consumer, Group, {invalid, Reason, GSt0}, TraceCtx, St0) ->
    %% FIXME loglevel
    ?tp(notice, "streams_shard_dispatch_progress_invalid", TraceCtx#{reason => Reason}),
    GSt = invalidate_proposals(GSt0),
    St = update_sgroup_state(Group, GSt, St0),
    case Reason of
        {leased, _} ->
            _ = schedule_command(?REPROVISION_TIMEOUT, Group, reprovision, Reason);
        conflict ->
            _ = schedule_command(?REPROVISION_TIMEOUT, Group, reprovision, Reason);
        _ ->
            ok
    end,
    #ret{reply = map_invalid_rc(Reason), st = St};
handle_progress_outcome(_Consumer, _Group, Error, TraceCtx, _St) ->
    %% FIXME error handling
    ?tp(warning, "streams_shard_dispatch_progress_error", TraceCtx#{reason => Error}),
    #ret{}.

handle_release_outcome(_Consumer, Group, GSt = #{}, TraceCtx, St0) ->
    ?tp(debug, "streams_shard_dispatch_release_success", TraceCtx),
    St = update_sgroup_state(Group, GSt, St0),
    #ret{reply = ?RC_SUCCESS, st = St};
handle_release_outcome(_Consumer, Group, {invalid, Reason, GSt0}, TraceCtx, St0) ->
    ?tp(notice, "streams_shard_dispatch_release_invalid", TraceCtx#{reason => Reason}),
    GSt = invalidate_proposals(GSt0),
    St = update_sgroup_state(Group, GSt, St0),
    #ret{reply = map_invalid_rc(Reason), st = St};
handle_release_outcome(_Consumer, _Group, Error, TraceCtx, _St) ->
    %% FIXME error handling
    ?tp(warning, "streams_shard_dispatch_release_error", TraceCtx#{reason => Error}),
    #ret{}.

map_invalid_rc({leased, _}) ->
    ?RC_NO_MATCHING_SUBSCRIBERS;
map_invalid_rc(conflict) ->
    ?RC_NO_MATCHING_SUBSCRIBERS;
map_invalid_rc(_Otherwise) ->
    ?RC_IMPLEMENTATION_SPECIFIC_ERROR.

on_command(Command, #{deliver := DeliverAcc, replies := ReplyAcc}, St0) ->
    Consumer = consumer(St0),
    Ret = handle_command(Consumer, Command, St0),
    RetAcc = #{
        deliver => DeliverAcc ++ Ret#ret.delivers,
        replies => ReplyAcc
    },
    case Ret of
        #ret{st = St} when St =/= unchanged ->
            {stop, RetAcc, St};
        #ret{} ->
            {stop, RetAcc}
    end.

handle_command(Consumer, #shard_dispatch_command{group = Group, c = reprovision}, St) ->
    Stream = maps:get({stream, Group}, St),
    Ret = reprovision(Consumer, Group, Stream, sgroup_state(Group, St)),
    case Ret of
        #ret{st = GSt} when GSt =/= unchanged ->
            Ret#ret{st = update_sgroup_state(Group, GSt, St)};
        #ret{} ->
            Ret
    end;
handle_command(Consumer, #shard_dispatch_command{group = Group, c = Takeover}, St) when
    element(1, Takeover) =:= takeover
->
    #ret{st = launch_takeover(Consumer, Group, Takeover, St)}.

handle_provision(Consumer, Group, Stream, Shards, GSt0) ->
    SGroup = ?streamgroup(Group, Stream),
    HBWatermark = timestamp_s(),
    Provisions = emqx_streams_shard_disp_group:provision(Consumer, SGroup, Shards, HBWatermark),
    Proposals = propose_leases(Provisions),
    %% NOTE piggyback
    GSt1 = GSt0#{proposed => Proposals},
    case Proposals of
        [_ | _] ->
            Offsets = emqx_streams_state_db:shard_progress_dirty(SGroup),
            Delivers = mk_proposal_delivers(Group, Stream, Proposals, Offsets),
            #ret{st = GSt1, delivers = Delivers};
        [] ->
            _ = schedule_command(?REPROVISION_RETRY_TIMEOUT, Group, reprovision, "empty provision"),
            %% Make others aware of us:
            GSt = announce_myself(Consumer, SGroup, GSt1),
            #ret{st = GSt}
    end.

reprovision(_Consumer, _Group, _Stream, #{proposed := [_ | _]}) ->
    #ret{};
reprovision(Consumer, Group, Stream, GSt0) ->
    %% FIXME error handling
    {ok, Shards} = get_stream_info(Stream, shards),
    SGroup = ?streamgroup(Group, Stream),
    HBWatermark = timestamp_s(),
    Provisions = emqx_streams_shard_disp_group:provision(Consumer, SGroup, Shards, HBWatermark),
    %% TODO too many consumers?
    case propose_leases(Provisions) of
        [_ | _] = Proposals ->
            %% NOTE piggyback
            GSt = GSt0#{proposed => Proposals},
            Offsets = emqx_streams_state_db:shard_progress_dirty(SGroup),
            Delivers = mk_proposal_delivers(Group, Stream, Proposals, Offsets),
            #ret{st = GSt, delivers = Delivers};
        [] ->
            reprovision_takeover(Consumer, Group, Stream, Shards, Provisions, GSt0)
    end.

reprovision_takeover(_Consumer, _Group, _Stream, _Shards, _Provisions, #{takeovers := [_ | _]}) ->
    #ret{};
reprovision_takeover(Consumer, Group, Stream, Shards, Provisions, GSt0) ->
    SGroup = ?streamgroup(Group, Stream),
    Takeovers = propose_takeovers(Consumer, SGroup, Shards),
    case Takeovers of
        [_ | _] ->
            %% TODO simplify
            %% TODO avoid concurrent takeovers
            lists:foreach(
                fun(Takeover) ->
                    self() ! #shard_dispatch_command{group = Group, c = Takeover}
                end,
                Takeovers
            ),
            %% NOTE piggyback
            GSt = GSt0#{takeovers => Takeovers},
            #ret{st = GSt};
        [] ->
            reprovision_release(Consumer, Group, Stream, Provisions, GSt0)
    end.

reprovision_release(Consumer, Group, Stream, Provisions, GSt0) ->
    case propose_releases(Provisions) of
        [_ | _] = Proposals ->
            %% NOTE piggyback
            GSt = GSt0#{proposed => Proposals},
            Delivers = mk_proposal_delivers(Group, Stream, Proposals, #{}),
            #ret{st = GSt, delivers = Delivers};
        [] ->
            GSt = reannounce_myself(Consumer, Group, Stream, GSt0),
            #ret{st = GSt}
    end.

remove_proposal(Proposal, GSt) ->
    Proposals = maps:get(proposed, GSt, []),
    case Proposals -- [Proposal] of
        Proposals ->
            {false, GSt};
        Rest ->
            {true, GSt#{proposed => Rest}}
    end.

invalidate_proposals(GSt) ->
    GSt#{proposed => []}.

remove_takeover(Shard, GSt) ->
    Takeovers = maps:get(takeovers, GSt, []),
    case lists:keydelete(Shard, 2, Takeovers) of
        Takeovers ->
            {false, GSt};
        Rest ->
            {true, GSt#{takeovers => Rest}}
    end.

reannounce_myself(Consumer, Group, Stream, GSt) ->
    case emqx_streams_shard_disp_group:n_leases(GSt) of
        0 ->
            %% Make others aware of us:
            _ = erlang:send_after(
                ?REPROVISION_RETRY_TIMEOUT,
                self(),
                #shard_dispatch_command{group = Group, c = reprovision, context = "reannounce"}
            ),
            announce_myself(Consumer, ?streamgroup(Group, Stream), GSt);
        _ ->
            %% Wait for rebalance:
            GSt
    end.

announce_myself(Consumer, SGroup, GSt0) ->
    Lifetime = ?ANNOUNCEMENT_LIFETIME,
    HB = timestamp_s() + Lifetime,
    case emqx_streams_shard_disp_group:announce(Consumer, SGroup, HB, Lifetime, GSt0) of
        GSt = #{} ->
            GSt;
        Error ->
            ?tp(info, "streams_shard_dispatch_announce_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            }),
            GSt0
    end.

deannounce_myself(Consumer, SGroup, GSt0) ->
    case emqx_streams_shard_disp_group:deannounce(Consumer, SGroup, GSt0) of
        GSt = #{} ->
            GSt;
        Error ->
            ?tp(info, "streams_shard_dispatch_deannounce_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            }),
            GSt0
    end.

launch_takeover(Consumer, Group, {takeover, Shard, DeadConsumer, HB}, St) ->
    SGroup = sgroup(Group, St),
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => SGroup,
        shard => Shard,
        from => DeadConsumer
    },
    %% TODO undefined?
    Offset = emqx_streams_state_db:shard_progress_dirty(SGroup, Shard),
    case emqx_streams_state_db:release_shard_async(SGroup, Shard, DeadConsumer, Offset, HB) of
        {async, Ref, Ret} ->
            ?tp(debug, "streams_shard_dispatch_takeover_tx_started", TraceCtx#{tx => Ref}),
            stash_tx(Ref, {takeover, Group, {Ret, Shard, DeadConsumer}}, St);
        Ret ->
            handle_takeover_outcome(Group, Shard, Ret, TraceCtx, St)
    end.

handle_takeover_tx_commit(Consumer, Group, Ref, Reply, {Ret, Shard, DeadConsumer}, St) ->
    TraceCtx = #{
        consumer => Consumer,
        streamgroup => sgroup(Group, St),
        shard => Shard,
        from => DeadConsumer
    },
    Ret = emqx_streams_state_db:progress_shard_tx_result(Ret, Ref, Reply),
    handle_takeover_outcome(Group, Shard, Ret, TraceCtx, St).

handle_takeover_outcome(Group, Shard, Ret, TraceCtx, St) ->
    case Ret of
        ok ->
            ?tp(debug, "streams_shard_dispatch_takeover_tx_success", TraceCtx);
        {invalid, Reason} ->
            ?tp(notice, "streams_shard_dispatch_takeover_tx_invalid", TraceCtx#{reason => Reason});
        Error ->
            ?tp(warning, "streams_shard_dispatch_takeover_tx_error", TraceCtx#{reason => Error})
    end,
    {_Removed, GSt} = remove_takeover(Shard, sgroup_state(Group, St)),
    #ret{st = update_sgroup_state(Group, GSt, St)}.

propose_leases(Provisions) ->
    ProvisionalLeases = [L || L = {lease, _} <- Provisions],
    lists:sublist(ProvisionalLeases, ?N_CONCURRENT_PROPOSALS).

propose_releases(Provisions) ->
    ProvisionalLeases = [L || L = {release, _} <- Provisions],
    lists:sublist(ProvisionalLeases, ?N_CONCURRENT_PROPOSALS).

propose_takeovers(Consumer, SGroup, Shards) ->
    HBWatermark = timestamp_s(),
    lists:sublist(
        emqx_streams_shard_disp_group:provision_takeovers(Consumer, SGroup, Shards, HBWatermark),
        ?N_CONCURRENT_TAKEOVERS
    ).

%%

protocol_error(Details) ->
    error({?MODULE, protocol_error, Details}).

mk_proposal_delivers(Group, Stream, Proposals, Offsets) ->
    DeliverTopic = mk_topic_consume(Group, [Stream]),
    lists:map(
        fun
            ({lease, S}) ->
                Offset = maps:get(S, Offsets, 0),
                #deliver{
                    topic = DeliverTopic,
                    message = emqx_message:make(mk_topic_lease(Group, Stream, S, Offset), <<>>)
                };
            ({release, S}) ->
                #deliver{
                    topic = DeliverTopic,
                    message = emqx_message:make(mk_topic_release(Group, Stream, S), <<>>)
                }
        end,
        Proposals
    ).

channel_deliver(Delivers) ->
    lists:foreach(
        fun(Deliver = #deliver{}) -> self() ! Deliver end,
        Delivers
    ).

heartbeat() ->
    timestamp_s() + ?HEARTBEAT_LIFETIME.

timestamp_s() ->
    erlang:system_time(second).

%% State

-spec set_consumer(consumer(), emqx_maybe:t(st())) -> st().
set_consumer(Consumer, St = #{}) ->
    St#{consumer => Consumer};
set_consumer(Consumer, undefined) ->
    #{consumer => Consumer}.

-spec consumer(emqx_maybe:t(st())) -> consumer() | undefined.
consumer(#{consumer := Consumer}) ->
    Consumer;
consumer(_St) ->
    undefined.

-spec sgroup(group(), st()) -> emqx_streams_shard_disp_group:streamgroup().
sgroup(Group, St) ->
    Stream = maps:get({stream, Group}, St),
    <<Group/binary, ":", Stream/binary>>.

-spec sgroup_state(group(), emqx_maybe:t(st())) ->
    emqx_streams_shard_disp_group:st() | undefined.
sgroup_state(Group, St) ->
    case St of
        #{{group, Group} := GroupSt} ->
            GroupSt;
        _ ->
            undefined
    end.

-spec set_sgroup_state(group(), stream(), emqx_streams_shard_disp_group:st(), st()) ->
    st().
set_sgroup_state(Group, Stream, GroupSt, St = #{}) ->
    St#{
        {group, Group} => GroupSt,
        {stream, Group} => Stream
    }.

-spec update_sgroup_state(group(), emqx_streams_shard_disp_group:st(), st()) ->
    st().
update_sgroup_state(Group, GroupSt, St = #{}) ->
    St#{{group, Group} := GroupSt}.

-spec find_stream(group(), emqx_maybe:t(st())) -> stream() | undefined.
find_stream(Group, St) ->
    case St of
        #{{stream, Group} := Stream} ->
            Stream;
        _ ->
            undefined
    end.

stash_tx(Ref, Context, St = #{}) ->
    K = {tx, Ref},
    false = maps:is_key(K, St),
    St#{K => Context}.

pop_tx(Ref, St = #{}) ->
    maps:take({tx, Ref}, St).

postpone_command(Group, Command, Context) ->
    self() ! #shard_dispatch_command{group = Group, c = Command, context = Context}.

schedule_command(Timeout, Group, Command, Context) ->
    erlang:send_after(
        Timeout,
        self(),
        #shard_dispatch_command{group = Group, c = Command, context = Context}
    ).

%% Protocol structures

parse_topic(Topic) when is_binary(Topic) ->
    parse_topic(emqx_topic:tokens(Topic));
parse_topic([_SDisp, VerbB, Group, Shard, OffsetB]) ->
    maybe
        Offset = int(OffsetB),
        true ?= is_integer(Offset),
        true ?= lists:member(VerbB, [<<"progress">>, <<"release">>]),
        Verb = binary_to_atom(VerbB),
        {Verb, Group, Shard, Offset}
    end;
parse_topic(_) ->
    false.

parse_subtopic(Topic) when is_binary(Topic) ->
    parse_subtopic(emqx_topic:tokens(Topic));
parse_subtopic([_SDisp, <<"consume">>, Group | StreamTokens]) ->
    Stream = emqx_topic:join(StreamTokens),
    {consume, Group, Stream};
parse_subtopic(_) ->
    false.

mk_topic_consume(Group, Suffix) ->
    emqx_topic:join([<<"$sdisp">>, <<"consume">>, Group | Suffix]).

mk_topic_lease(Group, Stream, Shard, Offset) ->
    mk_topic_consume(Group, [<<"lease">>, Shard, bin(Offset), Stream]).

mk_topic_release(Group, Stream, Shard) ->
    mk_topic_consume(Group, [<<"release">>, Shard, Stream]).

mk_puback(PacketId, RC) ->
    ?PUBACK_PACKET(PacketId, RC).

bin(V) when is_integer(V) ->
    integer_to_binary(V).

int(B) ->
    try
        binary_to_integer(B)
    catch
        error:badarg -> error
    end.

%%

get_stream_info(_Stream, shards) ->
    %% FIXME
    {ok, lists:map(fun integer_to_binary/1, lists:seq(1, 16))}.
