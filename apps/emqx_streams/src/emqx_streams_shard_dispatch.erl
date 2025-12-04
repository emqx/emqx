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
]).

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

-define(N_CONCURRENT_PROPOSALS, 2).
-define(HEARTBEAT_LIFETIME, 30).
-define(ANNOUNCEMENT_LIFETIME, 15).

-define(sgroup(GROUP, STREAM), <<GROUP/binary, ":", STREAM/binary>>).

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
    SGroup = ?sgroup(Group, Stream),
    maybe
        _Stream = undefined ?= find_stream(Group, St),
        {ok, Shards} ?= get_stream_info(Stream, shards),
        GSt0 = emqx_streams_shard_disp_group:new(),
        GSt = #{proposed := Proposals} = propose_shards(Consumer, SGroup, Shards, GSt0),
        case Proposals of
            [_ | _] ->
                Offsets = emqx_streams_state_db:shard_progress_dirty(SGroup),
                Delivers = mk_proposal_delivers(Group, Stream, Proposals, Offsets),
                channel_deliver(Delivers);
            [] ->
                case emqx_streams_shard_disp_group:n_leases(GSt) of
                    0 ->
                        %% Make others aware of us:
                        _ = announce_myself(Consumer, SGroup, GSt);
                    _ ->
                        %% Wait for rebalance:
                        ok
                end
        end,
        {ok, set_sgroup_state(Group, Stream, GSt, set_consumer(Consumer, St))}
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

propose_shards(Consumer, SGroup, Shards, GSt) ->
    HBWatermark = timestamp_s(),
    Provisions = emqx_streams_shard_disp_group:provision(Consumer, SGroup, Shards, HBWatermark),
    ProvisionalLeases = [L || L = {lease, _} <- Provisions],
    ProposedLeases = lists:sublist(ProvisionalLeases, ?N_CONCURRENT_PROPOSALS),
    %% NOTE piggyback
    GSt#{proposed => ProposedLeases}.

remove_proposal(Proposal, GSt) ->
    Proposals = maps:get(proposed, GSt, []),
    case Proposals -- [Proposal] of
        Proposals ->
            {false, GSt};
        NProposals ->
            {true, GSt#{proposed => NProposals}}
    end.

invalidate_proposals(GSt) ->
    GSt#{proposed => []}.

announce_myself(Consumer, SGroup, GSt) ->
    HB = heartbeat_announcement(),
    case emqx_streams_shard_disp_group:announce(Consumer, SGroup, HB, GSt) of
        #{} ->
            ok;
        Error ->
            ?tp(info, "streams_shard_dispatch_announce_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            })
    end.

on_unsubscription(_ClientInfo, Topic, _St) ->
    case parse_subtopic(Topic) of
        {consume, _Stream, _Group} ->
            ok;
        false ->
            protocol_error({subscribe, Topic})
    end.

on_publish(#message{topic = Topic} = Msg, St) ->
    case parse_topic(Topic) of
        Request = {_Command, Group, _Shard, _Offset} ->
            case sgroup_state(Group, St) of
                _GroupSt = #{} ->
                    {stop, Msg, St#{pending => Request}};
                undefined ->
                    ?tp(info, "streams_shard_dispatch_unexpected_message", #{
                        consumer => consumer(St),
                        topic => Topic,
                        payload => Msg#message.payload
                    }),
                    {stop, Msg}
            end;
        false ->
            protocol_error(Msg)
    end.

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

on_request(PacketId, {Command, Group, Shard, Offset}, St0) ->
    Consumer = consumer(St0),
    SGroup = sgroup(Group, St0),
    GSt0 = sgroup_state(Group, St0),
    case handle_request(Consumer, SGroup, Command, Shard, Offset, GSt0) of
        GSt = #{} ->
            %% Just in case, generally unreachable.
            St = update_sgroup_state(Group, GSt, St0),
            #ret{reply = ?RC_SUCCESS, st = St};
        {tx, Ref, Ctx, GSt} ->
            ?tp(debug, "streams_shard_dispatch_progress_tx_started", #{
                consumer => Consumer,
                streamgroup => SGroup,
                Command => {Shard, Offset},
                tx => Ref
            }),
            St = update_sgroup_state(Group, GSt, St0),
            #ret{st = stash_tx(Ref, {PacketId, Group, Ctx}, St)};
        {invalid, Reason, GSt} ->
            ?tp(info, "streams_shard_dispatch_progress_invalid_request", #{
                consumer => Consumer,
                streamgroup => SGroup,
                Command => {Shard, Offset},
                reason => Reason
            }),
            _ = on_request_error(Command, Group, Reason),
            St = update_sgroup_state(Group, invalidate_proposals(GSt), St0),
            #ret{reply = ?RC_IMPLEMENTATION_SPECIFIC_ERROR, st = St};
        Error ->
            %% FIXME error handling
            ?tp(info, "streams_shard_dispatch_progress_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                Command => {Shard, Offset},
                reason => Error
            }),
            #ret{}
    end.

handle_request(Consumer, SGroup, progress, Shard, Offset, GSt0) ->
    HB = heartbeat(),
    case emqx_streams_shard_disp_group:lookup_lease(Shard, GSt0) of
        undefined ->
            {Removed, GSt} = remove_proposal({lease, Shard}, GSt0),
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
handle_request(Consumer, SGroup, release, Shard, Offset, GSt) ->
    emqx_streams_shard_disp_group:release(Consumer, SGroup, Shard, Offset, GSt).

on_request_error(progress, Group, Reason) ->
    DispCommand = #shard_dispatch_command{group = Group, c = reprovision, context = Reason},
    case Reason of
        {leased, _} ->
            self() ! DispCommand;
        conflict ->
            self() ! DispCommand;
        _ ->
            ok
    end;
on_request_error(_, _, _) ->
    ok.

on_tx_commit(Ref, Reply, #{replies := Replies, deliver := Deliver}, St0) ->
    case pop_tx(Ref, St0) of
        {{PacketId, Group, Ctx}, St1} ->
            Consumer = consumer(St1),
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
    %% FIXME error handling
    GSt0 = sgroup_state(Group, St0),
    SGroup = sgroup(Group, St0),
    Command = element(1, Ctx),
    case emqx_streams_shard_disp_group:handle_tx_reply(Consumer, SGroup, Ref, Reply, Ctx, GSt0) of
        GSt = #{} ->
            ?tp(debug, "streams_shard_dispatch_progress_tx_success", #{
                consumer => Consumer,
                streamgroup => SGroup,
                tx => Ref,
                ctx => Ctx
            }),
            _ = on_tx_success(Command, Group, GSt),
            St = update_sgroup_state(Group, GSt, St0),
            #ret{reply = ?RC_SUCCESS, st = St};
        {tx, NRef, NCtx, GSt} ->
            ?tp(debug, "streams_shard_dispatch_progress_tx_restarted", #{
                consumer => Consumer,
                streamgroup => SGroup,
                tx => NRef
            }),
            St = update_sgroup_state(Group, GSt, St0),
            #ret{st = stash_tx(NRef, {PacketId, Group, NCtx}, St)};
        {invalid, Reason, GSt} ->
            ?tp(info, "streams_shard_dispatch_progress_tx_invalid", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Reason
            }),
            _ = on_request_error(Command, Group, Reason),
            St = update_sgroup_state(Group, invalidate_proposals(GSt), St0),
            #ret{reply = ?RC_IMPLEMENTATION_SPECIFIC_ERROR, st = St};
        Error ->
            ?tp(info, "streams_shard_dispatch_progress_tx_error", #{
                consumer => Consumer,
                streamgroup => SGroup,
                reason => Error
            }),
            #ret{}
    end.

on_tx_success(progress, Group, #{proposed := []}) ->
    self() ! #shard_dispatch_command{group = Group, c = reprovision, context = no_more_proposals};
on_tx_success(progress, _SGroup, #{proposed := [_ | _]}) ->
    ok;
on_tx_success(release, _SGroup, #{}) ->
    ok.

on_command(#shard_dispatch_command{group = Group, c = reprovision}, RetAcc0, St0) ->
    Consumer = consumer(St0),
    Stream = maps:get({stream, Group}, St0),
    GSt0 = sgroup_state(Group, St0),
    Ret = handle_reprovision(Consumer, Group, Stream, GSt0),
    RetAcc = RetAcc0#{
        delivers => Ret#ret.delivers
    },
    case Ret of
        #ret{st = St} when St =/= unchanged ->
            {stop, RetAcc, St};
        #ret{} ->
            {stop, RetAcc}
    end.

handle_reprovision(Consumer, Group, Stream, GSt0) ->
    %% FIXME error handling
    {ok, Shards} = get_stream_info(Stream, shards),
    SGroup = ?sgroup(Group, Stream),
    GSt = #{proposals := Proposals} = propose_shards(Consumer, SGroup, Shards, GSt0),
    case Proposals of
        [_ | _] ->
            Offsets = emqx_streams_state_db:shard_progress_dirty(SGroup),
            Delivers = mk_proposal_delivers(Group, Stream, Proposals, Offsets),
            #ret{st = GSt, delivers = Delivers};
        [] ->
            #ret{st = GSt}
    end.

%%

-dialyzer([{nowarn_function, [protocol_error/1]}]).
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

heartbeat_announcement() ->
    timestamp_s() + ?ANNOUNCEMENT_LIFETIME.

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

%% Protocol structures

parse_topic(Topic) when is_binary(Topic) ->
    parse_topic(emqx_topic:tokens(Topic));
parse_topic([_SDisp, CommandB, Group, Shard, OffsetB]) ->
    maybe
        Offset = int(OffsetB),
        true ?= is_integer(Offset),
        true ?= lists:member(CommandB, [<<"progress">>, <<"release">>]),
        Command = binary_to_atom(CommandB),
        {Command, Group, Shard, Offset}
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
