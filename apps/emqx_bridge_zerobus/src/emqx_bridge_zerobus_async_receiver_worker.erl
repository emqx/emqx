%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_async_receiver_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    where/2,
    follow_stream/5
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-moduledoc """
This worker receives gRPC client streams opened and managed by the stream writer, and
continually receives replies from it, since grpc_client does not have an "active" receive
option.  It sends replies to the stream writer, so the latter may reply to callers and
track the last acked sequence number.
""".

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_zerobus.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-define(name(ACTIONRESID, IDX), {n, l, {?MODULE, ACTIONRESID, IDX}}).
-define(via(ACTIONRESID, IDX), {via, gproc, ?name(ACTIONRESID, IDX)}).

%% calls/casts/infos/continues
-record(follow_stream, {writer, n_restarts, stream}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    #{action_res_id := ActionResId, idx := Idx} = Opts,
    gen_server:start_link(?via(ActionResId, Idx), ?MODULE, Opts, []).

where(Pool, Idx) ->
    gproc_pool:whereis_worker(Pool, {Pool, Idx}).

follow_stream(Pool, Idx, Writer, NRestarts, Stream) ->
    Pid = where(Pool, Idx),
    try
        gen_server:call(
            Pid,
            #follow_stream{
                writer = Writer,
                n_restarts = NRestarts,
                stream = Stream
            },
            infinity
        )
    catch
        exit:{noproc, _} ->
            %% really rare race condition: if the receiver is dead or dying as we make the
            %% call.  since the writer registers the stream before calling the receiver
            %% here, the receiver will recover using that state.
            ?tp("zerobus_receiver_dead_follow_stream", #{}),
            {ok, {-1, -1}}
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    process_flag(trap_exit, true),
    #{
        action_res_id := ActionResId,
        pool := Pool,
        idx := Idx
    } = Opts,
    true = gproc_pool:connect_worker(Pool, {Pool, Idx}),
    #{
        ?writer := Writer,
        ?n_restarts := NRestarts,
        ?stream := Stream,
        ?recv_handle := Handle
    } = recover(ActionResId, Idx),
    State = #{
        ?action_res_id => ActionResId,
        ?idx => Idx,
        ?pool => Pool,
        ?writer => Writer,
        ?n_restarts => NRestarts,
        ?last_acked_seq => -1,
        ?stream => Stream,
        ?recv_handle => Handle
    },
    {ok, State}.

terminate(_Reason, State) ->
    #{?pool := Pool, ?idx := Id} = State,
    gproc_pool:disconnect_worker(Pool, {Pool, Id}),
    ok.

handle_call(#follow_stream{} = FollowReq, _From, State0) ->
    {Reply, State} = handle_follow_stream(FollowReq, State0),
    {reply, Reply, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', Handle, _, _, Reason}, #{?recv_handle := Handle} = State0) ->
    State1 = State0#{?recv_handle := ?undefined, ?stream := ?undefined},
    State = handle_grpc_reply(Reason, State1),
    {noreply, State};
handle_info({grpc_reply, Handle, ResRaw}, #{?recv_handle := Handle} = State0) ->
    State = handle_grpc_reply(ResRaw, State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

recover(ActionResId, Idx) ->
    maybe
        [?META_ROW(_, Val)] ?=
            ets:lookup(?META_TAB, ?META_STREAM_KEY(ActionResId, Idx)),
        #{
            ?writer := Writer,
            ?n_restarts := _,
            ?stream := Stream
        } ?= Val,
        true ?= is_pid(Writer) andalso is_process_alive(Writer),
        Handle = grpc_client:recv_async(Stream, #{mode => active}),
        Val#{?recv_handle => Handle}
    else
        _ ->
            ?tp(~"zerobus_receiver_recv_no_stream", #{}),
            #{
                ?recv_handle => ?undefined,
                ?writer => ?undefined,
                ?n_restarts => -1,
                ?stream => ?undefined
            }
    end.

is_end_of_stream(Resp) ->
    maybe
        {value, {eos, Trailers}, Rest} ?= lists:keytake(eos, 1, Resp),
        {true, Rest, Trailers}
    end.

handle_follow_stream(FollowReq, State0) ->
    #follow_stream{
        writer = Writer,
        n_restarts = NRestarts,
        stream = Stream
    } = FollowReq,
    #{?n_restarts := NRestarts0, ?last_acked_seq := LastAckedSeq0} = State0,
    Handle = grpc_client:recv_async(Stream, #{mode => active}),
    Reply = {ok, {NRestarts0, LastAckedSeq0}},
    State = State0#{
        ?writer := Writer,
        ?n_restarts := NRestarts,
        ?last_acked_seq := -1,
        ?recv_handle := Handle,
        ?stream := Stream
    },
    {Reply, State}.

clear_state(State0) ->
    %% do not clear n_restarts nor last_acked_seq; otherwise, writer will not get the
    %% latest info when syncing.
    State0#{
        ?writer := ?undefined,
        ?stream := ?undefined
    }.

handle_grpc_reply({ok, Res0}, State0) ->
    #{
        ?action_res_id := ActionResId,
        ?last_acked_seq := LastAckedSeq0,
        ?stream := Stream
    } = State0,
    Res1 = grpc_client:map_recv_async_reply(Stream, Res0),
    Res =
        case is_end_of_stream(Res1) of
            {true, Results0, Trailers0} ->
                {done, Results0, Trailers0};
            false ->
                {more, Res1}
        end,
    case Res of
        {done, Results, Trailers} ->
            LastAckedSeq = find_last_acked_seq(Results, LastAckedSeq0, State0),
            ?tp("zerobus_last_seq_scanned", #{}),
            State1 = State0#{?last_acked_seq := LastAckedSeq},
            maybe_notify_acked(LastAckedSeq0, State1),
            Reason = maybe_format_grpc_reason(grpc_client:trailers_to_error(Trailers)),
            Error = {error, Reason},
            notify_errored(Error, State1),
            clear_state(State1);
        {more, Results} ->
            LastAckedSeq = find_last_acked_seq(Results, LastAckedSeq0, State0),
            ?tp("zerobus_last_seq_scanned", #{}),
            State1 = State0#{?last_acked_seq := LastAckedSeq},
            maybe_notify_acked(LastAckedSeq0, State1),
            ?tp("zerobus_last_seq_notified", #{}),
            State1;
        {error, {deadline_exceeded, _}} ->
            %% impossible?
            State0;
        {error, not_found} ->
            %% stream is gone
            Error = {error, stream_closed},
            notify_errored(Error, State0),
            clear_state(State0);
        {error, Reason} ->
            ?tp(info, "zerobus_receiver_unexpected_error_response", #{
                action_res_id => ActionResId,
                reason => Reason
            }),
            maybe
                #{client_pid := Pid} ?= Stream,
                false ?= is_process_alive(Pid),
                Error = {error, stream_closed},
                notify_errored(Error, State0),
                clear_state(State0)
            else
                _ ->
                    State0
            end
    end;
handle_grpc_reply(worker_aborted, State0) ->
    State0;
handle_grpc_reply(Reason, State0) ->
    #{?action_res_id := ActionResId} = State0,
    ?tp(warning, "zerobus_receiver_stream_error", #{
        action_res_id => ActionResId,
        reason => Reason
    }),
    State0.

find_last_acked_seq(Results, LastAckedSeq, State) ->
    #{?action_res_id := ActionResId} = State,
    lists:foldl(
        fun(Response, Acc) ->
            case Response of
                #{
                    payload :=
                        {ingest_record_response, #{
                            durability_ack_up_to_offset := Offset
                        }}
                } when is_integer(Offset) ->
                    max(Acc, Offset);
                #{
                    payload := {close_stream_signal, Data}
                } ->
                    ?tp(debug, "zerobus_server_will_close_stream", #{
                        action_res_id => ActionResId,
                        'when' => Data
                    }),
                    Acc;
                _ ->
                    ?tp(warning, "zerobus_unexpected_ingest_response", #{
                        action_res_id => ActionResId,
                        response => Response
                    }),
                    Acc
            end
        end,
        LastAckedSeq,
        Results
    ).

maybe_notify_acked(
    PrevLastAcked, #{?last_acked_seq := LastAckedSeq} = State
) when
    PrevLastAcked < LastAckedSeq
->
    #{?n_restarts := NRestarts, ?writer := Writer} = State,
    emqx_bridge_zerobus_stream_writer_worker:acked(
        Writer, NRestarts, LastAckedSeq
    ),
    ?tp("zerobus_receiver_notified_ack", #{seq => LastAckedSeq}),
    ok;
maybe_notify_acked(_PrevLastAcked, _State) ->
    ok.

notify_errored(Error, State) ->
    #{?writer := Writer, ?n_restarts := NRestarts} = State,
    emqx_bridge_zerobus_stream_writer_worker:errored(
        Writer, NRestarts, Error
    ).

maybe_format_grpc_reason({Code, Reason}) when is_binary(Reason) ->
    {Code, uri_string:unquote(Reason)};
maybe_format_grpc_reason(Reason) ->
    Reason.
