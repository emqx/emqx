%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_async_receiver_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    where/2,
    follow_stream/6
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Internal exports
-export([do_recv_stream_once_async1/2]).

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

-define(nudge, nudge).

%% calls/casts/infos/continues
-record(follow_stream, {writer, n_restarts, stream, opts}).
-record(recv, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    #{action_res_id := ActionResId, idx := Idx} = Opts,
    gen_server:start_link(?via(ActionResId, Idx), ?MODULE, Opts, []).

where(Pool, Idx) ->
    gproc_pool:whereis_worker(Pool, {Pool, Idx}).

follow_stream(Pool, Idx, Writer, NRestarts, Stream, Opts) ->
    Pid = where(Pool, Idx),
    try
        gen_server:call(
            Pid,
            #follow_stream{
                writer = Writer,
                n_restarts = NRestarts,
                stream = Stream,
                opts = Opts
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
        ?opts := StreamOpts
    } = recover(ActionResId, Idx),
    State = #{
        ?action_res_id => ActionResId,
        ?idx => Idx,
        ?pool => Pool,
        ?writer => Writer,
        ?n_restarts => NRestarts,
        ?last_acked_seq => -1,
        ?helper => ?undefined,
        ?stream => Stream,
        ?opts => StreamOpts
    },
    nudge_recv(),
    {ok, State}.

terminate(_Reason, State) ->
    #{?pool := Pool, ?idx := Id, ?helper := Helper} = State,
    gproc_pool:disconnect_worker(Pool, {Pool, Id}),
    maybe
        true ?= is_pid(Helper),
        exit(Helper, kill)
    end,
    ok.

handle_continue(#recv{}, State0) ->
    State = handle_recv(State0),
    {noreply, State}.

handle_call(#follow_stream{} = FollowReq, _From, State0) ->
    {Reply, State} = handle_follow_stream(FollowReq, State0),
    {reply, Reply, State, {continue, #recv{}}};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#recv{}, State0) ->
    State = handle_recv(State0),
    {noreply, State};
handle_info({'EXIT', Helper, Res}, #{?helper := Helper} = State0) ->
    State1 = State0#{?helper := ?undefined},
    {Action, State} = handle_helper_down(Res, State1),
    case Action of
        ?nudge ->
            nudge_recv();
        ?continue ->
            ok
    end,
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
            ?stream := _,
            ?opts := _
        } ?= Val,
        true ?= is_pid(Writer) andalso is_process_alive(Writer),
        Val
    else
        _ ->
            #{
                ?writer => ?undefined,
                ?n_restarts => -1,
                ?stream => ?undefined,
                ?opts => #{}
            }
    end.

is_end_of_stream(Resp) ->
    maybe
        {value, {eos, Trailers}, Rest} ?= lists:keytake(eos, 1, Resp),
        {true, Rest, Trailers}
    end.

nudge_recv() ->
    _ = self() ! #recv{},
    ok.

handle_follow_stream(FollowReq, State0) ->
    #follow_stream{
        writer = Writer,
        n_restarts = NRestarts,
        stream = Stream,
        opts = Opts
    } = FollowReq,
    State1 = abort_helper(State0),
    #{?n_restarts := NRestarts0, ?last_acked_seq := LastAckedSeq0} = State1,
    Reply = {ok, {NRestarts0, LastAckedSeq0}},
    State = State1#{
        ?writer := Writer,
        ?n_restarts := NRestarts,
        ?last_acked_seq := -1,
        ?stream := Stream,
        ?opts := Opts
    },
    {Reply, State}.

handle_recv(#{?stream := ?undefined} = State0) ->
    ?tp(~"zerobus_receiver_recv_no_stream", #{}),
    State0;
handle_recv(#{?helper := Helper} = State0) when is_pid(Helper) ->
    %% impossible?  helper is already running.
    State0;
handle_recv(#{?helper := ?undefined} = State0) ->
    #{
        ?stream := Stream,
        ?opts := Opts
    } = State0,
    Helper = do_recv_stream_once_async(Stream, Opts),
    State0#{?helper := Helper}.

handle_helper_down({ok, Res}, State0) ->
    #{
        ?action_res_id := ActionResId,
        ?last_acked_seq := LastAckedSeq0,
        ?stream := Stream
    } = State0,
    case Res of
        {done, Results, Trailers} ->
            LastAckedSeq = find_last_acked_seq(Results, LastAckedSeq0, State0),
            ?tp("zerobus_last_seq_scanned", #{}),
            State1 = State0#{?last_acked_seq := LastAckedSeq},
            maybe_notify_acked(LastAckedSeq0, State1),
            Reason = maybe_format_grpc_reason(grpc_client:trailers_to_error(Trailers)),
            Error = {error, Reason},
            notify_errored(Error, State1),
            {?continue, State1};
        {more, Results} ->
            LastAckedSeq = find_last_acked_seq(Results, LastAckedSeq0, State0),
            ?tp("zerobus_last_seq_scanned", #{}),
            State1 = State0#{?last_acked_seq := LastAckedSeq},
            maybe_notify_acked(LastAckedSeq0, State1),
            ?tp("zerobus_last_seq_notified", #{}),
            {?nudge, State1};
        {error, {deadline_exceeded, _}} ->
            {?nudge, State0};
        {error, not_found} ->
            %% stream is gone
            Error = {error, stream_closed},
            notify_errored(Error, State0),
            {?continue, State0};
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
                {?continue, State0}
            else
                _ ->
                    {?nudge, State0}
            end
    end;
handle_helper_down(worker_aborted, State0) ->
    {?nudge, State0};
handle_helper_down(Reason, State0) ->
    #{?action_res_id := ActionResId} = State0,
    ?tp(warning, "zerobus_receiver_helper_died", #{
        action_res_id => ActionResId,
        reason => Reason
    }),
    {?nudge, State0}.

%% we delegate to a helper process so that the main process is responsive, specially when
%% a shutdown request arrives.
do_recv_stream_once_async(Stream, Opts) ->
    spawn_link(?MODULE, do_recv_stream_once_async1, [Stream, Opts]).

-spec do_recv_stream_once_async1(_Stream, _Opts) -> no_return().
do_recv_stream_once_async1(Stream, Opts) ->
    Res = do_recv_stream_once(Stream, Opts),
    exit({ok, Res}).

abort_helper(#{?helper := Helper} = State0) when is_pid(Helper) ->
    exit(Helper, kill),
    receive
        {'EXIT', Helper, killed} ->
            Res = worker_aborted;
        {'EXIT', Helper, Res0} ->
            Res = Res0
    end,
    State1 = State0#{?helper := ?undefined},
    {_Action, State} = handle_helper_down(Res, State1),
    State;
abort_helper(State0) ->
    State0.

do_recv_stream_once(Stream, Opts) ->
    try grpc_client:recv(Stream, Opts) of
        {ok, Resp} ->
            case is_end_of_stream(Resp) of
                {true, Results0, Trailers} ->
                    {done, Results0, Trailers};
                false ->
                    {more, Resp}
            end;
        {error, Reason} ->
            {error, Reason}
    catch
        error:Reason ->
            {error, Reason};
        Kind:Reason:Stacktrace ->
            {error, {Kind, Reason, Stacktrace}}
    end.

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
