%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_stream_writer_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    acked/2,
    errored/3,
    serde_updated/2,
    ingest_record_batch_sync/3,
    ingest_record_batch_async/3
]).

%% Internal exports
-export([sync_reply_delegator/2]).
-export([where/2]).
-export([gproc_name/2]).
-export([grpc_reply_callback/3]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_zerobus.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(json, json).
-define(open_stream_retry_interval, open_stream_retry_interval).

-define(name(ACTIONRESID, IDX), {n, l, {?MODULE, ACTIONRESID, IDX}}).
-define(via(ACTIONRESID, IDX), {via, gproc, ?name(ACTIONRESID, IDX)}).

%% calls/casts/infos/continues
-record(open_stream, {}).
-record(ingest_record_batch_async, {records :: [binary()], reply_fn}).
-record(acked, {n_restarts}).
-record(errored, {n_restarts, error}).
-record(serde_updated, {serde_name}).

-define(SERVICE, 'databricks.zerobus.Zerobus').
-define(PROTO_MODULE, 'emqx_bridge_zerobus_gen_zerobus_service_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType), #{
    path => Path,
    service => ?SERVICE,
    message_type => MessageType,
    marshal => ?MARSHAL(Req),
    unmarshal => ?UNMARSHAL(Resp)
}).

-define(proto_record_type, 1).
-define(json_record_type, 2).

-define(SEQ_ATOMIC_IDX, 1).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    #{action_res_id := ActionResId, idx := Idx} = Opts,
    gen_server:start_link(?via(ActionResId, Idx), ?MODULE, Opts, []).

where(Pool, Idx) ->
    gproc_pool:whereis_worker(Pool, {Pool, Idx}).

acked(Pid, NRestarts) ->
    gen_server:cast(Pid, #acked{n_restarts = NRestarts}).

errored(Pid, NRestarts, Error) ->
    gen_server:cast(Pid, #errored{n_restarts = NRestarts, error = Error}).

ingest_record_batch_sync(Pool, Records, Timeout) ->
    maybe
        {ok, Pid} ?= pick_writer(Pool, self()),
        ReplyAlias = monitor(process, Pid, [{alias, reply_demonitor}]),
        ReplyFnAndArgs = {fun ?MODULE:sync_reply_delegator/2, [ReplyAlias]},
        gen_server:cast(Pid, #ingest_record_batch_async{
            records = Records, reply_fn = ReplyFnAndArgs
        }),
        receive
            {ReplyAlias, Response} ->
                Response;
            {'DOWN', ReplyAlias, _, _, Reason} ->
                ?tp(~"zerobus_sync_ingest_writer_died", #{}),
                {error, {recoverable_error, {writer_down, Reason}}}
        after Timeout ->
            _ = unalias(ReplyAlias),
            _ = demonitor(ReplyAlias),
            receive
                {ReplyAlias, Response} ->
                    Response;
                {'DOWN', ReplyAlias, _, _, Reason} ->
                    ?tp(~"zerobus_sync_ingest_writer_died", #{}),
                    {error, {recoverable_error, {writer_down, Reason}}}
            after 0 ->
                {error, {recoverable_error, timeout}}
            end
        end
    end.

sync_reply_delegator(ReplyAlias, Response0) ->
    ?tp(debug, "zerobus_writer_insert_result", #{result => Response0}),
    Response = handle_insert_result(Response0),
    _ = erlang:send(ReplyAlias, {ReplyAlias, Response}),
    ok.

ingest_record_batch_async(Pool, Records, ReplyFnAndArgs) ->
    maybe
        {ok, Pid} ?= pick_writer(Pool, self()),
        gen_server:cast(Pid, #ingest_record_batch_async{
            records = Records, reply_fn = ReplyFnAndArgs
        }),
        {ok, Pid}
    end.

serde_updated(Pid, SerdeName0) ->
    SerdeName = emqx_utils_conv:bin(SerdeName0),
    gen_server:cast(Pid, #serde_updated{serde_name = SerdeName}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    #{
        action_res_id := ActionResId,
        pool := Pool,
        client_pool := ClientPool,
        %% recv_pool := RecvPool,
        idx := Idx,
        record := Record,
        catalog := Catalog,
        schema := Schema,
        table := Table,
        request_ttl := RequestTTL,
        open_stream_retry_interval := OpenStreamRetryInterval
    } = Opts,
    true = gproc_pool:connect_worker(Pool, {Pool, Idx}),
    ?tp("zerobus_writer_init_will_clear_old_stream", #{}),
    TableFQN = fmt("${c}.${s}.${t}", #{
        c => Catalog,
        s => Schema,
        t => Table
    }),
    State = #{
        ?action_res_id => ActionResId,
        ?idx => Idx,
        ?pool => Pool,
        ?open_stream_retry_interval => OpenStreamRetryInterval,
        ?n_restarts => -1,
        ?seq => 0,
        ?acked => -1,
        ?atomics_ref => ?undefined,
        ?stream => ?undefined,
        ?recv_handle => ?undefined,
        ?callers => gb_trees:empty(),
        ?client_pool => ClientPool,
        %% ?recv_pool => RecvPool,
        ?request_ttl => RequestTTL,
        ?record => Record,
        ?catalog => Catalog,
        ?schema => Schema,
        ?table => Table,
        ?table_fqn => TableFQN
    },
    {ok, State, {continue, #open_stream{}}}.

terminate(_Reason, State) ->
    #{?pool := Pool, ?idx := Idx} = State,
    gproc_pool:disconnect_worker(Pool, {Pool, Idx}),
    _ = ensure_stream_closed(State),
    ok.

handle_continue(#open_stream{}, State0) ->
    State1 = ensure_stream_closed(State0),
    State = handle_open_stream(State1),
    {noreply, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#acked{n_restarts = NRestarts}, State0) ->
    State = handle_acked(NRestarts, State0),
    {noreply, State};
handle_cast(#errored{n_restarts = NRestarts, error = Error}, State0) ->
    case handle_errored(NRestarts, Error, State0) of
        {?continue, State} ->
            {noreply, State};
        {?reopen, State} ->
            {noreply, State, {continue, #open_stream{}}}
    end;
handle_cast(#ingest_record_batch_async{records = Records, reply_fn = ReplyFnAndArgs}, State0) ->
    case handle_ingest_record_batch_async(Records, ReplyFnAndArgs, State0) of
        {?continue, State} ->
            {noreply, State};
        {?reopen, State} ->
            {noreply, State, {continue, #open_stream{}}}
    end;
handle_cast(#serde_updated{serde_name = SerdeName}, State0) ->
    case handle_serde_updated(SerdeName, State0) of
        {?continue, State} ->
            {noreply, State};
        {?reopen, State} ->
            {noreply, State, {continue, #open_stream{}}}
    end;
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'DOWN', Handle, _, _, Reason}, #{?recv_handle := {_, Handle}} = State0) ->
    State1 = State0#{?recv_handle := ?undefined},
    State = handle_open_stream_reply(Reason, State1),
    {noreply, State};
handle_info({grpc_reply, Handle, ResRaw}, #{?recv_handle := {Stream, Handle}} = State0) ->
    State1 = State0#{?recv_handle := ?undefined},
    State = handle_open_stream_reply({ok, {ResRaw, Stream}}, State1),
    {noreply, State};
handle_info(#open_stream{}, State0) ->
    State1 = ensure_stream_closed(State0),
    State = handle_open_stream(State1),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal exports
%%------------------------------------------------------------------------------

gproc_name(ActionResId, Idx) ->
    ?name(ActionResId, Idx).

grpc_reply_callback({ok, Res0}, _ReplyAlias, Ctx) ->
    #{
        ?action_res_id := ActionResId,
        ?atomics_ref := AtomicsRef,
        ?stream := Stream,
        ?n_restarts := _,
        ?writer := _
    } = Ctx,
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
            LastAckedSeq = find_last_acked_seq(Results, Ctx),
            ?tp("zerobus_last_seq_scanned", #{}),
            save_last_acked_seq(AtomicsRef, LastAckedSeq),
            notify_acked(LastAckedSeq, Ctx),
            Reason = maybe_format_grpc_reason(grpc_client:trailers_to_error(Trailers)),
            Error = {error, Reason},
            notify_errored(Error, Ctx),
            ok;
        {more, Results} ->
            LastAckedSeq = find_last_acked_seq(Results, Ctx),
            ?tp("zerobus_last_seq_scanned", #{}),
            save_last_acked_seq(AtomicsRef, LastAckedSeq),
            notify_acked(LastAckedSeq, Ctx),
            ?tp("zerobus_last_seq_notified", #{}),
            ok;
        {error, not_found} ->
            %% stream is gone
            Error = {error, stream_closed},
            notify_errored(Error, Ctx),
            ok;
        {error, Reason} ->
            ?tp(info, "zerobus_receiver_unexpected_error_response", #{
                action_res_id => ActionResId,
                reason => Reason
            }),
            maybe
                #{client_pid := Pid} ?= Stream,
                false ?= is_process_alive(Pid),
                Error = {error, stream_closed},
                notify_errored(Error, Ctx),
                ok
            else
                _ ->
                    ok
            end
    end;
grpc_reply_callback(Reason, _ReplyAlias, Ctx) ->
    #{?action_res_id := ActionResId} = Ctx,
    ?tp(warning, "zerobus_receiver_stream_error", #{
        action_res_id => ActionResId,
        reason => Reason
    }),
    notify_errored(Reason, Ctx),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

fmt(FmtStr, Context) ->
    Template = emqx_template:parse(FmtStr),
    iolist_to_binary(emqx_template:render_strict(Template, Context)).

set_health(ActionResId, Idx, HealthStatus) ->
    emqx_bridge_zerobus_impl:set_health(ActionResId, Idx, HealthStatus).

append_error_detail(Msg, ~"") ->
    Msg;
append_error_detail(Msg, Details) when is_binary(Details) ->
    <<Msg/binary, ": ", Details/binary>>;
append_error_detail(Msg, _Details) ->
    Msg.

pick_writer(Pool, Key) ->
    case gproc_pool:pick_worker(Pool, Key) of
        false ->
            {error, {recoverable_error, empty_pool}};
        Pid when is_pid(Pid) ->
            {ok, Pid}
    end.

reply_callers_up_to(Reply, LastAckedSeq, State0) ->
    #{?callers := Callers0} = State0,
    Callers = do_reply_callers_up_to(Reply, LastAckedSeq, Callers0),
    State0#{?callers := Callers}.

do_reply_callers_up_to(Reply, LastAckedSeq, Callers0) ->
    case gb_trees:is_empty(Callers0) of
        true ->
            Callers0;
        false ->
            case gb_trees:take_smallest(Callers0) of
                {Seq, _, _} when Seq > LastAckedSeq ->
                    Callers0;
                {_Seq, ReplyFnAndArgs, Callers} ->
                    emqx_resource:apply_reply_fun(ReplyFnAndArgs, Reply),
                    do_reply_callers_up_to(Reply, LastAckedSeq, Callers)
            end
    end.

handle_open_stream(#{?recv_handle := {_, _}} = State0) ->
    %% impossible?
    State0;
handle_open_stream(#{?recv_handle := ?undefined} = State0) ->
    #{?action_res_id := ActionResId} = State0,
    ?tp(info, "zerobus_opening_stream", #{action_res_id => ActionResId}),
    Opts = grpc_opts(State0),
    Req = create_stream_req(State0),
    maybe
        {ok, Meta} ?= grpc_meta(State0),
        {ok, Stream} ?= do_create_stream_impl(Meta, Opts),
        ok ?= grpc_send(Stream, Req, Opts),
        Handle = grpc_client:recv_async(Stream, #{mode => once}),
        State0#{?recv_handle := {Stream, Handle}}
    else
        {error, Reason} ->
            handle_open_stream_error(Reason, State0)
    end.

handle_open_stream_reply({ok, {{ok, ResRaw}, Stream}}, State0) ->
    #{
        ?action_res_id := ActionResId,
        ?idx := Idx,
        ?seq := Seq,
        ?n_restarts := NRestarts0,
        ?acked := Acked0,
        ?atomics_ref := PrevAtomicRef
    } = State0,
    Res = grpc_client:map_recv_async_reply(Stream, ResRaw),
    case is_end_of_stream(Res) of
        {true, _, Trailers} ->
            %% only closed on errors
            Reason0 = grpc_client:trailers_to_error(Trailers),
            Reason = maybe_format_grpc_reason(Reason0),
            handle_open_stream_error({?stream_closed, Reason}, State0);
        false ->
            set_health(ActionResId, Idx, ?status_connected),
            NRestarts = NRestarts0 + 1,
            AtomicsRef = atomics:new(?SEQ_ATOMIC_IDX, [{signed, true}]),
            atomics:put(AtomicsRef, ?SEQ_ATOMIC_IDX, -1),
            Acked1 =
                case PrevAtomicRef of
                    ?undefined ->
                        -1;
                    _ ->
                        atomics:get(PrevAtomicRef, ?SEQ_ATOMIC_IDX)
                end,
            State1 = recv_async_active(Stream, NRestarts, AtomicsRef, State0),
            Acked = max(Acked0, Acked1),
            State2 = reply_callers_up_to(ok, Acked, State1),
            State3 = reply_callers_up_to({error, {recoverable_error, stream_closed}}, Seq, State2),
            State3#{
                ?seq := 0,
                ?callers := gb_trees:empty(),
                ?n_restarts := NRestarts
            }
    end;
handle_open_stream_reply({ok, {{error, Reason}, _Stream, _Opts}}, State0) ->
    handle_open_stream_error(Reason, State0);
handle_open_stream_reply(Reason, State0) ->
    handle_open_stream_error(Reason, State0).

handle_open_stream_error(Reason, State0) ->
    #{
        ?action_res_id := ActionResId,
        ?idx := Idx,
        ?open_stream_retry_interval := OpenStreamRetryInterval
    } = State0,
    ?tp(warning, "zerobus_failed_to_open_stream", #{
        action_res_id => ActionResId,
        reason => Reason
    }),
    case Reason of
        {?stream_closed, {unauthenticated, Details}} ->
            Msg = append_error_detail(~"Invalid credentials", Details),
            set_health(ActionResId, Idx, {?status_disconnected, {unhealthy_target, Msg}});
        {?stream_closed, {permission_denied, Details}} ->
            Msg = append_error_detail(~"Permission denied", Details),
            set_health(ActionResId, Idx, {?status_disconnected, {unhealthy_target, Msg}});
        {token_endpoint_error, 401, Details} ->
            Msg = append_error_detail(~"Invalid credentials", Details),
            set_health(ActionResId, Idx, {?status_disconnected, {unhealthy_target, Msg}});
        {?stream_closed, {Code, Details}} ->
            CodeBin = emqx_utils_conv:bin(Code),
            Msg = append_error_detail(CodeBin, Details),
            set_health(ActionResId, Idx, {?status_disconnected, Msg});
        _ ->
            set_health(ActionResId, Idx, {?status_disconnected, Reason})
    end,
    erlang:send_after(OpenStreamRetryInterval, self(), #open_stream{}),
    State0.

handle_acked(OldNRestarts, #{?n_restarts := NRestarts} = State0) when
    OldNRestarts < NRestarts
->
    %% stale response
    ?tp("zerobus_writer_stale_ack", #{}),
    State0;
handle_acked(_NRestarts, #{?atomics_ref := ?undefined} = State0) ->
    %% stale response
    ?tp("zerobus_writer_stale_ack", #{}),
    State0;
handle_acked(NRestarts, State0) ->
    #{?acked := Acked0, ?atomics_ref := AtomicsRef} = State0,
    LastAckedSeq = atomics:get(AtomicsRef, ?SEQ_ATOMIC_IDX),
    State1 = reply_callers_up_to(ok, LastAckedSeq, State0),
    ?tp("zerobus_writer_acked", #{seq => LastAckedSeq, n_restarts => NRestarts}),
    State1#{?acked := max(Acked0, LastAckedSeq)}.

handle_errored(OldNRestarts, _Error, #{?n_restarts := NRestarts} = State0) when
    OldNRestarts < NRestarts
->
    %% stale response
    ?tp("zerobus_writer_stale_error", #{}),
    {?continue, State0};
handle_errored(_NRestarts, Error0, State0) ->
    #{?action_res_id := ActionResId, ?idx := Idx, ?seq := Seq} = State0,
    ?tp(debug, "zerobus_writer_insert_error", #{error => Error0}),
    Error = handle_insert_result(Error0),
    State1 = reply_callers_up_to(Error, Seq, State0),
    State = ensure_stream_closed(State1),
    set_health(ActionResId, Idx, {?status_connecting, ~"reopening stream"}),
    {?reopen, State}.

handle_ingest_record_batch_async(_Records, ReplyFnAndArgs, #{?stream := ?undefined} = State0) ->
    %% race: request arrived just as we lost the stream (or couldn't open it)
    ?tp("zerobus_ingest_without_stream", #{}),
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, {error, {recoverable_error, stream_closed}}),
    {?continue, State0};
handle_ingest_record_batch_async(Records, ReplyFnAndArgs, State0) ->
    #{
        ?action_res_id := ActionResId,
        ?idx := Idx,
        ?callers := Callers0,
        ?stream := Stream,
        ?seq := Seq0
    } = State0,
    Opts = grpc_opts(State0),
    Req = ingest_record_batch_req(Records, State0),
    Seq = Seq0 + 1,
    State1 = State0#{?seq := Seq},
    ?tp("zerobus_will_send_batch0", #{stream => Stream}),
    ?tp("zerobus_will_send_batch1", #{seq => Seq0}),
    maybe
        ok ?= grpc_send(Stream, Req, Opts),
        ?tp("zerobus_sent_batch", #{}),
        Callers = gb_trees:insert(Seq0, ReplyFnAndArgs, Callers0),
        State = State1#{?callers := Callers},
        {?continue, State}
    else
        {error, {exit, noproc, _} = _Reason} ->
            set_health(ActionResId, Idx, {?status_connecting, ~"reopening stream"}),
            emqx_resource:apply_reply_fun(
                ReplyFnAndArgs, {error, {recoverable_error, stream_closed}}
            ),
            State2 = ensure_stream_closed(State1),
            {?reopen, State2};
        {error, Reason} when
            %% from grpc_client actually trying to handle the call
            Reason == closed;
            Reason == not_found;
            Reason == bad_stream
        ->
            ?tp("zerobus_writer_send_error", #{reason => Reason}),
            set_health(ActionResId, Idx, {?status_connecting, ~"reopening stream"}),
            emqx_resource:apply_reply_fun(
                ReplyFnAndArgs, {error, {recoverable_error, stream_closed}}
            ),
            State2 = ensure_stream_closed(State1),
            {?reopen, State2};
        {error, Reason} ->
            ?tp("zerobus_writer_send_error", #{reason => Reason}),
            set_health(ActionResId, Idx, {?status_connecting, ~"reopening stream"}),
            emqx_resource:apply_reply_fun(
                ReplyFnAndArgs, {error, {recoverable_error, {stream_closed, Reason}}}
            ),
            State2 = ensure_stream_closed(State1),
            {?reopen, State2}
    end.

handle_serde_updated(SerdeName, State0) ->
    case State0 of
        #{?record := {?proto, #{?serde_name := SerdeName} = OldProto}} ->
            #{?message_type := MessageTypeAtom, ?descriptor := OldDescriptor} = OldProto,
            MessageTypeBin = atom_to_binary(MessageTypeAtom, utf8),
            Opts = #{schema_name => SerdeName, message_type => MessageTypeBin},
            case emqx_bridge_zerobus_utils:get_serde(Opts) of
                {ok, #{?descriptor := OldDescriptor}} ->
                    {?continue, State0};
                {ok, NewProto} ->
                    State = State0#{?record := {?proto, NewProto}},
                    {?reopen, State};
                {error, Reason} ->
                    Msg = map_get_serde_error(Reason),
                    #{?action_res_id := ActionResId, ?idx := Idx} = State0,
                    set_health(ActionResId, Idx, {?status_disconnected, {unhealthy_target, Msg}}),
                    ?tp("zerobus_get_serde_error", #{}),
                    %% no point in reopening the stream, since this needs manual
                    %% intervention that'll restart the action.
                    {?continue, State0}
            end;
        _ ->
            {?continue, State0}
    end.

grpc_meta(State) ->
    #{
        ?action_res_id := ActionResId,
        ?table_fqn := TableFQN
    } = State,
    maybe
        {ok, Token} ?= emqx_connector_oauth2:get_token(ActionResId),
        Meta = #{
            ~"authorization" => <<"Bearer ", Token/binary>>,
            ~"x-databricks-zerobus-table-name" => TableFQN
        },
        {ok, Meta}
    end.

grpc_opts(State) ->
    #{
        ?client_pool := ClientPool,
        ?request_ttl := RequestTTL
    } = State,
    #{
        channel => ClientPool,
        timeout => RequestTTL,
        content_type => ~"application/grpc"
    }.

create_stream_req(#{?record := ?json} = State) ->
    #{
        ?table_fqn := TableFQN
    } = State,
    #{
        payload =>
            {
                create_stream,
                #{
                    table_name => TableFQN,
                    record_type => ?json_record_type
                }
            }
    };
create_stream_req(#{?record := {?proto, Proto}} = State) ->
    #{
        ?table_fqn := TableFQN
    } = State,
    #{?descriptor := Descriptor} = Proto,
    #{
        payload =>
            {
                create_stream,
                #{
                    table_name => TableFQN,
                    record_type => ?proto_record_type,
                    descriptor_proto => Descriptor
                }
            }
    }.

ingest_record_batch_req(Records, #{?record := ?json} = State) ->
    #{
        ?seq := Seq
    } = State,
    #{
        payload =>
            {
                ingest_record_batch,
                #{
                    offset_id => Seq,
                    batch => {
                        json_batch,
                        #{records => Records}
                    }
                }
            }
    };
ingest_record_batch_req(Records, #{?record := {?proto, _Proto}} = State) ->
    #{
        ?seq := Seq
    } = State,
    #{
        payload =>
            {
                ingest_record_batch,
                #{
                    offset_id => Seq,
                    batch => {
                        proto_encoded_batch,
                        #{records => Records}
                    }
                }
            }
    }.

grpc_send(Stream, Req, Opts) ->
    try
        grpc_client:send(Stream, Req, nofin, Opts)
    catch
        error:Reason ->
            {error, Reason};
        Kind:Reason:Stacktrace ->
            {error, {Kind, Reason, Stacktrace}}
    end.

notify_acked(LastAckedSeq, Ctx) ->
    #{?n_restarts := NRestarts, ?writer := Writer} = Ctx,
    emqx_bridge_zerobus_stream_writer_worker:acked(Writer, NRestarts),
    ?tp("zerobus_receiver_notified_ack", #{seq => LastAckedSeq}),
    ok.

notify_errored(Error, Ctx) ->
    #{?writer := Writer, ?n_restarts := NRestarts} = Ctx,
    emqx_bridge_zerobus_stream_writer_worker:errored(
        Writer, NRestarts, Error
    ).

recv_async_active(Stream, NRestarts, AtomicsRef, State0) ->
    #{?action_res_id := ActionResId} = State0,
    Ctx = #{
        ?action_res_id => ActionResId,
        ?atomics_ref => AtomicsRef,
        ?n_restarts => NRestarts,
        ?writer => self(),
        ?stream => Stream
    },
    ReplyFn = {fun ?MODULE:grpc_reply_callback/3, [Ctx]},
    Handle = grpc_client:recv_async(Stream, #{mode => active, reply_fn => ReplyFn}),
    State0#{
        ?atomics_ref := AtomicsRef,
        ?stream := Stream,
        ?recv_handle := Handle,
        ?n_restarts := NRestarts,
        ?acked := -1
    }.

save_last_acked_seq(AtomicsRef, LastAckedSeq) ->
    case atomics:get(AtomicsRef, ?SEQ_ATOMIC_IDX) of
        NewerSeq when NewerSeq >= LastAckedSeq ->
            ok;
        OlderSeq ->
            _ = atomics:compare_exchange(AtomicsRef, ?SEQ_ATOMIC_IDX, OlderSeq, LastAckedSeq),
            ok
    end.

find_last_acked_seq(Results, Ctx) ->
    #{?action_res_id := ActionResId} = Ctx,
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
        -1,
        Results
    ).

is_end_of_stream(Resp) ->
    maybe
        {value, {eos, Trailers}, Rest} ?= lists:keytake(eos, 1, Resp),
        {true, Rest, Trailers}
    end.

do_create_stream_impl(Metadata, Opts) ->
    try
        grpc_client:open(
            ?DEF(
                <<"/databricks.zerobus.Zerobus/EphemeralStream">>,
                'databricks.zerobus.ephemeral_stream_request',
                'databricks.zerobus.ephemeral_stream_response',
                <<"databricks.zerobus.EphemeralStreamRequest">>
            ),
            Metadata,
            Opts
        )
    catch
        exit:{noproc, _} ->
            %% race: client died just as we called it
            {error, grpc_client_restarting};
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

ensure_stream_closed(#{?stream := ?undefined} = State0) ->
    State0;
ensure_stream_closed(#{?stream := Stream} = State0) ->
    grpc_client:close_async(Stream),
    State0#{?stream := ?undefined, ?recv_handle := ?undefined}.

maybe_format_grpc_reason({Code, Reason}) when is_binary(Reason) ->
    {Code, uri_string:unquote(Reason)};
maybe_format_grpc_reason(Reason) ->
    Reason.

handle_insert_result({error, {Code, _Msg} = Reason}) when
    Code == unavailable;
    Code == resource_exhausted;
    Code == internal;
    Code == cancelled;
    Code == aborted
->
    %% "Stream timeout reached. Closing the stream (ID: ...). Error Code: 6002, Error State: 0."
    %% Zerobus server seems to close the stream after some minutes on its own.
    %% May be foreshadowed by a `CloseStreamSignal` message.
    {error, {recoverable_error, Reason}};
handle_insert_result({error, {Code, _Msg} = Reason}) when
    Code == unauthenticated;
    Code == permission_denied;
    Code == invalid_argument;
    Code == failed_precondition;
    Code == not_found;
    Code == data_loss;
    Code == unknown;
    Code == out_of_range;
    Code == unimplemented
->
    %% https://docs.databricks.com/aws/en/ingestion/zerobus-errors
    {error, {unrecoverable_error, Reason}};
handle_insert_result({error, stream_closed = Reason}) ->
    {error, {recoverable_error, Reason}};
handle_insert_result({error, {Kind, Reason}}) when
    Kind == unrecoverable_error;
    Kind == recoverable_error
->
    {error, {Kind, Reason}};
handle_insert_result({error, Reason}) ->
    {error, {unrecoverable_error, Reason}};
handle_insert_result(Res) ->
    Res.

map_get_serde_error(schema_not_found) ->
    %% should be a rare race condition, since we've just been notified of this serde
    %% changing (not being deleted).
    ~"Schema not found";
map_get_serde_error(bad_type) ->
    ~"Invalid schema type; must be protobuf";
map_get_serde_error(message_type_not_found) ->
    ~"Message type not found in protobuf schema/bundle".
