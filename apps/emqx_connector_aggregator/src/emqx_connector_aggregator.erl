%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% This module manages buffers for aggregating records and offloads them
%% to separate "delivery" processes when they are full or time interval
%% is over.
-module(emqx_connector_aggregator).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-include("emqx_connector_aggregator.hrl").

-export([
    start_link/2,
    push_records/3,
    tick/2,
    take_error/1,
    buffer_to_map/1
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export_type([
    container_type/0,
    record/0,
    timestamp/0
]).

-type container_type() :: csv.

%% Record.
-type record() :: #{binary() => _}.

%% Unix timestamp, seconds since epoch.
-type timestamp() :: _Seconds :: non_neg_integer().

%%

-define(VSN, 1).
-define(SRVREF(NAME), {via, gproc, {n, l, {?MODULE, NAME}}}).

%%

start_link(Name, Opts) ->
    gen_server:start_link(?SRVREF(Name), ?MODULE, mk_state(Name, Opts), []).

push_records(Name, Timestamp, Records = [_ | _]) ->
    %% FIXME: Error feedback.
    case pick_buffer(Name, Timestamp) of
        undefined ->
            BufferNext = next_buffer(Name, Timestamp),
            write_records_limited(Name, BufferNext, Records);
        Buffer ->
            write_records_limited(Name, Buffer, Records)
    end;
push_records(_Name, _Timestamp, []) ->
    ok.

tick(Name, Timestamp) ->
    case pick_buffer(Name, Timestamp) of
        #buffer{} ->
            ok;
        _Outdated ->
            send_close_buffer(Name, Timestamp)
    end.

take_error(Name) ->
    gen_server:call(?SRVREF(Name), take_error).

buffer_to_map(#buffer{} = Buffer) ->
    #{
        since => Buffer#buffer.since,
        until => Buffer#buffer.until,
        seq => Buffer#buffer.seq,
        filename => Buffer#buffer.filename,
        max_records => Buffer#buffer.max_records
    }.

%%

write_records_limited(Name, Buffer = #buffer{max_records = undefined}, Records) ->
    write_records(Name, Buffer, Records, _NumWritten = undefined);
write_records_limited(Name, Buffer = #buffer{max_records = MaxRecords}, Records) ->
    NR = length(Records),
    case inc_num_records(Buffer, NR) of
        NR ->
            %% NOTE: Allow unconditionally if it's the first write.
            write_records(Name, Buffer, Records, NR);
        NWritten when NWritten > MaxRecords ->
            NextBuffer = rotate_buffer(Name, Buffer),
            write_records_limited(Name, NextBuffer, Records);
        NWritten ->
            write_records(Name, Buffer, Records, NWritten)
    end.

write_records(Name, Buffer = #buffer{fd = Writer, max_records = MaxRecords}, Records, NumWritten) ->
    case emqx_connector_aggreg_buffer:write(Records, Writer) of
        ok ->
            ?tp(connector_aggreg_records_written, #{
                action => Name,
                records => Records,
                buffer => Buffer
            }),
            case is_number(NumWritten) andalso NumWritten >= MaxRecords of
                true ->
                    rotate_buffer_async(Name, Buffer);
                false ->
                    ok
            end,
            ok;
        {error, terminated} ->
            BufferNext = rotate_buffer(Name, Buffer),
            write_records_limited(Name, BufferNext, Records);
        {error, _} = Error ->
            Error
    end.

inc_num_records(#buffer{cnt_records = Counter}, Size) ->
    inc_counter(Counter, Size).

next_buffer(Name, Timestamp) ->
    gen_server:call(?SRVREF(Name), {next_buffer, Timestamp}).

rotate_buffer(Name, #buffer{fd = FD}) ->
    gen_server:call(?SRVREF(Name), {rotate_buffer, FD}).

rotate_buffer_async(Name, #buffer{fd = FD}) ->
    gen_server:cast(?SRVREF(Name), {rotate_buffer, FD}).

send_close_buffer(Name, Timestamp) ->
    gen_server:cast(?SRVREF(Name), {close_buffer, Timestamp}).

%%

-record(st, {
    name :: _Name,
    tab :: ets:tid() | undefined,
    buffer :: buffer() | undefined,
    queued :: buffer() | undefined,
    deliveries = #{} :: #{reference() => buffer()},
    errors = queue:new() :: queue:queue(_Error),
    interval :: emqx_schema:duration_s(),
    max_records :: pos_integer(),
    work_dir :: file:filename()
}).

-type state() :: #st{}.

mk_state(Name, Opts) ->
    Interval = maps:get(time_interval, Opts),
    MaxRecords = maps:get(max_records, Opts),
    WorkDir = maps:get(work_dir, Opts),
    ok = ensure_workdir(WorkDir),
    #st{
        name = Name,
        interval = Interval,
        max_records = MaxRecords,
        work_dir = WorkDir
    }.

ensure_workdir(WorkDir) ->
    %% NOTE
    %% Writing MANIFEST as a means to ensure the work directory is writable. It's not
    %% (yet) read back because there's only one version of the implementation.
    ok = filelib:ensure_path(WorkDir),
    ok = write_manifest(WorkDir).

write_manifest(WorkDir) ->
    Manifest = #{<<"version">> => ?VSN},
    file:write_file(filename:join(WorkDir, "MANIFEST"), hocon_pp:do(Manifest, #{})).

%%

-spec init(state()) -> {ok, state()}.
init(St0 = #st{name = Name}) ->
    _ = erlang:process_flag(trap_exit, true),
    St1 = St0#st{tab = create_tab(Name)},
    St = recover(St1),
    _ = announce_current_buffer(St),
    {ok, St}.

handle_call({next_buffer, Timestamp}, _From, St0) ->
    St = #st{buffer = Buffer} = handle_next_buffer(Timestamp, St0),
    {reply, Buffer, St};
handle_call({rotate_buffer, FD}, _From, St0) ->
    St = #st{buffer = Buffer} = handle_rotate_buffer(FD, St0),
    {reply, Buffer, St};
handle_call(take_error, _From, St0) ->
    {MaybeError, St} = handle_take_error(St0),
    {reply, MaybeError, St}.

handle_cast({close_buffer, Timestamp}, St) ->
    {noreply, handle_close_buffer(Timestamp, St)};
handle_cast({rotate_buffer, FD}, St0) ->
    St = handle_rotate_buffer(FD, St0),
    {noreply, St};
handle_cast(enqueue_delivery, St0) ->
    {noreply, handle_queued_buffer(St0)};
handle_cast(_Cast, St) ->
    {noreply, St}.

handle_info({'DOWN', MRef, _, Pid, Reason}, St0 = #st{name = Name, deliveries = Ds0}) ->
    case maps:take(MRef, Ds0) of
        {Buffer, Ds} ->
            St = St0#st{deliveries = Ds},
            {noreply, handle_delivery_exit(Buffer, Reason, St)};
        error ->
            ?SLOG(notice, #{
                msg => "unexpected_down_signal",
                action => Name,
                pid => Pid,
                reason => Reason
            }),
            {noreply, St0}
    end;
handle_info(_Msg, St) ->
    {noreply, St}.

terminate(_Reason, #st{name = Name}) ->
    cleanup_tab(Name).

%%

handle_next_buffer(Timestamp, St = #st{buffer = #buffer{until = Until}}) when Timestamp < Until ->
    St;
handle_next_buffer(Timestamp, St0 = #st{buffer = Buffer = #buffer{since = PrevSince}}) ->
    BufferClosed = close_buffer(Buffer),
    St = enqueue_closed_buffer(BufferClosed, St0),
    handle_next_buffer(Timestamp, PrevSince, St);
handle_next_buffer(Timestamp, St = #st{buffer = undefined}) ->
    handle_next_buffer(Timestamp, Timestamp, St).

handle_next_buffer(Timestamp, PrevSince, St0) ->
    NextBuffer = allocate_next_buffer(Timestamp, PrevSince, St0),
    St = St0#st{buffer = NextBuffer},
    _ = announce_current_buffer(St),
    St.

handle_rotate_buffer(
    FD,
    St0 = #st{buffer = Buffer = #buffer{since = Since, seq = Seq, fd = FD}}
) ->
    BufferClosed = close_buffer(Buffer),
    NextBuffer = allocate_buffer(Since, Seq + 1, St0),
    St = enqueue_closed_buffer(BufferClosed, St0#st{buffer = NextBuffer}),
    _ = announce_current_buffer(St),
    St;
handle_rotate_buffer(_ClosedFD, St) ->
    St.

enqueue_closed_buffer(Buffer, St = #st{queued = undefined}) ->
    trigger_enqueue_delivery(),
    St#st{queued = Buffer};
enqueue_closed_buffer(Buffer, St0) ->
    %% NOTE: Should never really happen unless interval / max records are too tight.
    St = handle_queued_buffer(St0),
    St#st{queued = Buffer}.

handle_queued_buffer(St = #st{queued = undefined}) ->
    St;
handle_queued_buffer(St = #st{queued = Buffer}) ->
    enqueue_delivery(Buffer, St#st{queued = undefined}).

allocate_next_buffer(Timestamp, PrevSince, St = #st{interval = Interval}) ->
    Since = compute_since(Timestamp, PrevSince, Interval),
    allocate_buffer(Since, 0, St).

compute_since(Timestamp, PrevSince, Interval) ->
    Timestamp - (Timestamp - PrevSince) rem Interval.

allocate_buffer(Since, Seq, St = #st{name = Name}) ->
    Buffer = #buffer{filename = Filename, cnt_records = Counter} = mk_buffer(Since, Seq, St),
    {ok, FD} = file:open(Filename, [write, binary]),
    Writer = emqx_connector_aggreg_buffer:new_writer(FD, _Meta = []),
    _ = add_counter(Counter),
    ?tp(connector_aggreg_buffer_allocated, #{action => Name, filename => Filename, buffer => Buffer}),
    Buffer#buffer{fd = Writer}.

recover_buffer(Buffer = #buffer{filename = Filename, cnt_records = Counter}) ->
    {ok, FD} = file:open(Filename, [read, write, binary]),
    case recover_buffer_writer(FD, Filename) of
        {ok, Writer, NWritten} ->
            _ = add_counter(Counter, NWritten),
            Buffer#buffer{fd = Writer};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "existing_buffer_recovery_failed",
                filename => Filename,
                reason => Reason,
                details => "Buffer is corrupted beyond repair, will be discarded."
            }),
            _ = file:close(FD),
            _ = file:delete(Filename),
            undefined
    end.

recover_buffer_writer(FD, Filename) ->
    try emqx_connector_aggreg_buffer:new_reader(FD) of
        {_Meta, Reader} -> recover_buffer_writer(FD, Filename, Reader, 0)
    catch
        error:Reason ->
            {error, Reason}
    end.

recover_buffer_writer(FD, Filename, Reader0, NWritten) ->
    try emqx_connector_aggreg_buffer:read(Reader0) of
        {Records, Reader} when is_list(Records) ->
            recover_buffer_writer(FD, Filename, Reader, NWritten + length(Records));
        {Unexpected, _Reader} ->
            %% Buffer is corrupted, should be discarded.
            {error, {buffer_unexpected_record, Unexpected}};
        eof ->
            %% Buffer is fine, continue writing at the end.
            {ok, FD, NWritten}
    catch
        error:Reason ->
            %% Buffer is truncated or corrupted somewhere in the middle.
            %% Continue writing after the last valid record.
            ?SLOG(warning, #{
                msg => "existing_buffer_recovered_partially",
                filename => Filename,
                reason => Reason,
                details =>
                    "Buffer is truncated or corrupted somewhere in the middle. "
                    "Corrupted records will be discarded."
            }),
            Writer = emqx_connector_aggreg_buffer:takeover(Reader0),
            {ok, Writer, NWritten}
    end.

mk_buffer(
    Since,
    Seq,
    #st{tab = Tab, interval = Interval, max_records = MaxRecords, work_dir = WorkDir}
) ->
    Name = mk_filename(Since, Seq),
    Counter = {Tab, {Since, Seq}},
    #buffer{
        since = Since,
        until = Since + Interval,
        seq = Seq,
        filename = filename:join(WorkDir, Name),
        max_records = MaxRecords,
        cnt_records = Counter
    }.

handle_close_buffer(
    Timestamp,
    St0 = #st{buffer = Buffer = #buffer{until = Until}}
) when Timestamp >= Until ->
    St = St0#st{buffer = undefined},
    _ = announce_current_buffer(St),
    enqueue_delivery(close_buffer(Buffer), St);
handle_close_buffer(_Timestamp, St = #st{buffer = undefined}) ->
    St.

close_buffer(Buffer = #buffer{fd = FD}) ->
    ok = file:close(FD),
    Buffer#buffer{fd = undefined}.

discard_buffer(#buffer{filename = Filename, cnt_records = Counter}) ->
    %% NOTE: Hopefully, no process is touching this counter anymore.
    _ = del_counter(Counter),
    file:delete(Filename).

pick_buffer(Name, Timestamp) ->
    case lookup_current_buffer(Name) of
        #buffer{until = Until} = Buffer when Timestamp < Until ->
            Buffer;
        #buffer{since = Since} when Timestamp < Since ->
            %% TODO: Support timestamps going back.
            error({invalid_timestamp, Timestamp});
        _Outdated ->
            undefined
    end.

announce_current_buffer(#st{tab = Tab, buffer = Buffer}) ->
    ets:insert(Tab, {buffer, Buffer}).

lookup_current_buffer(Name) ->
    ets:lookup_element(lookup_tab(Name), buffer, 2).

%%

trigger_enqueue_delivery() ->
    gen_server:cast(self(), enqueue_delivery).

enqueue_delivery(Buffer, St = #st{name = Name, deliveries = Ds}) ->
    case emqx_connector_aggreg_upload_sup:start_delivery(Name, Buffer) of
        {ok, Pid} ->
            MRef = erlang:monitor(process, Pid),
            St#st{deliveries = Ds#{MRef => Buffer}};
        {error, _} = Error ->
            handle_delivery_exit(Buffer, Error, St)
    end.

handle_delivery_exit(Buffer, Normal, St = #st{name = Name}) when
    Normal == normal; Normal == noproc
->
    ?tp(debug, "aggregated_buffer_delivery_completed", #{
        action => Name,
        buffer => Buffer#buffer.filename
    }),
    ok = discard_buffer(Buffer),
    St;
handle_delivery_exit(Buffer, {shutdown, {skipped, Reason}}, St = #st{name = Name}) ->
    ?tp(info, "aggregated_buffer_delivery_skipped", #{
        action => Name,
        buffer => {Buffer#buffer.since, Buffer#buffer.seq},
        reason => Reason
    }),
    ok = discard_buffer(Buffer),
    St;
handle_delivery_exit(Buffer, Error, St = #st{name = Name}) ->
    ?tp(error, "aggregated_buffer_delivery_failed", #{
        action => Name,
        buffer => {Buffer#buffer.since, Buffer#buffer.seq},
        filename => Buffer#buffer.filename,
        reason => Error
    }),
    %% TODO: Retries?
    enqueue_status_error(Error, St).

enqueue_status_error({upload_failed, Error}, St = #st{errors = QErrors}) ->
    %% TODO
    %% This code feels too specific, errors probably need classification.
    St#st{errors = queue:in(Error, QErrors)};
enqueue_status_error(_AnotherError, St = #st{name = Name}) ->
    ?SLOG(debug, #{
        msg => "aggregated_buffer_error_not_enqueued",
        error => _AnotherError,
        action => Name
    }),
    St.

handle_take_error(St = #st{errors = QErrors0}) ->
    case queue:out(QErrors0) of
        {{value, Error}, QErrors} ->
            {[Error], St#st{errors = QErrors}};
        {empty, QErrors} ->
            {[], St#st{errors = QErrors}}
    end.

%%

recover(St0 = #st{work_dir = WorkDir}) ->
    {ok, Filenames} = file:list_dir(WorkDir),
    ExistingBuffers = lists:flatmap(fun(FN) -> read_existing_file(FN, St0) end, Filenames),
    case lists:reverse(lists:keysort(#buffer.since, ExistingBuffers)) of
        [Buffer | ClosedBuffers] ->
            St = lists:foldl(fun enqueue_delivery/2, St0, ClosedBuffers),
            St#st{buffer = recover_buffer(Buffer)};
        [] ->
            St0
    end.

read_existing_file("MANIFEST", _St) ->
    [];
read_existing_file(Name, St) ->
    case parse_filename(Name) of
        {Since, Seq} ->
            [read_existing_buffer(Since, Seq, Name, St)];
        error ->
            %% TODO: log?
            []
    end.

read_existing_buffer(Since, Seq, Name, St = #st{work_dir = WorkDir}) ->
    Filename = filename:join(WorkDir, Name),
    Buffer = mk_buffer(Since, Seq, St),
    Buffer#buffer{filename = Filename}.

%%

mk_filename(Since, Seq) ->
    "T" ++ integer_to_list(Since) ++ "_" ++ pad_number(Seq, 4).

parse_filename(Filename) ->
    case re:run(Filename, "^T(\\d+)_(\\d+)$", [{capture, all_but_first, list}]) of
        {match, [Since, Seq]} ->
            {list_to_integer(Since), list_to_integer(Seq)};
        nomatch ->
            error
    end.

%%

-define(COUNTER_POS, 2).

add_counter({Tab, Counter}) ->
    add_counter({Tab, Counter}, 0).

add_counter({Tab, Counter}, N) ->
    ets:insert(Tab, {Counter, N}).

inc_counter({Tab, Counter}, Size) ->
    ets:update_counter(Tab, Counter, {?COUNTER_POS, Size}).

del_counter({Tab, Counter}) ->
    ets:delete(Tab, Counter).

-undef(COUNTER_POS).

%%

create_tab(Name) ->
    Tab = ets:new(?MODULE, [public, set, {write_concurrency, auto}]),
    ok = persistent_term:put({?MODULE, Name}, Tab),
    Tab.

lookup_tab(Name) ->
    persistent_term:get({?MODULE, Name}).

cleanup_tab(Name) ->
    persistent_term:erase({?MODULE, Name}).

%%

pad_number(I, L) ->
    string:pad(integer_to_list(I), L, leading, $0).
