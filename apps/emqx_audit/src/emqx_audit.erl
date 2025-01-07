%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_audit).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_audit.hrl").

%% API
-export([start_link/0]).
-export([log/3]).

-export([trans_clean_expired/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(FILTER_REQ, [cert, host_info, has_sent_resp, pid, path_info, peer, ref, sock, streamid]).

-define(CHARS_LIMIT_IN_DB, 1024).

-ifdef(TEST).
-define(INTERVAL, 100).
-else.
-define(INTERVAL, 10000).
-endif.

to_audit(#{from := cli, cmd := Cmd, args := Args, duration_ms := DurationMs}) ->
    #?AUDIT{
        operation_id = <<"">>,
        operation_type = atom_to_binary(Cmd),
        args = Args,
        operation_result = <<"">>,
        failure = <<"">>,
        duration_ms = DurationMs,
        from = cli,
        source = <<"">>,
        source_ip = <<"">>,
        http_status_code = <<"">>,
        http_method = <<"">>,
        http_request = <<"">>
    };
to_audit(#{from := erlang_console, function := F, args := Args}) ->
    #?AUDIT{
        from = erlang_console,
        source = <<"">>,
        source_ip = <<"">>,
        %% operation info
        operation_id = <<"">>,
        operation_type = <<"">>,
        operation_result = <<"">>,
        failure = <<"">>,
        %% request detail
        http_status_code = <<"">>,
        http_method = <<"">>,
        http_request = <<"">>,
        duration_ms = 0,
        args = iolist_to_binary(io_lib:format("~p: ~ts", [F, Args]))
    };
to_audit(#{from := From} = Log) when is_atom(From) ->
    #{
        source := Source,
        source_ip := SourceIp,
        %% operation info
        operation_id := OperationId,
        operation_type := OperationType,
        operation_result := OperationResult,
        %% request detail
        http_status_code := StatusCode,
        http_method := Method,
        http_request := Request,
        duration_ms := DurationMs
    } = Log,
    #?AUDIT{
        from = From,
        source = Source,
        source_ip = SourceIp,
        %% operation info
        operation_id = OperationId,
        operation_type = OperationType,
        operation_result = OperationResult,
        failure = maps:get(failure, Log, <<"">>),
        %% request detail
        http_status_code = StatusCode,
        http_method = Method,
        http_request = truncate_http_body(Request),
        duration_ms = DurationMs,
        args = <<"">>
    }.

log(_Level, undefined, _Handler) ->
    ok;
log(Level, Meta1, Handler) ->
    Meta2 = Meta1#{time => logger:timestamp(), level => Level},
    log_to_file(Level, Meta2, Handler),
    log_to_db(Meta2),
    remove_handler_when_disabled().

remove_handler_when_disabled() ->
    case emqx_config:get([log, audit, enable], false) of
        true ->
            ok;
        false ->
            _ = logger:remove_handler(?AUDIT_HANDLER),
            ok
    end.

log_to_db(Log) ->
    Audit0 = to_audit(Log),
    Audit = Audit0#?AUDIT{
        node = node(),
        created_at = erlang:system_time(microsecond)
    },
    mria:dirty_write(?AUDIT, Audit).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ok = mria:create_table(?AUDIT, [
        {type, ordered_set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?AUDIT},
        {attributes, record_info(fields, ?AUDIT)}
    ]),
    {ok, #{}, {continue, setup}}.

handle_continue(setup, State) ->
    ok = mria:wait_for_tables([?AUDIT]),
    NewState = State#{role => mria_rlog:role()},
    ?AUDIT(alert, #{
        cmd => emqx,
        args => [<<"start">>],
        version => emqx_release:version(),
        from => cli,
        duration_ms => 0
    }),
    {noreply, NewState, interval(NewState)}.

handle_call(_Request, _From, State) ->
    {reply, ignore, State, interval(State)}.

handle_cast(_Request, State) ->
    {noreply, State, interval(State)}.

handle_info(timeout, State) ->
    ExtraWait = clean_expired_logs(),
    {noreply, State, interval(State) + ExtraWait};
handle_info(_Info, State) ->
    {noreply, State, interval(State)}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% if clean_expired transaction aborted, it will be scheduled with extra 60 seconds.
clean_expired_logs() ->
    MaxSize = max_size(),
    Oldest = mnesia:dirty_first(?AUDIT),
    CurSize = mnesia:table_info(?AUDIT, size),
    case CurSize - MaxSize of
        DelSize when DelSize > 0 ->
            case
                mria:transaction(
                    ?COMMON_SHARD,
                    fun ?MODULE:trans_clean_expired/2,
                    [Oldest, DelSize]
                )
            of
                {atomic, ok} ->
                    0;
                {aborted, Reason} ->
                    ?SLOG(error, #{
                        msg => "clean_expired_audit_aborted",
                        reason => Reason,
                        delete_size => DelSize,
                        current_size => CurSize,
                        max_count => MaxSize
                    }),
                    60000
            end;
        _ ->
            0
    end.

trans_clean_expired(Oldest, DelCount) ->
    First = mnesia:first(?AUDIT),
    %% Other node already clean from the oldest record.
    %% ensure not delete twice, otherwise records that should not be deleted will be deleted.
    case First =:= Oldest of
        true -> do_clean_expired(First, DelCount);
        false -> ok
    end.

do_clean_expired(_, DelSize) when DelSize =< 0 -> ok;
do_clean_expired('$end_of_table', _DelSize) ->
    ok;
do_clean_expired(CurKey, DeleteSize) ->
    mnesia:delete(?AUDIT, CurKey, sticky_write),
    do_clean_expired(mnesia:next(?AUDIT, CurKey), DeleteSize - 1).

max_size() ->
    emqx_conf:get([log, audit, max_filter_size], 5000).

interval(#{role := replicant}) -> hibernate;
interval(#{role := core}) -> ?INTERVAL + rand:uniform(?INTERVAL).

log_to_file(Level, Meta, #{module := Module} = Handler) ->
    Log = #{level => Level, meta => Meta, msg => undefined},
    Handler1 = maps:without(?OWN_KEYS, Handler),
    try
        erlang:apply(Module, log, [Log, Handler1])
    catch
        C:R:S ->
            case logger:remove_handler(?AUDIT_HANDLER) of
                ok ->
                    logger:internal_log(
                        error, {removed_failing_handler, ?AUDIT_HANDLER, C, R, S}
                    );
                {error, {not_found, _}} ->
                    ok;
                {error, Reason} ->
                    logger:internal_log(
                        error,
                        {removed_handler_failed, ?AUDIT_HANDLER, Reason, C, R, S}
                    )
            end
    end.

truncate_http_body(Req = #{body := Body}) ->
    Req#{body => truncate_large_term(Body)};
truncate_http_body(Req) ->
    Req.

truncate_large_term(Req) ->
    unicode:characters_to_binary(io_lib:format("~0p", [Req], [{chars_limit, ?CHARS_LIMIT_IN_DB}])).
