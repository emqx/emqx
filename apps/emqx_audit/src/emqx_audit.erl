%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_audit).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_audit.hrl").

%% API
-export([start_link/0]).
-export([log/1, log/2]).

-export([dirty_clean_expired/1]).

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

-ifdef(TEST).
-define(INTERVAL, 100).
-else.
-define(INTERVAL, 2500).
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
to_audit(#{http_method := get}) ->
    ok;
to_audit(#{from := From} = Log) when From =:= dashboard orelse From =:= rest_api ->
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
        http_request = Request,
        duration_ms = DurationMs,
        args = <<"">>
    };
to_audit(#{from := event, event := Event}) ->
    #?AUDIT{
        from = event,
        source = <<"">>,
        source_ip = <<"">>,
        %% operation info
        operation_id = iolist_to_binary(Event),
        operation_type = <<"">>,
        operation_result = <<"">>,
        failure = <<"">>,
        %% request detail
        http_status_code = <<"">>,
        http_method = <<"">>,
        http_request = <<"">>,
        duration_ms = 0,
        args = <<"">>
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
        args = iolist_to_binary(io_lib:format("~p: ~p~n", [F, Args]))
    }.

log(_Level, undefined) ->
    ok;
log(Level, Meta1) ->
    Meta2 = Meta1#{time => logger:timestamp(), level => Level},
    Filter = [{emqx_audit, fun(L, _) -> L end, undefined, undefined}],
    emqx_trace:log(Level, Filter, undefined, Meta2),
    emqx_audit:log(Meta2).

log(Log) ->
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
    case mria_rlog:role() of
        core -> {ok, #{}, {continue, setup}};
        _ -> {ok, #{}}
    end.

handle_continue(setup, State) ->
    ok = mria:wait_for_tables([?AUDIT]),
    clean_expired(),
    Interval = clean_expired_interval(),
    {noreply, State#{interval => Interval}, Interval}.

handle_call(_Request, _From, State = #{interval := Interval}) ->
    {reply, ignore, State, Interval}.

handle_cast(_Request, State = #{interval := Interval}) ->
    {noreply, State, Interval}.

handle_info(timeout, State = #{interval := Interval}) ->
    clean_expired(),
    {noreply, State, Interval};
handle_info(_Info, State = #{interval := Interval}) ->
    {noreply, State, Interval}.

terminate(_Reason, _State = #{}) ->
    ok.

code_change(_OldVsn, State = #{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

clean_expired() ->
    MaxSize = max_size(),
    CurSize = mnesia:table_info(?AUDIT, size),
    case CurSize - MaxSize of
        DelCount when DelCount > 0 ->
            mria:async_dirty(
                ?COMMON_SHARD,
                fun ?MODULE:dirty_clean_expired/1,
                [DelCount]
            );
        _ ->
            ok
    end.

dirty_clean_expired(DelCount) ->
    dirty_clean_expired(mnesia:dirty_first(?AUDIT), DelCount).

dirty_clean_expired(_, DelCount) when DelCount =< 0 -> ok;
dirty_clean_expired('$end_of_table', _DelCount) ->
    ok;
dirty_clean_expired(CurKey, DeleteCount) ->
    mnesia:dirty_delete(?AUDIT, CurKey),
    dirty_clean_expired(mnesia:dirty_next(?AUDIT, CurKey), DeleteCount - 1).

max_size() ->
    emqx_conf:get([log, audit, max_filter_size], 5000).

%% Try to make the time interval of each node is different.
%% 2 * Interval ~ 3 * Interval (5000~7500)
clean_expired_interval() ->
    Interval = ?INTERVAL,
    Interval * 2 + erlang:phash2(node(), Interval).
