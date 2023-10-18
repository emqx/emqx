%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_audit).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_audit.hrl").

%% API
-export([start_link/0]).
-export([log/1, log/2]).

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
-define(CLEAN_EXPIRED_MS, 60 * 1000).

to_audit(#{from := cli, cmd := Cmd, args := Args, duration_ms := DurationMs}) ->
    #?AUDIT{
        created_at = erlang:system_time(microsecond),
        node = node(),
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
        created_at = erlang:system_time(microsecond),
        node = node(),
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
        created_at = erlang:system_time(microsecond),
        node = node(),
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
        created_at = erlang:system_time(microsecond),
        node = node(),
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
    gen_server:cast(?MODULE, {write, to_audit(Log)}).

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

handle_continue(setup, #{} = State) ->
    ok = mria:wait_for_tables([?AUDIT]),
    clean_expired(),
    {noreply, State}.

handle_call(_Request, _From, State = #{}) ->
    {reply, ok, State}.

handle_cast({write, Log}, State) ->
    _ = write_log(Log),
    {noreply, State#{}, ?CLEAN_EXPIRED_MS};
handle_cast(_Request, State = #{}) ->
    {noreply, State}.

handle_info(timeout, State = #{}) ->
    clean_expired(),
    {noreply, State, hibernate};
handle_info(_Info, State = #{}) ->
    {noreply, State}.

terminate(_Reason, _State = #{}) ->
    ok.

code_change(_OldVsn, State = #{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

write_log(Log) ->
    case
        mria:transaction(
            ?COMMON_SHARD,
            fun(L) ->
                New =
                    case mnesia:last(?AUDIT) of
                        '$end_of_table' -> 1;
                        LastId -> LastId + 1
                    end,
                mnesia:write(L#?AUDIT{seq = New})
            end,
            [Log]
        )
    of
        {atomic, ok} ->
            ok;
        Reason ->
            ?SLOG(warning, #{
                msg => "write_audit_log_failed",
                reason => Reason
            })
    end.

clean_expired() ->
    MaxSize = max_size(),
    LatestId = latest_id(),
    Min = LatestId - MaxSize,
    %% MS = ets:fun2ms(fun(#?AUDIT{seq = Seq}) when Seq =< Min -> true end),
    MS = [{#?AUDIT{seq = '$1', _ = '_'}, [{'=<', '$1', Min}], [true]}],
    NumDeleted = mnesia:ets(fun ets:select_delete/2, [?AUDIT, MS]),
    ?SLOG(debug, #{
        msg => "clean_audit_log",
        latest_id => LatestId,
        min => Min,
        deleted_number => NumDeleted
    }),
    ok.

latest_id() ->
    case mnesia:dirty_last(?AUDIT) of
        '$end_of_table' -> 0;
        Seq -> Seq
    end.

max_size() ->
    emqx_conf:get([log, audit, max_filter_size], 5000).
