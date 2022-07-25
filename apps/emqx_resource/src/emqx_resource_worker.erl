%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% An FIFO queue using ETS-ReplayQ as backend.

-module(emqx_resource_worker).

-include("emqx_resource.hrl").
-include("emqx_resource_utils.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(gen_statem).

-export([
    start_link/2,
    query/2,
    query_async/3,
    query_mfa/3
]).

-export([
    callback_mode/0,
    init/1
]).

-export([do/3]).

%% count
-define(DEFAULT_BATCH_SIZE, 100).
%% milliseconds
-define(DEFAULT_BATCH_TIME, 10).

-define(QUERY(FROM, REQUEST), {FROM, REQUEST}).
-define(REPLY(FROM, REQUEST, RESULT), {FROM, REQUEST, RESULT}).
-define(EXPAND(RESULT, BATCH), [?REPLY(FROM, REQUEST, RESULT) || ?QUERY(FROM, REQUEST) <- BATCH]).

-type id() :: binary().
-type request() :: term().
-type result() :: term().
-type reply_fun() :: {fun((result(), Args :: term()) -> any()), Args :: term()}.
-type from() :: pid() | reply_fun().

-callback batcher_flush(Acc :: [{from(), request()}], CbState :: term()) ->
    {{from(), result()}, NewCbState :: term()}.

callback_mode() -> [state_functions].

start_link(Id, Opts) ->
    gen_statem:start_link({local, name(Id)}, ?MODULE, {Id, Opts}, []).

-spec query(id(), request()) -> ok.
query(Id, Request) ->
    gen_statem:call(name(Id), {query, Request}).

-spec query_async(id(), request(), reply_fun()) -> ok.
query_async(Id, Request, ReplyFun) ->
    gen_statem:cast(name(Id), {query, Request, ReplyFun}).

-spec name(id()) -> atom().
name(Id) ->
    Mod = atom_to_binary(?MODULE, utf8),
    <<Mod/binary, ":", Id/binary>>.

disk_cache_dir(Id) ->
    filename:join([emqx:data_dir(), Id, cache]).

init({Id, Opts}) ->
    BatchSize = maps:get(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    Queue =
        case maps:get(cache_enabled, Opts, true) of
            true -> replayq:open(#{dir => disk_cache_dir(Id), seg_bytes => 10000000});
            false -> undefined
        end,
    St = #{
        id => Id,
        batch_enabled => maps:get(batch_enabled, Opts, true),
        batch_size => BatchSize,
        batch_time => maps:get(batch_time, Opts, ?DEFAULT_BATCH_TIME),
        cache_queue => Queue,
        acc => [],
        acc_left => BatchSize,
        tref => undefined
    },
    {ok, do, St}.

do(cast, {query, Request, ReplyFun}, #{batch_enabled := true} = State) ->
    do_acc(ReplyFun, Request, State);
do(cast, {query, Request, ReplyFun}, #{batch_enabled := false} = State) ->
    do_query(ReplyFun, Request, State);
do({call, From}, {query, Request}, #{batch_enabled := true} = State) ->
    do_acc(From, Request, State);
do({call, From}, {query, Request}, #{batch_enabled := false} = State) ->
    do_query(From, Request, State);
do(info, {flush, Ref}, St = #{tref := {_TRef, Ref}}) ->
    {keep_state, flush(St#{tref := undefined})};
do(info, {flush, _Ref}, _St) ->
    keep_state_and_data;
do(info, Info, _St) ->
    ?SLOG(error, #{msg => unexpected_msg, info => Info}),
    keep_state_and_data.

do_acc(From, Request, #{acc := Acc, acc_left := Left} = St0) ->
    Acc1 = [?QUERY(From, Request) | Acc],
    St = St0#{acc := Acc1, acc_left := Left - 1},
    case Left =< 1 of
        true -> {keep_state, flush(St)};
        false -> {keep_state, ensure_flush_timer(St)}
    end.

do_query(From, Request, #{id := Id, cache_queue := Q0} = St0) ->
    Result = call_query(Id, Request),
    Q1 = reply_caller(Id, Q0, ?REPLY(From, Request, Result)),
    {keep_state, St0#{cache_queue := Q1}}.

flush(#{acc := []} = St) ->
    St;
flush(
    #{
        id := Id,
        acc := Batch,
        batch_size := Size,
        cache_queue := Q0
    } = St
) ->
    BatchResults = call_batch_query(Id, Batch),
    Q1 = batch_reply_caller(Id, Q0, BatchResults),
    cancel_flush_timer(
        St#{
            acc_left := Size,
            acc := [],
            cache_queue := Q1
        }
    ).

maybe_append_cache(undefined, _Request) -> undefined;
maybe_append_cache(Q, Request) -> replayq:append(Q, Request).

batch_reply_caller(Id, Q, BatchResults) ->
    lists:foldl(
        fun(Reply, Q1) ->
            reply_caller(Id, Q1, Reply)
        end,
        Q,
        BatchResults
    ).

reply_caller(Id, Q, ?REPLY({ReplyFun, Args}, Request, Result)) when is_function(ReplyFun) ->
    ?SAFE_CALL(ReplyFun(Result, Args)),
    handle_query_result(Id, Q, Request, Result);
reply_caller(Id, Q, ?REPLY(From, Request, Result)) ->
    gen_statem:reply(From, Result),
    handle_query_result(Id, Q, Request, Result).

handle_query_result(Id, Q, _Request, ok) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, success),
    Q;
handle_query_result(Id, Q, _Request, {ok, _}) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, success),
    Q;
handle_query_result(Id, Q, _Request, {error, _}) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, failed),
    Q;
handle_query_result(Id, Q, Request, {error, {resource_error, #{reason := not_connected}}}) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, resource_error),
    maybe_append_cache(Q, Request);
handle_query_result(Id, Q, _Request, {error, {resource_error, #{}}}) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, resource_error),
    Q;
handle_query_result(Id, Q, Request, {error, {exception, _}}) ->
    emqx_metrics_worker:inc(?RES_METRICS, Id, exception),
    maybe_append_cache(Q, Request).

call_query(Id, Request) ->
    ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched),
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{mod := Mod, state := ResourceState, status := connected}} ->
            try Mod:on_query(Id, Request, ResourceState) of
                Result -> Result
            catch
                Err:Reason:ST ->
                    ModB = atom_to_binary(Mod, utf8),
                    Msg = <<"call failed, func: ", ModB/binary, ":on_query/3">>,
                    exception_error(Reason, Msg, {Err, Reason, ST})
            end;
        {ok, _Group, #{status := stopped}} ->
            resource_error(stopped, <<"resource stopped or disabled">>);
        {ok, _Group, _Data} ->
            resource_error(not_connected, <<"resource not connected">>);
        {error, not_found} ->
            resource_error(not_found, <<"resource not found">>)
    end.

call_batch_query(Id, Batch) ->
    ok = emqx_metrics_worker:inc(?RES_METRICS, Id, matched, length(Batch)),
    case emqx_resource_manager:ets_lookup(Id) of
        {ok, _Group, #{mod := Mod, state := ResourceState, status := connected}} ->
            try Mod:on_batch_query(Id, Batch, ResourceState) of
                BatchResults -> BatchResults
            catch
                Err:Reason:ST ->
                    ModB = atom_to_binary(Mod, utf8),
                    Msg = <<"call failed, func: ", ModB/binary, ":on_batch_query/3">>,
                    ?EXPAND(exception_error(Reason, Msg, {Err, Reason, ST}), Batch)
            end;
        {ok, _Group, _Data} ->
            ?EXPAND(resource_error(not_connected, <<"resource not connected">>), Batch);
        {error, not_found} ->
            ?EXPAND(resource_error(not_found, <<"resource not found">>), Batch)
    end.

resource_error(Reason, Msg) ->
    {error, {resource_error, #{reason => Reason, msg => Msg}}}.
exception_error(Reason, Msg, Details) ->
    {error, {exception, #{reason => Reason, msg => Msg, details => Details}}}.

%% ==========================================
ensure_flush_timer(St = #{tref := undefined, batch_time := T}) ->
    Ref = make_ref(),
    TRef = erlang:send_after(T, self(), {flush, Ref}),
    St#{tref => {TRef, Ref}};
ensure_flush_timer(St) ->
    St.

cancel_flush_timer(St = #{tref := undefined}) ->
    St;
cancel_flush_timer(St = #{tref := {TRef, _Ref}}) ->
    _ = erlang:cancel_timer(TRef),
    St#{tref => undefined}.

query_mfa(InsertMode, Request, SyncTimeout) ->
    {?MODULE, query_fun(InsertMode), query_args(InsertMode, Request, SyncTimeout)}.

query_fun(<<"sync">>) -> query;
query_fun(<<"async">>) -> query_async.

query_args(<<"sync">>, Request, SyncTimeout) ->
    [Request, SyncTimeout];
query_args(<<"async">>, Request, _) ->
    [Request].
