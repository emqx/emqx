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

-module(emqx_retainer_dispatcher).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").

%% API
-export([
    start_link/2,
    dispatch/2,
    refresh_limiter/0,
    refresh_limiter/1,
    wait_dispatch_complete/1,
    worker/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/2
]).

-type limiter() :: emqx_htb_limiter:limiter().

-define(POOL, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================
dispatch(Context, Topic) ->
    cast({?FUNCTION_NAME, Context, self(), Topic}).

%% reset the client's limiter after updated the limiter's config
refresh_limiter() ->
    Conf = emqx:get_config([retainer]),
    refresh_limiter(Conf).

refresh_limiter(Conf) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            gen_server:cast(Pid, {?FUNCTION_NAME, Conf})
        end,
        Workers
    ).

wait_dispatch_complete(Timeout) ->
    Workers = gproc_pool:active_workers(?POOL),
    lists:foreach(
        fun({_, Pid}) ->
            ok = gen_server:call(Pid, ?FUNCTION_NAME, Timeout)
        end,
        Workers
    ).

worker() ->
    gproc_pool:pick_worker(?POOL, self()).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(atom(), pos_integer()) ->
    {ok, Pid :: pid()}
    | {error, Error :: {already_started, pid()}}
    | {error, Error :: term()}
    | ignore.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_misc:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: term()}
    | {ok, State :: term(), Timeout :: timeout()}
    | {ok, State :: term(), hibernate}
    | {stop, Reason :: term()}
    | ignore.
init([Pool, Id]) ->
    erlang:process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    BucketName = emqx:get_config([retainer, flow_control, batch_deliver_limiter], undefined),
    {ok, Limiter} = emqx_limiter_server:connect(batch, BucketName),
    {ok, #{pool => Pool, id => Id, limiter => Limiter}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()}
    | {reply, Reply :: term(), NewState :: term(), hibernate}
    | {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.
handle_call(wait_dispatch_complete, _From, State) ->
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), NewState :: term()}.
handle_cast({dispatch, Context, Pid, Topic}, #{limiter := Limiter} = State) ->
    {ok, Limiter2} = dispatch(Context, Pid, Topic, undefined, Limiter),
    {noreply, State#{limiter := Limiter2}};
handle_cast({refresh_limiter, Conf}, State) ->
    BucketName = emqx_map_lib:deep_get([flow_control, batch_deliver_limiter], Conf, undefined),
    {ok, Limiter} = emqx_limiter_server:connect(batch, BucketName),
    {noreply, State#{limiter := Limiter}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term()
) -> any().
terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).
%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(
    OldVsn :: term() | {down, term()},
    State :: term(),
    Extra :: term()
) ->
    {ok, NewState :: term()}
    | {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(
    Opt :: normal | terminate,
    Status :: list()
) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% @private
cast(Msg) ->
    gen_server:cast(worker(), Msg).

-spec dispatch(context(), pid(), topic(), cursor(), limiter()) -> {ok, limiter()}.
dispatch(Context, Pid, Topic, Cursor, Limiter) ->
    Mod = emqx_retainer:get_backend_module(),
    case Cursor =/= undefined orelse emqx_topic:wildcard(Topic) of
        false ->
            {ok, Result} = erlang:apply(Mod, read_message, [Context, Topic]),
            deliver(Result, Context, Pid, Topic, undefined, Limiter);
        true ->
            {ok, Result, NewCursor} = erlang:apply(Mod, match_messages, [Context, Topic, Cursor]),
            deliver(Result, Context, Pid, Topic, NewCursor, Limiter)
    end.

-spec deliver(list(emqx_types:message()), context(), pid(), topic(), cursor(), limiter()) ->
    {ok, limiter()}.
deliver([], _Context, _Pid, _Topic, undefined, Limiter) ->
    {ok, Limiter};
deliver([], Context, Pid, Topic, Cursor, Limiter) ->
    dispatch(Context, Pid, Topic, Cursor, Limiter);
deliver(Result, Context, Pid, Topic, Cursor, Limiter) ->
    case erlang:is_process_alive(Pid) of
        false ->
            {ok, Limiter};
        _ ->
            DeliverNum = emqx_conf:get([retainer, flow_control, batch_deliver_number], undefined),
            case DeliverNum of
                0 ->
                    do_deliver(Result, Pid, Topic),
                    {ok, Limiter};
                _ ->
                    case do_deliver(Result, DeliverNum, Pid, Topic, Limiter) of
                        {ok, Limiter2} ->
                            deliver([], Context, Pid, Topic, Cursor, Limiter2);
                        {drop, Limiter2} ->
                            {ok, Limiter2}
                    end
            end
    end.

do_deliver([], _DeliverNum, _Pid, _Topic, Limiter) ->
    {ok, Limiter};
do_deliver(Msgs, DeliverNum, Pid, Topic, Limiter) ->
    {Num, ToDelivers, Msgs2} = safe_split(DeliverNum, Msgs),
    case emqx_htb_limiter:consume(Num, Limiter) of
        {ok, Limiter2} ->
            do_deliver(ToDelivers, Pid, Topic),
            do_deliver(Msgs2, DeliverNum, Pid, Topic, Limiter2);
        {drop, _} = Drop ->
            ?SLOG(debug, #{
                msg => "retained_message_dropped",
                reason => "reached_ratelimit",
                dropped_count => length(ToDelivers)
            }),
            Drop
    end.

do_deliver([Msg | T], Pid, Topic) ->
    Pid ! {deliver, Topic, Msg},
    do_deliver(T, Pid, Topic);
do_deliver([], _, _) ->
    ok.

safe_split(N, List) ->
    safe_split(N, List, 0, []).

safe_split(0, List, Count, Acc) ->
    {Count, lists:reverse(Acc), List};
safe_split(_N, [], Count, Acc) ->
    {Count, lists:reverse(Acc), []};
safe_split(N, [H | T], Count, Acc) ->
    safe_split(N - 1, T, Count + 1, [H | Acc]).
