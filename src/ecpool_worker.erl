%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ecpool_worker).

-include("ecpool.hrl").

-behaviour(gen_server).

-export([start_link/4]).

%% API Function Exports
-export([ client/1
        , exec/3
        , exec_async/2
        , exec_async/3
        , is_connected/1
        , set_reconnect_callback/2
        , set_disconnect_callback/2
        , add_reconnect_callback/2
        , add_disconnect_callback/2
        ]).

%% gen_server Function Exports
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
          pool :: ecpool:poo_name(),
          id :: pos_integer(),
          client :: pid() | undefined,
          mod :: module(),
          on_reconnect :: ecpool:conn_callback(),
          on_disconnect :: ecpool:conn_callback(),
          supervisees = [],
          opts :: proplists:proplist()
         }).

%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------

-callback(connect(ConnOpts :: list())
          -> {ok, pid()} | {error, Reason :: term()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a pool worker.
-spec(start_link(atom(), pos_integer(), module(), list()) ->
      {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Mod, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Id, Mod, Opts], []).

%% @doc Get client/connection.
-spec(client(pid()) -> {ok, Client :: pid()} | {error, Reason :: term()}).
client(Pid) ->
    gen_server:call(Pid, client, infinity).

-spec(exec(pid(), action(), timeout()) -> Result :: any() | {error, Reason :: term()}).
exec(Pid, Action, Timeout) ->
    gen_server:call(Pid, {exec, Action}, Timeout).

-spec exec_async(pid(), action()) -> ok.
exec_async(Pid, Action) ->
    gen_server:cast(Pid, {exec_async, Action}).

-spec exec_async(pid(), action(), callback()) -> ok.
exec_async(Pid, Action, Callback) ->
    gen_server:cast(Pid, {exec_async, Action, Callback}).

%% @doc Is client connected?
-spec(is_connected(pid()) -> boolean()).
is_connected(Pid) ->
    gen_server:call(Pid, is_connected, infinity).

-spec(set_reconnect_callback(pid(), ecpool:conn_callback()) -> ok).
set_reconnect_callback(Pid, OnReconnect = {_, _, _}) ->
    gen_server:cast(Pid, {set_reconn_callbk, OnReconnect}).

-spec(set_disconnect_callback(pid(), ecpool:conn_callback()) -> ok).
set_disconnect_callback(Pid, OnDisconnect = {_, _, _}) ->
    gen_server:cast(Pid, {set_disconn_callbk, OnDisconnect}).

-spec(add_reconnect_callback(pid(), ecpool:conn_callback()) -> ok).
add_reconnect_callback(Pid, OnReconnect  = {_, _, _}) ->
    gen_server:cast(Pid, {add_reconn_callbk, OnReconnect}).

-spec(add_disconnect_callback(pid(), ecpool:conn_callback()) -> ok).
add_disconnect_callback(Pid, OnDisconnect = {_, _, _}) ->
    gen_server:cast(Pid, {add_disconn_callbk, OnDisconnect}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Mod, Opts]) ->
    process_flag(trap_exit, true),
    State = #state{pool = Pool,
                   id   = Id,
                   mod  = Mod,
                   opts = Opts,
                   on_reconnect = ensure_callback(proplists:get_value(on_reconnect, Opts)),
                   on_disconnect = ensure_callback(proplists:get_value(on_disconnect, Opts))
                  },
    case connect_internal(State) of
        {ok, NewState} ->
            gproc_pool:connect_worker(ecpool:name(Pool), {Pool, Id}),
            {ok, NewState};
        {error, Error} -> {stop, Error}
    end.

handle_call(is_connected, _From, State = #state{client = Client}) when is_pid(Client) ->
    IsAlive = Client =/= undefined andalso is_process_alive(Client),
    {reply, IsAlive, State};

handle_call(is_connected, _From, State = #state{client = Client}) ->
    {reply, Client =/= undefined, State};

handle_call(client, _From, State = #state{client = undefined}) ->
    {reply, {error, disconnected}, State};

handle_call(client, _From, State = #state{client = Client}) ->
    {reply, {ok, Client}, State};

handle_call({exec, Action}, _From, State = #state{client = Client}) ->
    {reply, safe_exec(Action, Client), State};

handle_call(Req, _From, State) ->
    logger:error("[PoolWorker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({exec_async, Action}, State = #state{client = Client}) ->
    _ = safe_exec(Action, Client),
    {noreply, State};

handle_cast({exec_async, Action, Callback}, State = #state{client = Client}) ->
    _ = safe_exec(Callback, safe_exec(Action, Client)),
    {noreply, State};

handle_cast({set_reconn_callbk, OnReconnect}, State) ->
    {noreply, State#state{on_reconnect = OnReconnect}};

handle_cast({set_disconn_callbk, OnDisconnect}, State) ->
    {noreply, State#state{on_disconnect = OnDisconnect}};

handle_cast({add_reconn_callbk, OnReconnect}, State = #state{on_reconnect = OldOnReconnect}) ->
    {noreply, State#state{on_reconnect = add_conn_callback(OnReconnect, OldOnReconnect)}};

handle_cast({add_disconn_callbk, OnDisconnect}, State = #state{on_disconnect = OldOnDisconnect}) ->
    {noreply, State#state{on_disconnect = add_conn_callback(OnDisconnect, OldOnDisconnect)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{opts = Opts, supervisees = SupPids}) ->
    case lists:member(Pid, SupPids) of
        true ->
            case proplists:get_value(auto_reconnect, Opts, false) of
                false -> {stop, Reason, State};
                Secs -> reconnect(Secs, State)
            end;
        false ->
            logger:debug("~p received unexpected exit:~0p from ~p. Supervisees: ~p",
                         [?MODULE, Reason, Pid, SupPids]),
            {noreply, State}
    end;

handle_info(reconnect, State = #state{opts = Opts, on_reconnect = OnReconnect}) ->
     case connect_internal(State) of
         {ok, NewState = #state{client = Client}}  ->
             handle_reconnect(Client, OnReconnect),
             {noreply, NewState};
         {Err, _Reason} when Err =:= error orelse Err =:= 'EXIT'  ->
             reconnect(proplists:get_value(auto_reconnect, Opts), State)
     end;

handle_info(Info, State) ->
    logger:error("[PoolWorker] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id,
                          client = Client,
                          on_disconnect = Disconnect}) ->
    handle_disconnect(Client, Disconnect),
    gproc_pool:disconnect_worker(ecpool:name(Pool), {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

connect(#state{mod = Mod, opts = Opts, id = Id}) ->
    Mod:connect([{ecpool_worker_id, Id} | connopts(Opts, [])]).

connopts([], Acc) ->
    Acc;
connopts([{Key, _}|Opts], Acc)
  when Key =:= pool_size;
       Key =:= pool_type;
       Key =:= auto_reconnect;
       Key =:= on_reconnect ->
    connopts(Opts, Acc);

connopts([Opt|Opts], Acc) ->
    connopts(Opts, [Opt|Acc]).

reconnect(Secs, State = #state{client = Client, on_disconnect = Disconnect, supervisees = SubPids}) ->
    [erlang:unlink(P) || P <- SubPids, is_pid(P)],
    handle_disconnect(Client, Disconnect),
    erlang:send_after(timer:seconds(Secs), self(), reconnect),
    {noreply, State#state{client = undefined}}.

handle_reconnect(_, undefined) ->
    ok;
handle_reconnect(undefined, _) ->
    ok;
handle_reconnect(Client, OnReconnectList) when is_list(OnReconnectList) ->
    [safe_exec(OnReconnect, Client) || OnReconnect <- OnReconnectList];
handle_reconnect(Client, OnReconnect) ->
    safe_exec(OnReconnect, Client).

handle_disconnect(undefined, _) ->
    ok;
handle_disconnect(_, undefined) ->
    ok;
handle_disconnect(Client, Disconnect) ->
    safe_exec(Disconnect, Client).

connect_internal(State) ->
    try connect(State) of
        {ok, Client} when is_pid(Client) ->
            erlang:link(Client),
            {ok, State#state{client = Client, supervisees = [Client]}};
        {ok, Client, #{supervisees := SupPids} = _SupOpts} when is_list(SupPids) ->
            [erlang:link(P) || P <- SupPids],
            {ok, State#state{client = Client, supervisees = SupPids}};
        {error, Error} ->
            {error, Error}
    catch
        _C:Reason:ST -> {error, {Reason, ST}}
    end.

safe_exec(Action, MainArg) ->
    try exec(Action, MainArg)
    catch E:R:ST ->
        logger:error("[PoolWorker] safe_exec ~p, failed: ~0p", [Action, {E,R,ST}]),
        {error, {exec_failed, E, R}}
    end.

exec({M, F, A}, MainArg) ->
    erlang:apply(M, F, [MainArg]++A);
exec(Action, MainArg) when is_function(Action) ->
    Action(MainArg).

ensure_callback(undefined) -> undefined;
ensure_callback({_,_,_} = Callback) -> Callback.

add_conn_callback(OnReconnect, OldOnReconnects) when is_list(OldOnReconnects) ->
    [OnReconnect | OldOnReconnects];
add_conn_callback(OnReconnect, undefined) ->
    [OnReconnect];
add_conn_callback(OnReconnect, OldOnReconnect) ->
    [OnReconnect, OldOnReconnect].
