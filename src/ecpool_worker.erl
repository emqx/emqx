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

-behaviour(gen_server).

-export([start_link/4]).

%% API Function Exports
-export([client/1, is_connected/1, set_reconnect_callback/2]).

%% gen_server Function Exports
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {pool, id, client, mod, on_reconnect, on_disconnect, supervisees = [], opts}).

%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------

-ifdef(use_specs).

-callback(connect(ConnOpts :: list())
          -> {ok, pid()} | {error, Reason :: term()}).

-else.

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{connect, 1}];
behaviour_info(_Other) ->
    undefined.

-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a pool worker.
-spec(start_link(atom(), pos_integer(), module(), list()) ->
      {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Mod, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Id, Mod, Opts], []).

%% @doc Get client/connection.
-spec(client(pid()) -> undefined | pid()).
client(Pid) ->
    gen_server:call(Pid, client, infinity).

%% @doc Is client connected?
-spec(is_connected(pid()) -> boolean()).
is_connected(Pid) ->
    gen_server:call(Pid, is_connected, infinity).

-spec(set_reconnect_callback(pid(), ecpool:reconn_callback()) -> ok).
set_reconnect_callback(Pid, OnReconnect) ->
    gen_server:cast(Pid, {set_reconn_callbk, OnReconnect}).

-spec(set_reconnect_callback(pid(), ecpool:reconn_callback()) -> ok).
set_reconnect_callback(Pid, OnReconnect) ->
    gen_server:cast(Pid, {set_reconn_callbk, OnReconnect}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Mod, Opts]) ->
    process_flag(trap_exit, true),
    State = #state{pool = Pool, id = Id, mod = Mod, opts = Opts,
                   on_reconnect = proplists:get_value(on_reconnect, Opts),
                   on_disconnect = proplists:get_value(on_disconnect, Opts)},
    case connect_internal(State) of
        {ok, NewState}  ->
            gproc_pool:connect_worker(ecpool:name(Pool), {Pool, Id}),
            {ok, NewState};
        {error, Error} ->
            {stop, Error}
    end.

handle_call(is_connected, _From, State = #state{client = Client}) ->
    IsAlive = Client =/= undefined andalso is_process_alive(Client),
    {reply, IsAlive, State};

handle_call(is_connected, _From, State = #state{client = Client}) ->
    {reply, Client =/= undefined, State};

handle_call(client, _From, State = #state{client = undefined}) ->
    {reply, {error, disconnected}, State};

handle_call(client, _From, State = #state{client = Client}) ->
    {reply, {ok, Client}, State};

handle_call(Req, _From, State) ->
    logger:error("[PoolWorker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({set_reconn_callbk, OnReconnect}, State) ->
    {noreply, State#state{on_reconnect = OnReconnect}};

handle_cast({set_reconn_callbk, OnReconnect}, State) ->
    {noreply, State#state{on_reconnect = OnReconnect}};

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
connopts([{pool_size, _}|Opts], Acc) ->
    connopts(Opts, Acc);
connopts([{pool_type, _}|Opts], Acc) ->
    connopts(Opts, Acc);
connopts([{auto_reconnect, _}|Opts], Acc) ->
    connopts(Opts, Acc);
connopts([{bind, _}|Opts], Acc) ->
    connopts(Opts, Acc);
connopts([{unbind, _}|Opts], Acc) ->
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
handle_reconnect(Client, OnReconnect) ->
    OnReconnect(Client).

handle_disconnect(undefined, _) ->
    ok;
handle_disconnect(_, undefined) ->
    ok;
handle_disconnect(Client, Disconnect) ->
    Disconnect(Client).

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
        _C:Reason -> {error, Reason}
    end.
