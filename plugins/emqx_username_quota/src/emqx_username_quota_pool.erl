%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_pool).

-behaviour(gen_server).

-export([start_link/2]).
-export([add/3]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-define(POOL, ?MODULE).

start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

add(Username, ClientId, Pid) ->
    Worker = gproc_pool:pick_worker(?POOL, {Username, ClientId}),
    _ = erlang:send(Worker, {add, Username, ClientId, Pid}),
    ok.

init([Pool, Id]) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({add, Username, ClientId, Pid}, State) ->
    case emqx_username_quota_state:add(Username, ClientId, Pid) of
        new -> _ = erlang:monitor(process, Pid);
        existing -> ok
    end,
    {noreply, State};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    ok = emqx_username_quota_state:del(Pid),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).
