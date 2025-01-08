%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_pool).

-behaviour(gen_server).

-include("emqx_mt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([start_link/2]).

-export([add/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(POOL, ?MODULE).

%% @doc Start pool.
-spec start_link(atom(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_utils:proc_name(Pool, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

add(Tns, ClientId, Pid) ->
    Worker = pick(Tns, ClientId),
    _ = erlang:send(Worker, {add, Tns, ClientId, Pid}),
    ok.

pick(Tns, ClientId) ->
    gproc_pool:pick_worker(?POOL, {Tns, ClientId}).

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call(Req, _From, State) ->
    ?LOG(error, #{msg => "unexpected_call", server => ?MODULE, call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, #{msg => "unexpected_cast", server => ?MODULE, cast => Msg}),
    {noreply, State}.

handle_info({add, Tns, ClientId, Pid}, State) ->
    _ = erlang:monitor(process, Pid),
    ok = emqx_mt_state:add(Tns, ClientId, Pid),
    {noreply, State};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    ok = emqx_mt_state:del(Pid),
    {noreply, State};
handle_info(Info, State) ->
    ?LOG(error, #{msg => "unexpected_info", server => ?MODULE, info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
