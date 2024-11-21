%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt).

-behaviour(gen_server).

-export([start_link/0, stop/1, call/2, cast/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {extra = #{}}).

-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop).

-spec call(pid(), term()) -> term().
call(Pid, Request) ->
    gen_server:call(Pid, {custom_call, Request}).

-spec cast(pid(), term()) -> ok.
cast(Pid, Request) ->
    gen_server:cast(Pid, {custom_cast, Request}).

-spec init(term()) -> {ok, state()}.
init(_Args) ->
    {ok, #state{extra = #{}}}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call({_Call, _Request}, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({custom_cast, _Request}, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
