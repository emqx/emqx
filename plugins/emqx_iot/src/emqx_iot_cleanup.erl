%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_iot_cleanup).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_info/2, handle_call/3, handle_cast/2]).

-include("emqx_iot.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Interval = cleanup_interval(),
    schedule(Interval),
    {ok, Interval}.

handle_info(cleanup, Interval) ->
    ok = emqx_iot_storage:cleanup_expired(),
    schedule(Interval),
    {noreply, Interval};
handle_info(_Msg, State) ->
    {noreply, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

schedule(Interval) ->
    erlang:send_after(Interval, self(), cleanup).

cleanup_interval() ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(cleanup_interval, Config, 60) * 1000.
