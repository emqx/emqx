%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_plugins_monitor).
-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ get_plugins/0
        , start_link/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{ref => next_check_time(), failed => 0}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({timeout, Ref, check}, State = #{failed := Failed}) ->
    erlang:cancel_timer(Ref),
    NewFailed = maybe_alarm(check(), Failed),
    {noreply, State#{ref => next_check_time(), failed => NewFailed}};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

next_check_time() ->
    Check = emqx_plugins:get_config(check_interval, 5000),
    emqx_misc:start_timer(Check, check).

check() ->
    Nodes = mria_mnesia:running_nodes(),
    case rpc:multicall(Nodes, ?MODULE, get_plugins_list, [], 15000) of
        {Plugins, []} -> check_plugins(Plugins);
        {_ , BadNodes} -> {error, io_lib:format("~p rpc to ~p failed", [node(), BadNodes])}
    end.

get_plugins() ->
    {node(), emqx_plugins:list()}.

check_plugins(Plugins) ->
    check_status(Plugins),
    ok.

check_status(_Plugins) ->
    ok.

%% alarm when failed 3 time.
maybe_alarm({error, _Reason}, Failed) when Failed >= 2 ->
    %alarm(Reason),
    0;
maybe_alarm({error, _Reason}, Failed) -> Failed + 1;
maybe_alarm(ok, _Failed) -> 0.
