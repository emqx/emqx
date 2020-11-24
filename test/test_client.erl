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

-module(test_client).

-behaviour(ecpool_worker).

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([ connect/1
        , plus/3
        , callback/2
        , stop/2
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

connect(Opts) ->
    case proplists:get_value(multiprocess, Opts, false) of
        true ->
            {ok, Pid1} = gen_server:start_link(?MODULE, [Opts], []),
            {ok, Pid2} = gen_server:start_link(?MODULE, [Opts], []),
            {ok, {Pid1, Pid2}, #{supervisees => [Pid1, Pid2]}};
        false ->
            gen_server:start_link(?MODULE, [Opts], [])
    end.

plus(Pid, L, R) ->
    gen_server:call(Pid, {plus, L, R}).

callback(Result, SendTo) ->
    SendTo ! {result, Result}.

stop(Pid, Reason) ->
    gen_server:call(Pid, {stop, Reason}).

%%-----------------------------------------------------------------------------
%% gen_server Function Definitions
%%-----------------------------------------------------------------------------

init(Args) ->
    {ok, Args}.

handle_call({stop, Reason}, _From, State) ->
    {stop, Reason, ok, State};

handle_call({plus, L, R}, _From, State) ->
    {reply, L + R, State};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

