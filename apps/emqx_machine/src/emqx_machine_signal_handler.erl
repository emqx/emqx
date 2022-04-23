%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This module implements a gen_event handler which
%% swap-in replaces the default one from OTP.
%% The kill signal (sigterm) is captured so we can
%% perform graceful shutdown.
-module(emqx_machine_signal_handler).

-export([
    start/0,
    init/1,
    format_status/2,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include_lib("emqx/include/logger.hrl").

start() ->
    ok = gen_event:swap_sup_handler(
        erl_signal_server,
        {erl_signal_handler, []},
        {?MODULE, []}
    ).

init({[], _}) -> {ok, #{}}.

handle_event(sigterm, State) ->
    ?ULOG("Received terminate signal, shutting down now~n", []),
    emqx_machine_terminator:graceful(),
    {ok, State};
handle_event(Event, State) ->
    %% delegate other events back to erl_signal_handler
    %% erl_signal_handler does not make use of the State
    %% so we can pass whatever from here
    _ = erl_signal_handler:handle_event(Event, State),
    {ok, State}.

handle_info(stop, State) ->
    {ok, State};
handle_info(_Other, State) ->
    {ok, State}.

handle_call(_Request, State) ->
    {ok, ok, State}.

format_status(_Opt, [_Pdict, _S]) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Args, _State) ->
    ok.
