%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_responder).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([start/3]).
-export([kickoff/2]).
-export([ack/2]).

%% Supervisor API
-export([start_link/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(REF(Key), {via, gproc, {n, l, {?MODULE, Key}}}).

-type key() :: term().
-type respfun() :: fun(({ack, _Result} | {down, _Result} | timeout) -> _SideEffect).

%%--------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

-spec start(key(), respfun(), timeout()) -> startlink_ret().
start(Key, RespFun, Timeout) ->
    emqx_ft_responder_sup:start_child(Key, RespFun, Timeout).

-spec kickoff(key(), pid()) -> ok.
kickoff(Key, Pid) ->
    gen_server:call(?REF(Key), {kickoff, Pid}).

-spec ack(key(), _Result) -> _Return.
ack(Key, Result) ->
    % TODO: it's possible to avoid term copy
    gen_server:call(?REF(Key), {ack, Result}, infinity).

-spec start_link(key(), timeout(), respfun()) -> startlink_ret().
start_link(Key, RespFun, Timeout) ->
    gen_server:start_link(?REF(Key), ?MODULE, {Key, RespFun, Timeout}, []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init({Key, RespFun, Timeout}) ->
    _ = erlang:process_flag(trap_exit, true),
    _TRef = erlang:send_after(Timeout, self(), timeout),
    {ok, {Key, RespFun}}.

handle_call({kickoff, Pid}, _From, St) ->
    % TODO: more state?
    _MRef = erlang:monitor(process, Pid),
    _ = Pid ! kickoff,
    {reply, ok, St};
handle_call({ack, Result}, _From, {Key, RespFun}) ->
    Ret = apply(RespFun, [{ack, Result}]),
    ?tp(debug, ft_responder_ack, #{key => Key, result => Result, return => Ret}),
    {stop, {shutdown, Ret}, Ret, undefined};
handle_call(Msg, _From, State) ->
    ?SLOG(warning, #{msg => "unknown_call", call_msg => Msg}),
    {reply, {error, unknown_call}, State}.

handle_cast(Msg, State) ->
    ?SLOG(warning, #{msg => "unknown_cast", cast_msg => Msg}),
    {noreply, State}.

handle_info(timeout, {Key, RespFun}) ->
    Ret = apply(RespFun, [timeout]),
    ?tp(debug, ft_responder_timeout, #{key => Key, return => Ret}),
    {stop, {shutdown, Ret}, undefined};
handle_info({'DOWN', _MRef, process, _Pid, Reason}, {Key, RespFun}) ->
    Ret = apply(RespFun, [{down, map_down_reason(Reason)}]),
    ?tp(debug, ft_responder_procdown, #{key => Key, reason => Reason, return => Ret}),
    {stop, {shutdown, Ret}, undefined};
handle_info(Msg, State) ->
    ?SLOG(warning, #{msg => "unknown_message", info_msg => Msg}),
    {noreply, State}.

terminate(_Reason, undefined) ->
    ok;
terminate(Reason, {Key, RespFun}) ->
    Ret = apply(RespFun, [timeout]),
    ?tp(debug, ft_responder_shutdown, #{key => Key, reason => Reason, return => Ret}),
    ok.

map_down_reason(normal) ->
    ok;
map_down_reason(shutdown) ->
    ok;
map_down_reason({shutdown, Result}) ->
    Result;
map_down_reason(noproc) ->
    {error, noproc};
map_down_reason(Error) ->
    {error, {internal_error, Error}}.
