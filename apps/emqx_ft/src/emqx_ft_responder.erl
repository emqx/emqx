%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start_link/0]).

-export([
    register/3,
    unregister/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).

-type key() :: term().

%%--------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register(Key, DefaultAction, Timeout) -> ok | {error, already_registered} when
    Key :: key(),
    DefaultAction :: fun((Key) -> any()),
    Timeout :: timeout().
register(Key, DefaultAction, Timeout) ->
    case ets:lookup(?TAB, Key) of
        [] ->
            gen_server:call(?SERVER, {register, Key, DefaultAction, Timeout});
        [{Key, _Action, _Ref}] ->
            {error, already_registered}
    end.

-spec unregister(Key) -> ok | {error, not_found} when
    Key :: key().
unregister(Key) ->
    gen_server:call(?SERVER, {unregister, Key}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------

init([]) ->
    _ = ets:new(?TAB, [named_table, protected, set, {read_concurrency, true}]),
    {ok, #{}}.

handle_call({register, Key, DefaultAction, Timeout}, _From, State) ->
    ?SLOG(warning, #{msg => "register", key => Key, timeout => Timeout}),
    case ets:lookup(?TAB, Key) of
        [] ->
            TRef = erlang:start_timer(Timeout, self(), {timeout, Key}),
            true = ets:insert(?TAB, {Key, DefaultAction, TRef}),
            {reply, ok, State};
        [{_, _Action, _Ref}] ->
            {reply, {error, already_registered}, State}
    end;
handle_call({unregister, Key}, _From, State) ->
    ?SLOG(warning, #{msg => "unregister", key => Key}),
    case ets:lookup(?TAB, Key) of
        [] ->
            {reply, {error, not_found}, State};
        [{_, _Action, TRef}] ->
            _ = erlang:cancel_timer(TRef),
            true = ets:delete(?TAB, Key),
            {reply, ok, State}
    end.

handle_cast(Msg, State) ->
    ?SLOG(warning, #{msg => "unknown cast", cast_msg => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, {timeout, Key}}, State) ->
    case ets:lookup(?TAB, Key) of
        [] ->
            {noreply, State};
        [{_, Action, TRef}] ->
            _ = erlang:cancel_timer(TRef),
            true = ets:delete(?TAB, Key),
            %% TODO: safe apply
            _ = Action(Key),
            {noreply, State}
    end;
handle_info(Msg, State) ->
    ?SLOG(warning, #{msg => "unknown message", info_msg => Msg}),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
