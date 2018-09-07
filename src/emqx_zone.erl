%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_zone).

-behaviour(gen_server).

-include("emqx.hrl").

-export([start_link/0]).
-export([get_env/2, get_env/3]).
-export([set_env/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(get_env(emqx_types:zone() | undefined, atom()) -> undefined | term()).
get_env(undefined, Key) ->
    emqx_config:get_env(Key);
get_env(Zone, Key) ->
    get_env(Zone, Key, undefined).

-spec(get_env(emqx_types:zone() | undefined, atom(), term()) -> undefined | term()).
get_env(undefined, Key, Def) ->
    emqx_config:get_env(Key, Def);
get_env(Zone, Key, Def) ->
    try ets:lookup_element(?TAB, {Zone, Key}, 2)
    catch error:badarg ->
        emqx_config:get_env(Key, Def)
    end.

-spec(set_env(emqx_types:zone(), atom(), term()) -> ok).
set_env(Zone, Key, Val) ->
    gen_server:cast(?MODULE, {set_env, Zone, Key, Val}).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:new(?TAB, [set, {read_concurrency, true}]),
    {ok, element(2, handle_info(reload, #{timer => undefined}))}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Zone] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({set_env, Zone, Key, Val}, State) ->
    true = ets:insert(?TAB, {{Zone, Key}, Val}),
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_logger:error("[Zone] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, reload}, State = #{timer := TRef}) ->
    [ets:insert(?TAB, [{{Zone, Key}, Val} || {Key, Val} <- Opts])
     || {Zone, Opts} <- emqx_config:get_env(zones, [])],
    {noreply, ensure_reload_timer(State), hibernate};

handle_info(Info, State) ->
    emqx_logger:error("[Zone] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

ensure_reload_timer(State) ->
    State#{timer := emqx_misc:start_timer(timer:minutes(5), reload)}.

