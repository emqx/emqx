%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("logger.hrl").

-export([start_link/0]).
-export([get_env/2, get_env/3]).
-export([set_env/3]).
-export([force_reload/0]).
%% for test
-export([stop/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
    gen_server:cast(?SERVER, {set_env, Zone, Key, Val}).

-spec(force_reload() -> ok).
force_reload() ->
    gen_server:call(?SERVER, force_reload).

-spec(stop() -> ok).
stop() ->
    gen_server:stop(?SERVER, normal, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(?TAB, [set, {read_concurrency, true}]),
    {ok, element(2, handle_info(reload, #{timer => undefined}))}.

handle_call(force_reload, _From, State) ->
    _ = do_reload(),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?ERROR("[Zone] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({set_env, Zone, Key, Val}, State) ->
    true = ets:insert(?TAB, {{Zone, Key}, Val}),
    {noreply, State};

handle_cast(Msg, State) ->
    ?ERROR("[Zone] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(reload, State) ->
    _ = do_reload(),
    {noreply, ensure_reload_timer(State#{timer := undefined}), hibernate};

handle_info(Info, State) ->
    ?ERROR("[Zone] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_reload() ->
    [ets:insert(?TAB, [{{Zone, Key}, Val} || {Key, Val} <- Opts])
     || {Zone, Opts} <- emqx_config:get_env(zones, [])].

ensure_reload_timer(State = #{timer := undefined}) ->
    State#{timer := erlang:send_after(timer:minutes(5), self(), reload)};
ensure_reload_timer(State) ->
    State.

