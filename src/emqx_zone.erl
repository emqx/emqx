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
-include("types.hrl").

%% APIs
-export([start_link/0]).

-export([ get_env/2
        , get_env/3
        , set_env/3
        , force_reload/0
        ]).

%% for test
-export([stop/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% dummy state
-record(state, {}).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(KEY(Zone, Key), {?MODULE, Zone, Key}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(get_env(maybe(emqx_types:zone()), atom()) -> maybe(term())).
get_env(undefined, Key) ->
    emqx_config:get_env(Key);
get_env(Zone, Key) ->
    get_env(Zone, Key, undefined).

-spec(get_env(maybe(emqx_types:zone()), atom(), term()) -> maybe(term())).
get_env(undefined, Key, Def) ->
    emqx_config:get_env(Key, Def);
get_env(Zone, Key, Def) ->
    try persistent_term:get(?KEY(Zone, Key))
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
    _ = do_reload(),
    {ok, #state{}}.

handle_call(force_reload, _From, State) ->
    _ = do_reload(),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "[Zone] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({set_env, Zone, Key, Val}, State) ->
    ok = persistent_term:put(?KEY(Zone, Key), Val),
    {noreply, State};

handle_cast(Msg, State) ->
    ?LOG(error, "[Zone] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "[Zone] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

do_reload() ->
    [ persistent_term:put(?KEY(Zone, Key), Val)
      || {Zone, Opts} <- emqx_config:get_env(zones, []), {Key, Val} <- Opts ].

