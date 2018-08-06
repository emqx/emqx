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

-export([start_link/0]).
-export([get_env/2, get_env/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {timer}).
-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_env(Zone, Par) ->
    get_env(Zone, Par, undefined).

get_env(Zone, Par, Def) ->
    try ets:lookup_element(?TAB, {Zone, Par}, 2) catch error:badarg -> Def end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    _ = emqx_tables:new(?TAB, [set, {read_concurrency, true}]),
    {ok, element(2, handle_info(reload, #state{}))}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[Zone] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[Zone] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(reload, State) ->
    lists:foreach(
      fun({Zone, Options}) ->
          [ets:insert(?TAB, {{Zone, Par}, Val}) || {Par, Val} <- Options]
      end, emqx_config:get_env(zones, [])),
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
    State#state{timer = erlang:send_after(5000, self(), reload)}.

