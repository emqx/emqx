%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_sup).

-behaviour(supervisor).

-include_lib("emqx/include/types.hrl").

-export([ start_link/0
        , start_child/1
        , start_child/2
        , start_child/3
        , stop_child/1
        ]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, Type, Args),
        #{id => Mod,
          start => {Mod, start_link, Args},
          restart => permanent,
          shutdown => 5000,
          type => Type,
          modules => [Mod]}).

-define(CHILD(MOD, Type), ?CHILD(MOD, Type, [])).

-spec(start_link() -> startlink_ret()).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_child(supervisor:child_spec()) -> ok.
start_child(ChildSpec) when is_map(ChildSpec) ->
    assert_started(supervisor:start_child(?MODULE, ChildSpec)).

-spec start_child(atom(), atom()) -> ok.
start_child(Mod, Type) when is_atom(Mod) andalso is_atom(Type) ->
    assert_started(supervisor:start_child(?MODULE, ?CHILD(Mod, Type))).

-spec start_child(atom(), atom(), list(any())) -> ok.
start_child(Mod, Type, Args) when is_atom(Mod) andalso is_atom(Type) ->
    assert_started(supervisor:start_child(?MODULE, ?CHILD(Mod, Type, Args))).

-spec(stop_child(any()) -> ok | {error, term()}).
stop_child(ChildId) ->
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok -> supervisor:delete_child(?MODULE, ChildId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = emqx_tables:new(emqx_modules, [set, public, {write_concurrency, true}]),
    emqx_slow_subs:init_tab(),
    {ok, {{one_for_one, 10, 100}, []}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

assert_started({ok, _Pid}) -> ok;
assert_started({ok, _Pid, _Info}) -> ok;
assert_started({error, {already_started, _Pid}}) -> ok;
assert_started({error, Reason}) -> erlang:error(Reason).
