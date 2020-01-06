%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("types.hrl").

-export([ start_link/0
        , start_child/1
        , start_child/2
        , stop_child/1
        ]).

-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Mod, Type), #{id => Mod,
                            start => {Mod, start_link, []},
                            restart => permanent,
                            shutdown => 5000,
                            type => Type,
                            modules => [Mod]}).

-spec(start_link() -> startlink_ret()).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(ChildSpec) when is_map(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

start_child(Mod, Type) when is_atom(Mod) andalso is_atom(Type) ->
    supervisor:start_child(?MODULE, ?CHILD(Mod, Type)).

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
    {ok, {{one_for_one, 10, 100}, []}}.

