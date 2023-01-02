%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_jwt_sup).

-behaviour(supervisor).

-export([ start_link/0
        , ensure_worker_present/2
        , ensure_worker_deleted/1
        ]).

-export([init/1]).

-include_lib("emqx_rule_engine/include/rule_actions.hrl").

-type worker_id() :: term().

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ensure_jwt_table(),
    SupFlags = #{ strategy => one_for_one
                , intensity => 10
                , period => 5
                , auto_shutdown => never
                },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

%% @doc Starts a new JWT worker.  The caller should use
%% `emqx_rule_engine_jwt_sup:ensure_jwt/1' to ensure that a JWT has
%% been stored, if synchronization is needed.
-spec ensure_worker_present(worker_id(), map()) ->
          {ok, supervisor:child()}.
ensure_worker_present(Id, Config) ->
    ChildSpec = jwt_worker_child_spec(Id, Config),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, already_present} ->
            supervisor:restart_child(?MODULE, Id)
    end.

%% @doc Stops a given JWT worker by its id.
-spec ensure_worker_deleted(worker_id()) -> ok.
ensure_worker_deleted(Id) ->
    case supervisor:terminate_child(?MODULE, Id) of
        ok ->
            ok = supervisor:delete_child(?MODULE, Id);
        {error, not_found} ->
            ok
    end.

jwt_worker_child_spec(Id, Config) ->
    #{ id => Id
     , start => {emqx_rule_engine_jwt_worker, start_link, [Config]}
     , restart => transient
     , type => worker
     , significant => false
     , shutdown => 5_000
     , modules => [emqx_rule_engine_jwt_worker]
     }.

-spec ensure_jwt_table() -> ok.
ensure_jwt_table() ->
    case ets:whereis(?JWT_TABLE) of
        undefined ->
            Opts = [named_table, public,
                    {read_concurrency, true}, ordered_set],
            _ = ets:new(?JWT_TABLE, Opts),
            ok;
        _ ->
            ok
    end.
