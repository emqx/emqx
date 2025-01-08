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

%% @doc This supervisor manages the global worker processes needed for
%% the functioning of builtin local databases, and all builtin local
%% databases that attach to it.
-module(emqx_ds_builtin_local_sup).

-behaviour(supervisor).

%% API:
-export([start_db/2, stop_db/1]).

%% behavior callbacks:
-export([init/1]).

%% internal exports:
-export([start_top/0, start_databases_sup/0]).

-export_type([]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(top, ?MODULE).
-define(databases, emqx_ds_builtin_local_db_sup).

%%================================================================================
%% API functions
%%================================================================================

-spec start_top() -> {ok, pid()}.
start_top() ->
    supervisor:start_link({local, ?top}, ?MODULE, ?top).

-spec start_db(emqx_ds:db(), emqx_ds_builtin_local:db_opts()) ->
    supervisor:startchild_ret().
start_db(DB, Opts) ->
    ChildSpec = #{
        id => DB,
        start => {?databases, start_db, [DB, Opts]},
        type => supervisor,
        shutdown => infinity
    },
    supervisor:start_child(?databases, ChildSpec).

-spec stop_db(emqx_ds:db()) -> ok.
stop_db(DB) ->
    case whereis(?databases) of
        Pid when is_pid(Pid) ->
            _ = supervisor:terminate_child(?databases, DB),
            _ = supervisor:delete_child(?databases, DB),
            ok;
        undefined ->
            ok
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

%% There are two layers of supervision:
%%
%% 1. top supervisor for the builtin backend. It contains the global
%% worker processes (like the metadata server), and `?databases'
%% supervisior.
%%
%% 2. `?databases': a `one_for_one' supervisor where each child is a
%% `db' supervisor that contains processes that represent the DB.
%% Chidren are attached dynamically to this one.
init(?top) ->
    %% Children:
    MetadataServer = #{
        id => metadata_server,
        start => {emqx_ds_builtin_local_meta, start_link, []},
        restart => permanent,
        type => worker,
        shutdown => 5000
    },
    DBsSup = #{
        id => ?databases,
        start => {?MODULE, start_databases_sup, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity
    },
    %%
    SupFlags = #{
        strategy => one_for_all,
        intensity => 1,
        period => 1,
        auto_shutdown => never
    },
    {ok, {SupFlags, [MetadataServer, DBsSup]}};
init(?databases) ->
    %% Children are added dynamically:
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, []}}.

%%================================================================================
%% Internal exports
%%================================================================================

start_databases_sup() ->
    supervisor:start_link({local, ?databases}, ?MODULE, ?databases).

%%================================================================================
%% Internal functions
%%================================================================================
