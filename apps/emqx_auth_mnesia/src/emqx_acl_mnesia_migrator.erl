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

-module(emqx_acl_mnesia_migrator).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_statem).

-define(CHECK_ALL_NODES_INTERVAL, 60000).

-type(migration_delay_reason() :: old_nodes | bad_nodes).

-export([
    callback_mode/0,
    init/1
]).

-export([
    waiting_all_nodes/3,
    checking_old_table/3,
    migrating/3
]).

-export([
    start_link/0,
    start_link/1,
    start_supervised/0,
    stop_supervised/0,
    migrate_records/0,
    is_migrating_on_node/1,
    is_old_table_migrated/0
]).

%%--------------------------------------------------------------------
%% External interface
%%--------------------------------------------------------------------

start_link() ->
    start_link(?MODULE).

start_link(Name) when is_atom(Name) ->
    start_link(#{
        name => Name
    });

start_link(#{name := Name} = Opts) ->
    gen_statem:start_link({local, Name}, ?MODULE, Opts, []).

start_supervised() ->
    try
        {ok, _} = supervisor:restart_child(emqx_auth_mnesia_sup, ?MODULE),
        ok
    catch
        exit:{noproc, _} -> ok
    end.

stop_supervised() ->
    try
        ok = supervisor:terminate_child(emqx_auth_mnesia_sup, ?MODULE),
        ok = supervisor:delete_child(emqx_auth_mnesia_sup, ?MODULE)
    catch
        exit:{noproc, _} -> ok
    end.

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> state_functions.

init(Opts) ->
    ok = emqx_acl_mnesia_db:create_table(),
    ok = emqx_acl_mnesia_db:create_table2(),
    Name = maps:get(name, Opts, ?MODULE),
    CheckNodesInterval = maps:get(check_nodes_interval, Opts, ?CHECK_ALL_NODES_INTERVAL),
    GetNodes = maps:get(get_nodes, Opts, fun all_nodes/0),
    Data =
        #{name => Name,
          check_nodes_interval => CheckNodesInterval,
          get_nodes => GetNodes},
    {ok, waiting_all_nodes, Data, [{state_timeout, 0, check_nodes}]}.

%%--------------------------------------------------------------------
%% state callbacks
%%--------------------------------------------------------------------

waiting_all_nodes(state_timeout, check_nodes, Data) ->
    #{name := Name, check_nodes_interval := CheckNodesInterval, get_nodes := GetNodes} = Data,
    case is_all_nodes_migrating(Name, GetNodes()) of
        true ->
            ?tp(info, emqx_acl_mnesia_migrator_check_old_table, #{}),
            {next_state, checking_old_table, Data, [{next_event, internal, check_old_table}]};
        {false, Reason, Nodes} ->
            ?tp(info,
                emqx_acl_mnesia_migrator_bad_nodes_delay,
                #{delay => CheckNodesInterval,
                  reason => Reason,
                  name => Name,
                  nodes => Nodes}),
            {keep_state_and_data, [{state_timeout, CheckNodesInterval, check_nodes}]}
    end.

checking_old_table(internal, check_old_table, Data) ->
    case is_old_table_migrated() of
        true ->
            ?tp(info, emqx_acl_mnesia_migrator_finish, #{}),
            {next_state, finished, Data, [{hibernate, true}]};
        false ->
            ?tp(info, emqx_acl_mnesia_migrator_start_migration, #{}),
            {next_state, migrating, Data, [{next_event, internal, start_migration}]}
    end.

migrating(internal, start_migration, Data) ->
    ok = migrate_records(),
    {next_state, checking_old_table, Data, [{next_event, internal, check_old_table}]}.

%% @doc Returns `true` if migration is started in the local node, otherwise crash.
-spec(is_migrating_on_node(atom()) -> true).
is_migrating_on_node(Name) ->
    true = is_pid(erlang:whereis(Name)).

%% @doc Run migration of records
-spec(migrate_records() -> ok).
migrate_records() ->
    ok = add_migration_mark(),
    Key = peek_record(),
    do_migrate_records(Key).

%% @doc Run migration of records
-spec(is_all_nodes_migrating(atom(), list(node())) -> true | {false, migration_delay_reason(), list(node())}).
is_all_nodes_migrating(Name, Nodes) ->
    case rpc:multicall(Nodes, ?MODULE, is_migrating_on_node, [Name]) of
        {Results, []} ->
            OldNodes = [ Node || {Node, Result} <- lists:zip(Nodes, Results), Result =/= true ],
            case OldNodes of
                [] -> true;
                _ -> {false, old_nodes, OldNodes}
            end;
        {_, [_BadNode | _] = BadNodes} ->
            {false, bad_nodes, BadNodes}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

all_nodes() ->
    ekka_mnesia:cluster_nodes(all).

is_old_table_migrated() ->
    Result =
        mnesia:transaction(fun() ->
                              case mnesia:first(?ACL_TABLE) of
                                  ?MIGRATION_MARK_KEY ->
                                      case mnesia:next(?ACL_TABLE, ?MIGRATION_MARK_KEY) of
                                          '$end_of_table' -> true;
                                          _OtherKey -> false
                                      end;
                                  '$end_of_table' -> false;
                                  _OtherKey -> false
                              end
                           end),
    case Result of
        {atomic, true} ->
            true;
        _ ->
            false
    end.

add_migration_mark() ->
    {atomic, ok} = mnesia:transaction(fun() -> mnesia:write(?MIGRATION_MARK_RECORD) end),
    ok.

peek_record() ->
    Key = mnesia:dirty_first(?ACL_TABLE),
    case Key of
        ?MIGRATION_MARK_KEY ->
            mnesia:dirty_next(?ACL_TABLE, Key);
        _ -> Key
    end.

do_migrate_records('$end_of_table') -> ok;
do_migrate_records({_Login, _Topic} = Key) ->
    ?tp(emqx_acl_mnesia_migrator_record_selected, #{key => Key}),
    _ = mnesia:transaction(fun migrate_one_record/1, [Key]),
    do_migrate_records(peek_record()).

migrate_one_record({Login, _Topic} = Key) ->
    case mnesia:wread({?ACL_TABLE, Key}) of
        [] ->
            ?tp(emqx_acl_mnesia_migrator_record_missed, #{key => Key}),
            record_missing;
        OldRecs ->
            Acls = mnesia:wread({?ACL_TABLE2, Login}),
            UpdatedAcl = emqx_acl_mnesia_db:merge_acl_records(Login, OldRecs, Acls),
            ok = mnesia:write(UpdatedAcl),
            ok = mnesia:delete({?ACL_TABLE, Key}),
            ?tp(emqx_acl_mnesia_migrator_record_migrated, #{key => Key})
    end.
