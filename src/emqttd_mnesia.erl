%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% TODO: refactor this module
%% @doc emqttd mnesia
%% @author Feng Lee <feng@emqtt.io>
-module(emqttd_mnesia).

-include("emqttd.hrl").

-export([start/0, cluster/1]).

-export([create_table/2, copy_table/1]). 

-export([dump/3]).

start() ->
    case init_schema() of
        ok ->
            ok;
        {error, {_Node, {already_exists, _Node}}} ->
            ok;
        {error, Reason} -> 
            lager:error("mnesia init_schema error: ~p", [Reason])
    end,
    ok = mnesia:start(),
    init_tables(),
    wait_for_tables().

%% @private
%% @doc Init mnesia schema.
init_schema() ->
    case mnesia:system_info(extra_db_nodes) of
        [] ->
            %% create schema
            mnesia:create_schema([node()]);
        __ ->
            ok
    end.

%% @private
%% @doc Init mnesia tables.
init_tables() ->
    case mnesia:system_info(extra_db_nodes) of
        [] ->
            create_tables();
        _ ->
            copy_tables()
    end.

%% @private
%% @doc create tables.
create_tables() ->
    emqttd_util:apply_module_attributes(boot_mnesia).

create_table(Table, Attrs) ->
    case mnesia:create_table(Table, Attrs) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} -> ok;
        {aborted, {already_exists, Table, _}} -> ok;
        Error -> Error
    end.

%% @private
%% @doc copy tables.
copy_tables() ->
    emqttd_util:apply_module_attributes(copy_mnesia).

copy_table(Table) ->
    case mnesia:add_table_copy(Table, node(), ram_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists, Table}} -> ok;
        {aborted, {already_exists, Table, _Node}} -> ok;
        {aborted, Error} -> Error
    end.

%% @private
%% @doc wait for tables.
wait_for_tables() ->
    %% io:format("mnesia wait_for_tables: ~p~n", [mnesia:system_info(local_tables)]),
    mnesia:wait_for_tables(mnesia:system_info(local_tables), infinity).

%% TODO: should move to cluster.
%% @private
%% @doc Simple cluster with another nodes.
cluster(Node) ->
    %% stop mnesia 
    mnesia:stop(),
    ok = wait_for_mnesia(stop),
    %% delete mnesia
    ok = mnesia:delete_schema([node()]),
    %% start mnesia
    ok = mnesia:start(),
    %% connect with extra_db_nodes
    case mnesia:change_config(extra_db_nodes, [Node]) of
        {ok, []} -> 
            throw({error, failed_to_connect_extra_db_nodes});
        {ok, Nodes} ->
            case lists:member(Node, Nodes) of
                true ->  lager:info("mnesia connected to extra_db_node '~s' successfully!", [Node]);
                false -> lager:error("mnesia failed to connect extra_db_node '~s'!", [Node])
            end,
            mnesia:change_table_copy_type(schema, node(), disc_copies)
    end,
    copy_tables(),
    wait_for_tables().
 
wait_for_mnesia(stop) ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            lager:info("Waiting for mnesia to stop..."),
            timer:sleep(1000),
            wait_for_mnesia(stop);
        yes ->
            {error, mnesia_unexpectedly_running};
        starting ->
            {error, mnesia_unexpectedly_starting}
    end.

dump(ets, Table, Fun) ->
    dump(ets, Table, ets:first(Table), Fun).

dump(ets, _Table, '$end_of_table', _Fun) ->
    ok;

dump(ets, Table, Key, Fun) ->
    case ets:lookup(Table, Key) of
        [Record] -> Fun(Record);
        [] -> ignore
    end,
    dump(ets, Table, ets:next(Table, Key), Fun).

