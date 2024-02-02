%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_replication_layer_shard).

-export([start_link/2]).
-export([shard_servers/2]).

-export([
    servers/3,
    server/3
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    terminate/2
]).

-define(PTERM(DB, SHARD, L), {?MODULE, DB, SHARD, L}).
-define(MEMOIZE(DB, SHARD, EXPR),
    case persistent_term:get(__X_Key = ?PTERM(DB, SHARD, ?LINE), undefined) of
        undefined ->
            ok = persistent_term:put(__X_Key, __X_Value = (EXPR)),
            __X_Value;
        __X_Value ->
            __X_Value
    end
).

%%

start_link(DB, Shard) ->
    gen_server:start_link(?MODULE, {DB, Shard}, []).

shard_servers(DB, Shard) ->
    {ok, ReplicaSet} = emqx_ds_replication_layer_meta:replica_set(DB, Shard),
    [
        {server_name(DB, Shard, Site), emqx_ds_replication_layer_meta:node(Site)}
     || Site <- ReplicaSet
    ].

local_server(DB, Shard) ->
    Site = emqx_ds_replication_layer_meta:this_site(),
    {server_name(DB, Shard, Site), node()}.

cluster_name(DB, Shard) ->
    iolist_to_binary(io_lib:format("~s_~s", [DB, Shard])).

server_name(DB, Shard, Site) ->
    DBBin = atom_to_binary(DB),
    binary_to_atom(<<"ds_", DBBin/binary, Shard/binary, "_", Site/binary>>).

%%

servers(DB, Shard, _Order = leader_preferred) ->
    get_servers_leader_preferred(DB, Shard);
servers(DB, Shard, _Order = undefined) ->
    get_shard_servers(DB, Shard).

server(DB, Shard, _Which = random_follower) ->
    pick_random_replica(DB, Shard);
server(DB, Shard, _Which = local) ->
    get_local_server(DB, Shard).

get_servers_leader_preferred(DB, Shard) ->
    %% NOTE: Contact last known leader first, then rest of shard servers.
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_leader(ClusterName) of
        Leader when Leader /= undefined ->
            Servers = ra_leaderboard:lookup_members(ClusterName),
            [Leader | lists:delete(Leader, Servers)];
        undefined ->
            %% TODO: Dynamic membership.
            get_shard_servers(DB, Shard)
    end.

pick_random_replica(DB, Shard) ->
    %% NOTE: Contact random replica that is not a known leader.
    %% TODO: Replica may be down, so we may need to retry.
    ClusterName = get_cluster_name(DB, Shard),
    case ra_leaderboard:lookup_members(ClusterName) of
        Servers when is_list(Servers) ->
            Leader = ra_leaderboard:lookup_leader(ClusterName),
            pick_replica(Servers, Leader);
        undefined ->
            %% TODO
            %% Leader is unkonwn if there are no servers of this group on the
            %% local node. We want to pick a replica in that case as well.
            %% TODO: Dynamic membership.
            pick_server(get_shard_servers(DB, Shard))
    end.

pick_replica(Servers, Leader) ->
    case lists:delete(Leader, Servers) of
        [] ->
            Leader;
        Followers ->
            pick_server(Followers)
    end.

pick_server(Servers) ->
    lists:nth(rand:uniform(length(Servers)), Servers).

get_cluster_name(DB, Shard) ->
    ?MEMOIZE(DB, Shard, cluster_name(DB, Shard)).

get_local_server(DB, Shard) ->
    ?MEMOIZE(DB, Shard, local_server(DB, Shard)).

get_shard_servers(DB, Shard) ->
    maps:get(servers, emqx_ds_builtin_db_sup:lookup_shard_meta(DB, Shard)).

%%

init({DB, Shard}) ->
    _ = process_flag(trap_exit, true),
    _Meta = start_shard(DB, Shard),
    {ok, {DB, Shard}}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, {DB, Shard}) ->
    LocalServer = get_local_server(DB, Shard),
    ok = ra:stop_server(LocalServer).

%%

start_shard(DB, Shard) ->
    System = default,
    Site = emqx_ds_replication_layer_meta:this_site(),
    ClusterName = cluster_name(DB, Shard),
    LocalServer = local_server(DB, Shard),
    Servers = shard_servers(DB, Shard),
    case ra:restart_server(System, LocalServer) of
        ok ->
            ok;
        {error, name_not_registered} ->
            ok = ra:start_server(System, #{
                id => LocalServer,
                uid => <<ClusterName/binary, "_", Site/binary>>,
                cluster_name => ClusterName,
                initial_members => Servers,
                machine => {module, emqx_ds_replication_layer, #{db => DB, shard => Shard}},
                log_init_args => #{}
            })
    end,
    case Servers of
        [LocalServer | _] ->
            %% TODO
            %% Not super robust, but we probably don't expect nodes to be down
            %% when we bring up a fresh consensus group. Triggering election
            %% is not really required otherwise.
            %% TODO
            %% Ensure that doing that on node restart does not disrupt consensus.
            ok = ra:trigger_election(LocalServer);
        _ ->
            ok
    end,
    #{
        cluster_name => ClusterName,
        servers => Servers,
        local_server => LocalServer
    }.
